//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRecoveryWindowSpansAnUnpromotedFlip(t *testing.T) {
	tests := []struct {
		name       string
		rec        func(MigrationSubject) MigrationRecord
		unreadable bool
		wantIn     bool
		because    string
	}{
		{
			name:    "iterating",
			rec:     func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
			because: "the scheduler restarts the unit and arms the mirror itself",
		},
		{
			name:   "iterated",
			rec:    func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			wantIn: true,
		},
		{
			name:   "merged",
			rec:    func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			wantIn: true,
		},
		{
			name: "swapped but not promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties(), map[string]string{"title": s.Props["title"].Canonical})
			},
			wantIn: true,
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties(), map[string]string{"title": s.Props["title"].Canonical})
			},
			because: "the staged copy is the canonical one, so there is nothing left to mirror into",
		},
		{
			name:       "merged, beside a record this build cannot read",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			unreadable: true,
			wantIn:     true,
			because:    "one unreadable record must not cost the readable ones their mirror",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			lsm := filepath.Join(root, "books_abc", "shard-1", "lsm")
			require.NoError(t, os.MkdirAll(lsm, 0o777))
			logger, _ := test.NewNullLogger()
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			store := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, store.Load())
			require.NoError(t, store.Put(tt.rec(subject)))
			if tt.unreadable {
				plantUnreadableRecord(t, store.Dir())
			}

			recovered, err := DiscoverInFlightReindexTasks(root, logger, nil)
			require.NoError(t, err)

			if !tt.wantIn {
				require.Empty(t, recovered, tt.because)
				return
			}
			require.Len(t, recovered, 1, tt.because)
			require.Len(t, recovered[0].Tasks, 1)
			require.Equal(t, subject.Key, recovered[0].Tasks[0].migrationRecordKey())
		})
	}
}

func TestShardLoadArmsTheMirrorForAnUnpromotedFlip(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name        string
		rec         func(MigrationSubject) MigrationRecord
		noStagedDir bool
		wantArmed   bool
	}{
		{
			name: "swapped but not promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties(), map[string]string{propName: s.Props[propName].Canonical})
			},
			wantArmed: true,
		},
		{
			name: "swapped, staged dir already promoted away",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties(), map[string]string{propName: s.Props[propName].Canonical})
			},
			noStagedDir: true,
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties(), map[string]string{propName: s.Props[propName].Canonical})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RecoveryWindowArming_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newFilterableToRangeableTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			task, _ := newFilterableToRangeableTask(t, idx, className, propName, shard.migrationUnit())
			subject := task.migrationSubject(shard, []string{propName}, time.Now())
			require.NoError(t, task.putMigrationRecord(shard, tt.rec(subject)))
			stagedDir := filepath.Join(shard.pathLSM(), subject.Props[propName].Staged)
			if !tt.noStagedDir {
				require.NoError(t, os.MkdirAll(stagedDir, 0o777))
			}
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())

			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			if tt.wantArmed {
				require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors(),
					"a flip the next promotion will act on still needs its writes mirrored")
				return
			}
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
			if tt.noStagedDir {
				require.NoDirExists(t, stagedDir,
					"opening a staged dir promotion already renamed away re-creates it empty for the next promotion to rename over the live index")
			}
		})
	}
}

func TestOnlyAPromotedFlipReportsRangeableReady(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name       string
		rec        func(MigrationSubject) MigrationRecord
		unreadable bool
		wantReady  bool
	}{
		{
			name: "iterating",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordIterating(s, MigrationCheckpoint{})
			},
		},
		{
			name: "iterated",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
		},
		{
			name: "merged",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
		{
			name: "swapped but not promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties(), map[string]string{propName: s.Props[propName].Canonical})
			},
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties(), map[string]string{propName: s.Props[propName].Canonical})
			},
			wantReady: true,
		},
		{
			// No record decodes, so a finished flip is indistinguishable from a
			// running one. Bucket present and no explicit entry is the one
			// combination that would otherwise default to ready.
			name:       "a record that does not decode leaves the flip undecidable",
			unreadable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RangeableReadiness_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, rangeableEnabledTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			require.True(t, shard.IsRangeableLocallyReady(propName),
				"fixture: a property whose rangeable bucket exists defaults to ready")

			if tt.unreadable {
				plantUnreadableRecord(t, shard.migrationRecords.Dir())
				require.NoError(t, shard.migrationRecords.Load())
				require.NotEmpty(t, shard.migrationRecords.Unreadable())
			} else {
				task, _ := newFilterableToRangeableTask(t, idx, className, propName, shard.migrationUnit())
				subject := task.migrationSubject(shard, []string{propName}, time.Now())
				require.NoError(t, task.putMigrationRecord(shard, tt.rec(subject)))
			}

			markInFlightRangeableMigrationsNotReady(shard)

			require.Equal(t, tt.wantReady, shard.IsRangeableLocallyReady(propName))
		})
	}
}

func rangeableEnabledTestClass(className string) *models.Class {
	class := newFilterableToRangeableTestClass(className)
	on := true
	class.Properties[0].IndexRangeFilters = &on
	return class
}

// One line per fault kind: these faults are systemic, so a line per shard would
// follow the tenant count at every boot. Only the walk's own line is counted —
// the record store reports separately per shard — and matching is on the walk's
// prefix at any level, since keying on Warn misses the per-shard line.
func TestRecoveryWalkAggregatesUnreadableShardsIntoOneLine(t *testing.T) {
	const shards = 12
	root := t.TempDir()
	indexPath := filepath.Join(root, "books_abc")

	for i := 0; i < shards; i++ {
		lsm := filepath.Join(indexPath, fmt.Sprintf("tenant-%02d", i), "lsm")
		recordsDir := filepath.Join(lsm, ".migrations", "records")
		require.NoError(t, os.MkdirAll(recordsDir, 0o777))
		// One unreadable record makes the whole set unreadable: the "recovering
		// nothing on this shard" arm.
		require.NoError(t, os.WriteFile(
			filepath.Join(recordsDir, "searchable_retokenize_title_1.json"),
			[]byte("not json"), 0o600))
	}

	logger, hook := test.NewNullLogger()
	recovered, err := DiscoverInFlightReindexTasks(root, logger, nil)
	require.NoError(t, err)
	require.Empty(t, recovered)

	// Every line about unreadable records, not just the summary: a per-shard line
	// would pass a summary-only assertion and still follow the tenant count.
	var about []string
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "reindex recovery:") &&
			strings.Contains(e.Message, "could not be read") {
			about = append(about, e.Message)
		}
	}
	require.Len(t, about, 1, "one line for the whole walk, not one per shard: %v", about)
	require.Contains(t, about[0], fmt.Sprintf("%d shard(s)", shards),
		"the one line carries the count the per-shard lines used to carry")
}
