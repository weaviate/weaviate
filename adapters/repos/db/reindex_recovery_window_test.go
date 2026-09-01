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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRecoveryWindowSpansAnUnpromotedFlip(t *testing.T) {
	const trackerDir = "searchable_retokenize_title_1"

	tests := []struct {
		name             string
		rec              func(MigrationSubject) MigrationRecord
		oversizedPayload bool
		pastWalkBound    bool
		noPayload        bool
		rawPayload       string
		wantIn           bool
		wantFault        recoveryPayloadFault
		because          string
	}{
		{
			name:      "iterating",
			rec:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
			wantFault: recoveryPayloadNotApplicable,
			because:   "the scheduler restarts the unit and arms the mirror itself",
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
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{"title": s.CanonicalDirs["title"]})
			},
			wantIn: true,
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties, map[string]string{"title": s.CanonicalDirs["title"]})
			},
			wantFault: recoveryPayloadNotApplicable,
			because:   "the staged copy is the canonical one, so there is nothing left to mirror into",
		},
		{
			name:             "merged, with a payload past the apply-path parse bound",
			rec:              func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			oversizedPayload: true,
			wantIn:           true,
			because:          "an ordinary large multi-tenant migration still has to recover its mirror",
		},
		{
			name:          "merged, with a payload past the walk's own memory bound",
			rec:           func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			pastWalkBound: true,
			wantFault:     recoveryPayloadOversized,
			because:       "a payload no migration can write is not one to read into memory at boot",
		},
		{
			name:      "merged, with no payload.mig at all",
			rec:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			noPayload: true,
			wantFault: recoveryPayloadUnreadable,
			because:   "a payload that is not there is missing, not too large to read",
		},
		{
			name:       "merged, with a property name that escapes the shard",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			rawPayload: `{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":["../../../etc"]}}`,
			wantFault:  recoveryPayloadMalformed,
			because:    "a property name that is not one directory inside the shard is not a property list",
		},
		{
			name:       "merged, with a property name carrying a separator",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			rawPayload: `{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":["a/b"]}}`,
			wantFault:  recoveryPayloadMalformed,
			because:    "a separator makes the name address a directory the shard does not own",
		},
		{
			name:       "merged, with an empty property name",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			rawPayload: `{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":[""]}}`,
			wantFault:  recoveryPayloadMalformed,
			because:    "an empty name composes into another property's sidecar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migDir := filepath.Join(t.TempDir(), trackerDir)
			require.NoError(t, os.MkdirAll(migDir, 0o777))
			payload := []byte(`{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":["title"]}}`)
			if tt.rawPayload != "" {
				payload = []byte(tt.rawPayload)
			}
			if tt.oversizedPayload {
				payload = append(payload, bytes.Repeat([]byte(" "), maxRecoveryPayloadBytes)...)
			}
			payloadPath := filepath.Join(migDir, reindexRecoveryPayloadFile)
			if !tt.noPayload {
				require.NoError(t, os.WriteFile(payloadPath, payload, 0o600))
			}
			if tt.pastWalkBound {
				require.NoError(t, os.Truncate(payloadPath, maxRecoveryWalkPayloadBytes+1))
			}

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TrackerDir = trackerDir

			_, fault, _ := loadReindexRecoveryRecord(migDir, []MigrationRecord{tt.rec(subject)})
			require.Equal(t, tt.wantIn, fault == recoveryPayloadOK, tt.because)
			if !tt.wantIn {
				require.Equal(t, tt.wantFault, fault, tt.because)
			}
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
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{propName: s.CanonicalDirs[propName]})
			},
			wantArmed: true,
		},
		{
			name: "swapped, staged dir already promoted away",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{propName: s.CanonicalDirs[propName]})
			},
			noStagedDir: true,
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties, map[string]string{propName: s.CanonicalDirs[propName]})
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

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			subject := task.migrationSubject(shard, []string{propName}, time.Now())
			require.NoError(t, task.putMigrationRecord(shard, tt.rec(subject)))
			stagedDir := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])
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
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{propName: s.CanonicalDirs[propName]})
			},
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties, map[string]string{propName: s.CanonicalDirs[propName]})
			},
			wantReady: true,
		},
		{
			// No record decodes, so this shard cannot tell a flip that has
			// finished from one still running. The rangeable bucket is there
			// and nothing set an explicit entry, which is the one combination
			// that would otherwise default to ready.
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
				require.NoError(t, os.MkdirAll(shard.migrationRecords.Dir(), 0o777))
				require.NoError(t, os.WriteFile(
					filepath.Join(shard.migrationRecords.Dir(), "99_enable_searchable.json"),
					[]byte("{"), 0o600))
				require.NoError(t, shard.migrationRecords.Load())
				require.NotEmpty(t, shard.migrationRecords.Unreadable())
			} else {
				task, _ := newFilterableToRangeableTask(t, idx, className, propName)
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

// TestRecoveryWalkAggregatesUnreadableShardsIntoOneLine pins the startup walk's
// own fault reporting to one line per fault kind. Each of these faults is
// systemic, so a line per shard would follow the tenant count at every boot.
//
// The walk's line is the only one counted here. The record store it calls
// reports each unreadable set on its own shard, so the boot total is higher
// than one; that leaf predates this branch and is not this test's to hold.
// Matching is on the walk's own prefix at any level, because keying the count
// on Warn made it blind to exactly the per-shard line it exists to catch.
func TestRecoveryWalkAggregatesUnreadableShardsIntoOneLine(t *testing.T) {
	const shards = 12
	root := t.TempDir()
	indexPath := filepath.Join(root, "books_abc")

	for i := 0; i < shards; i++ {
		lsm := filepath.Join(indexPath, fmt.Sprintf("tenant-%02d", i), "lsm")
		recordsDir := filepath.Join(lsm, ".migrations", "records")
		require.NoError(t, os.MkdirAll(recordsDir, 0o777))
		// A record this build cannot read makes the whole set unreadable, which
		// is the "recovering nothing on this shard" arm.
		require.NoError(t, os.WriteFile(
			filepath.Join(recordsDir, "searchable_retokenize_title_1.json"),
			[]byte("not json"), 0o600))
	}

	logger, hook := test.NewNullLogger()
	recovered, err := DiscoverInFlightReindexTasks(root, logger, nil)
	require.NoError(t, err)
	require.Empty(t, recovered)

	// Every line the walk writes about unreadable records, not just the
	// summary: a per-shard line would pass a summary-only assertion while
	// still following the tenant count.
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

// TestRecoveryWalkReadsEachShardsRecordsOnce pins the startup walk to one
// record-set read per shard. Each shard carries several tracker dirs, so a
// walk that read per tracker instead would go to disk a multiple of the tenant
// count at every boot, and nothing else would say so.
func TestRecoveryWalkReadsEachShardsRecordsOnce(t *testing.T) {
	const (
		shards   = 6
		trackers = 2
	)
	root := t.TempDir()
	indexPath := filepath.Join(root, "books_abc")

	for i := 0; i < shards; i++ {
		migs := filepath.Join(indexPath, fmt.Sprintf("tenant-%02d", i), "lsm", ".migrations")
		for gen := 1; gen <= trackers; gen++ {
			require.NoError(t, os.MkdirAll(
				filepath.Join(migs, fmt.Sprintf("searchable_retokenize_title_%d", gen)), 0o777))
		}
	}

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	recovered, err := DiscoverInFlightReindexTasks(root, logger, nil)
	require.NoError(t, err)
	require.Empty(t, recovered, "no record names any of these trackers, so nothing recovers")

	require.Equal(t, shards, recordSetReadsReported(hook),
		"one read per shard, whatever the tracker count on it")
}
