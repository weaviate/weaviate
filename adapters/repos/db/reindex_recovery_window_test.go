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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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

			recovered, err := DiscoverInFlightReindexTasks(root, true, logger, nil)
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
		name string
		// rec nil leaves the shard without a record.
		rec         func(MigrationSubject) MigrationRecord
		noStagedDir bool
		wantArmed   bool
		// wantUntouched: re-attach must neither write a record nor open a directory.
		wantUntouched bool
		// wedged: the load reconciler marked the record stuck before re-attach ran.
		wedged bool
	}{
		{
			name:          "iterated, wedged by a task verdict at load",
			rec:           func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			wedged:        true,
			wantUntouched: true,
		},
		{
			name: "swapped, wedged by a promotion path",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties(), map[string]string{propName: s.Props[propName].Canonical})
			},
			wedged:    true,
			wantArmed: true,
		},
		{
			name:          "the load reconciler discarded the record",
			noStagedDir:   true,
			wantUntouched: true,
		},
		{
			name: "iterating",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordIterating(s, MigrationCheckpoint{})
			},
			wantUntouched: true,
		},
		{
			name:      "iterated",
			rec:       func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterated(s) },
			wantArmed: true,
		},
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
			if tt.rec != nil {
				require.NoError(t, task.putMigrationRecord(shard, tt.rec(subject)))
			}
			if tt.wedged {
				shard.migrationRecords.MarkWedged(subject.Key)
			}
			stagedDir := filepath.Join(shard.pathLSM(), subject.Props[propName].Staged)
			if !tt.noStagedDir {
				require.NoError(t, os.MkdirAll(stagedDir, 0o777))
			}
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
			recordsBefore, dirsBefore := migrationRecordsOf(shard), lsmDirNames(t, shard.pathLSM())

			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			if tt.wantUntouched {
				require.Equal(t, recordsBefore, migrationRecordsOf(shard),
					"a rebuilt task would restart a migration its task list may have ended")
				require.Equal(t, dirsBefore, lsmDirNames(t, shard.pathLSM()))
			}
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

// A withheld range index is this node's state, so a filter that needs it must
// not tell the user to change a schema that already has it.
func TestAFilterOnAWithheldRangeIndexDoesNotBlameTheSchema(t *testing.T) {
	const propName = filterableToRangeablePropName
	off := false

	for _, tt := range []struct {
		name      string
		rangeable bool
		wantMsg   string
	}{
		{name: "the schema has the range index this shard withholds", rangeable: true, wantMsg: ".migrations"},
		{name: "the schema has no index to filter on", wantMsg: "indexFilterable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WithheldRangeIndex_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)
			class.Properties[0].IndexFilterable = &off
			class.Properties[0].IndexRangeFilters = &tt.rangeable
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			plantUnreadableRecord(t, shard.migrationRecords.Dir())
			require.NoError(t, shard.migrationRecords.Load())
			markInFlightRangeableMigrationsNotReady(shard)

			_, _, err := shard.ObjectSearch(ctx, 10, &filters.LocalFilter{Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On:       &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(propName)},
				Value:    &filters.Value{Value: 1, Type: schema.DataTypeInt},
			}}, nil, nil, nil, additional.Properties{}, nil)

			var missing inverted.MissingIndexError
			require.ErrorAs(t, err, &missing, "the API keeps answering it as a missing index")
			require.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

func rangeableEnabledTestClass(className string) *models.Class {
	class := newFilterableToRangeableTestClass(className)
	on := true
	class.Properties[0].IndexRangeFilters = &on
	return class
}
