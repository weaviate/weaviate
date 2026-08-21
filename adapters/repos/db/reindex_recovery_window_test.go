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

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestRecoveryWindowSpansAnUnpromotedFlip pins which recorded states still
// need their double-write mirror re-armed after a restart.
//
// A recorded flip is not a promoted one. The pointer flip lives only in the
// process that made it, so the next load serves the property from the
// canonical directory again — and promotion removes that directory before
// renaming the staged one over it. Every write taken in between goes with it
// unless the mirror is armed, and promotion is withheld for as long as the
// shard is frozen, a handle is missing, or a stat fails.
func TestRecoveryWindowSpansAnUnpromotedFlip(t *testing.T) {
	const trackerDir = "searchable_retokenize_title_1"

	tests := []struct {
		name    string
		rec     func(MigrationSubject) MigrationRecord
		wantIn  bool
		because string
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
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{"title": s.CanonicalDirs["title"]})
			},
			wantIn: true,
		},
		{
			name: "promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties, map[string]string{"title": s.CanonicalDirs["title"]})
			},
			because: "the staged copy is the canonical one, so there is nothing left to mirror into",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			migDir := filepath.Join(t.TempDir(), trackerDir)
			require.NoError(t, os.MkdirAll(migDir, 0o777))
			require.NoError(t, os.WriteFile(filepath.Join(migDir, reindexRecoveryPayloadFile),
				[]byte(`{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":["title"]}}`), 0o600))

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TrackerDir = trackerDir

			_, ok := loadReindexRecoveryRecord(migDir, []MigrationRecord{tt.rec(subject)}, logger)
			require.Equal(t, tt.wantIn, ok, tt.because)
		})
	}
}

// TestShardLoadArmsTheMirrorForAnUnpromotedFlip is the other half of the same
// window: the recovery walk reconstructs the task, and this is the hook that
// actually arms the mirror when that task reaches the shard.
func TestShardLoadArmsTheMirrorForAnUnpromotedFlip(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name      string
		rec       func(MigrationSubject) MigrationRecord
		wantArmed bool
	}{
		{
			name: "swapped but not promoted",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{propName: s.CanonicalDirs[propName]})
			},
			wantArmed: true,
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
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())

			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			if tt.wantArmed {
				require.NotZero(t, shard.migrationMirrors.ArmedMigrationMirrors(),
					"a flip the next promotion will act on still needs its writes mirrored")
				return
			}
			require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors())
		})
	}
}
