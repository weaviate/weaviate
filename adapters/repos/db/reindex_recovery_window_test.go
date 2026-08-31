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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestRecoveryWindowSpansAnUnpromotedFlip pins that every recorded state
// short of promoted needs its double-write mirror re-armed after a restart:
// the pointer flip lives only in the process that made it, so the next load
// serves the property from the canonical directory again until promotion
// renames the staged one over it — and every write in between goes with it
// unless the mirror is armed.
func TestRecoveryWindowSpansAnUnpromotedFlip(t *testing.T) {
	const trackerDir = "searchable_retokenize_title_1"

	tests := []struct {
		name string
		rec  func(MigrationSubject) MigrationRecord
		// oversizedPayload pads payload.mig past the parse bound.
		oversizedPayload bool
		// pastWalkBound grows payload.mig past the walk's own memory bound.
		pastWalkBound bool
		// rawPayload replaces the well-formed payload this tracker carries.
		rawPayload string
		wantIn     bool
		// wantWarn is the line an operator has to see. Refusal and a failed
		// parse both leave the mirror unarmed, so only the line tells them
		// apart.
		wantWarn string
		because  string
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
		{
			// A payload naming every tenant and unit of a large multi-tenant
			// migration clears a megabyte on its own, which is the bound the
			// apply-path probes use. Refusing it here would arm no mirror, and
			// the flip that follows takes the canonical directory away with
			// every write since the restart.
			name:             "merged, with a payload past the apply-path parse bound",
			rec:              func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			oversizedPayload: true,
			wantIn:           true,
			because:          "an ordinary large multi-tenant migration still has to recover its mirror",
		},
		{
			// The walk runs off any RAFT apply, so its bound is memory, not
			// latency: a corrupt or hostile file read whole at boot is an OOM
			// in a loop nothing recovers from.
			name:          "merged, with a payload past the walk's own memory bound",
			rec:           func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			pastWalkBound: true,
			wantWarn:      "beyond any size a migration can produce",
			because:       "a payload no migration can write is not one to read into memory at boot",
		},
		{
			// These names are composed into bucket and sidecar directory
			// names, which the strategies then create and remove. A record's
			// names passed the decoder's check on the way in; a payload's
			// never did, and a restored archive is free to carry any bytes.
			name:       "merged, with a property name that escapes the shard",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			rawPayload: `{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":["../../../etc"]}}`,
			because:    "a property name that is not one directory inside the shard is not a property list",
		},
		{
			name:       "merged, with a property name carrying a separator",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			rawPayload: `{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":["a/b"]}}`,
			because:    "a separator makes the name address a directory the shard does not own",
		},
		{
			name:       "merged, with an empty property name",
			rec:        func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			rawPayload: `{"taskID":"t","taskVersion":42,"unitID":"shard-1__node-0","payload":{"collection":"Books","properties":[""]}}`,
			because:    "an empty name composes into another property's sidecar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
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
			require.NoError(t, os.WriteFile(payloadPath, payload, 0o600))
			if tt.pastWalkBound {
				// Sparse, so the file is over the bound without the bytes.
				require.NoError(t, os.Truncate(payloadPath, maxRecoveryWalkPayloadBytes+1))
			}

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.TrackerDir = trackerDir

			_, ok := loadReindexRecoveryRecord(migDir, []MigrationRecord{tt.rec(subject)}, logger)
			require.Equal(t, tt.wantIn, ok, tt.because)
			if tt.wantWarn != "" {
				require.Contains(t, hook.LastEntry().Message, tt.wantWarn, tt.because)
			}
		})
	}
}

// TestShardLoadArmsTheMirrorForAnUnpromotedFlip is the other half of the same
// window: the recovery walk reconstructs the task, and this is the hook that
// actually arms the mirror when that task reaches the shard.
func TestShardLoadArmsTheMirrorForAnUnpromotedFlip(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name string
		rec  func(MigrationSubject) MigrationRecord
		// noStagedDir plants the record without the staged directory it
		// names, which is what a promotion that already renamed it leaves
		// behind.
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
			// Promotion renamed the staged directory onto the canonical name,
			// so the next one has nothing left to act on. Opening the name
			// again creates the empty directory that promotion then renames
			// over the live index.
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
			// The record alone is not the fixture: what the next promotion
			// will act on is the staged directory the record names.
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

// TestOnlyAPromotedFlipReportsRangeableReady pins which recorded states leave
// a rangeable property answering range filters from its canonical bucket.
//
// The flip decision is recorded before the first pointer moves and it lives
// only in the process that made it, so at a load the canonical rangeable
// directory is the empty one shard init just recreated. Answering "ready" for
// it plans range filters against nothing while IndexRangeFilters is already
// committed cluster-wide.
func TestOnlyAPromotedFlipReportsRangeableReady(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name      string
		rec       func(MigrationSubject) MigrationRecord
		wantReady bool
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

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			subject := task.migrationSubject(shard, []string{propName}, time.Now())
			require.NoError(t, task.putMigrationRecord(shard, tt.rec(subject)))

			markInFlightRangeableMigrationsNotReady(shard)

			require.Equal(t, tt.wantReady, shard.IsRangeableLocallyReady(propName))
		})
	}
}

// rangeableEnabledTestClass is [newFilterableToRangeableTestClass] with the
// rangeable index already on, which is what a load sees once the migration's
// schema effect is committed cluster-wide: shard init creates the canonical
// rangeable bucket whether or not anything ever promoted data into it.
func rangeableEnabledTestClass(className string) *models.Class {
	class := newFilterableToRangeableTestClass(className)
	on := true
	class.Properties[0].IndexRangeFilters = &on
	return class
}
