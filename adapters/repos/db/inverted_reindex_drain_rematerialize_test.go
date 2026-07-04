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
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestModeADrainRematerialize deterministically reproduces the race where a
// cancelled-but-still-draining DTM reindex goroutine re-enters a tracker
// init after a concurrent DELETE: Index.drop renamed the class dir away, but
// pathLSM() re-resolves to the ORIGINAL path and an unguarded tracker
// MkdirAll re-creates it — an orphaned class dir the DELETE was supposed to
// remove. The TEST-ONLY reindexTrackerInitHook parks the goroutine just
// before the MkdirAll until drop() completes — no sleeps.
//
// Each subtest drives a REAL production entry point (not the tracker factory
// directly), so reverting the guard wiring at either callsite turns the
// corresponding subtest red at the final os.Stat:
//
//   - the worker drain path: OnAfterLsmInitAsync, the per-iteration re-entry
//     a cancelled worker takes with no ctx check before the tracker MkdirAll;
//   - the DTM lifecycle path: RunReindexOnlyOnShard → runShardLifecycle →
//     onAfterLsmInitGuarded — the same route enterDTMPhase's iteration
//     resume ladder takes (and RunOnShard shares).
func TestModeADrainRematerialize(t *testing.T) {
	for _, tc := range drainRematerializeCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ModeADrain_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)

			idxPath := idx.path()
			require.DirExists(t, idxPath, "class dir must exist before drop")

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)

			// The worker signals inHook at the top of the first tracker init,
			// then blocks on releaseHook until drop() has completed — no sleeps.
			inHook := make(chan struct{})
			releaseHook := make(chan struct{})
			var hookOnce sync.Once

			reindexTrackerInitHook = func() {
				hookOnce.Do(func() { close(inHook) })
				<-releaseHook
			}
			t.Cleanup(func() { reindexTrackerInitHook = nil })

			var driveErr error
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				driveErr = tc.drive(ctx, task, shard)
			}()

			// Worker parked pre-MkdirAll; drive the DELETE to completion.
			<-inHook
			require.NoError(t, idx.drop())
			require.NoFileExists(t, idxPath,
				"drop() must have renamed the class dir away before the worker proceeds")

			// Release the worker so its MkdirAll runs AFTER the rename.
			close(releaseHook)
			<-workerDone

			// The guard bails with context.Canceled (clean stop); a nil error
			// would also be acceptable if a future refactor swallows the stop.
			if driveErr != nil {
				assert.Truef(t, errors.Is(driveErr, context.Canceled),
					"production entry failed with an unexpected error (want context.Canceled): %v", driveErr)
			}

			// The load-bearing assertion.
			_, statErr := os.Stat(idxPath)
			assert.Truef(t, os.IsNotExist(statErr),
				"draining reindex goroutine re-materialized class dir %q after DELETE renamed it away (stat err: %v)",
				idxPath, statErr)
		})
	}
}

type drainRematerializeCase struct {
	name  string
	drive func(ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error
}

// drainRematerializeCases are the two production entry points whose guarded
// tracker init can race a concurrent delete (whole-index drop or per-shard
// dropShards); both rematerialize tests drive the same pair.
func drainRematerializeCases() []drainRematerializeCase {
	return []drainRematerializeCase{
		{
			name: "worker drain via OnAfterLsmInitAsync",
			drive: func(ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
				_, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				return err
			},
		},
		{
			name: "DTM lifecycle via RunReindexOnlyOnShard",
			drive: func(ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
				return task.RunReindexOnlyOnShard(ctx, shard)
			},
		},
	}
}

// TestDropShardsDrainRematerialize is the per-shard-tenant-delete companion of
// TestModeADrainRematerialize (weaviate/0-weaviate-issues#288): dropShards
// runs under closeLock.RLock and never sets i.closed, so the close guard alone
// is blind to it — an unguarded tracker MkdirAll racing a tenant delete
// re-creates <class>/<shard>/lsm/.migrations/<mig>/ right after dropShards
// removed the shard dir, leaving an orphaned tenant shard dir on disk. The
// TEST-ONLY reindexTrackerInitHook parks the DTM goroutine just before the
// MkdirAll until dropShards completes — no sleeps.
func TestDropShardsDrainRematerialize(t *testing.T) {
	for _, tc := range drainRematerializeCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "DropShardsDrain_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			shardName := shard.Name()

			shardDir := shardPath(idx.path(), shardName)
			require.DirExists(t, shardDir, "shard dir must exist before dropShards")

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)

			// The worker signals inHook at the top of the first tracker init,
			// then blocks on releaseHook until dropShards has completed.
			inHook := make(chan struct{})
			releaseHook := make(chan struct{})
			var hookOnce sync.Once

			reindexTrackerInitHook = func() {
				hookOnce.Do(func() { close(inHook) })
				<-releaseHook
			}
			t.Cleanup(func() { reindexTrackerInitHook = nil })

			var driveErr error
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				driveErr = tc.drive(ctx, task, shard)
			}()

			// Worker parked pre-MkdirAll; drive the tenant delete to completion.
			<-inHook
			require.NoError(t, idx.dropShards([]string{shardName}))
			require.NoDirExists(t, shardDir,
				"dropShards must have removed the shard dir before the worker proceeds")

			// Release the worker so its MkdirAll runs AFTER the removal.
			close(releaseHook)
			<-workerDone

			// The guard bails with context.Canceled (clean stop); a nil error
			// would also be acceptable if a future refactor swallows the stop.
			if driveErr != nil {
				assert.Truef(t, errors.Is(driveErr, context.Canceled),
					"production entry failed with an unexpected error (want context.Canceled): %v", driveErr)
			}

			// The load-bearing assertion.
			_, statErr := os.Stat(shardDir)
			assert.Truef(t, os.IsNotExist(statErr),
				"draining reindex goroutine re-materialized tenant shard dir %q after dropShards removed it (stat err: %v)",
				shardDir, statErr)
		})
	}
}
