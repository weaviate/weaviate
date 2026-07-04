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

// TestModeADrainRematerialize pins the race where a draining reindex
// goroutine's tracker MkdirAll re-creates the class dir a concurrent DELETE
// (Index.drop) just renamed away.
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

			inHook := make(chan struct{})
			releaseHook := make(chan struct{})
			var hookOnce sync.Once

			// Interpose a barrier at the tracker's pre-MkdirAll point (before
			// the real close-lock guard runs, holding no lock) so the DELETE
			// lands between the worker parking and its guarded MkdirAll.
			realGuardFor := task.trackerMkdirGuard
			task.trackerMkdirGuard = func(s ShardLike) func(func() error) error {
				guard := realGuardFor(s)
				return func(mkdir func() error) error {
					hookOnce.Do(func() { close(inHook) })
					<-releaseHook
					return guard(mkdir)
				}
			}

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

			// nil is also fine: only a clean stop is acceptable.
			if driveErr != nil {
				assert.Truef(t, errors.Is(driveErr, context.Canceled),
					"production entry failed with an unexpected error (want context.Canceled): %v", driveErr)
			}

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
// test wraps trackerMkdirGuard to park the DTM goroutine just before the
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

			inHook := make(chan struct{})
			releaseHook := make(chan struct{})
			var hookOnce sync.Once

			// Interpose a barrier at the tracker's pre-MkdirAll point (before
			// the real shard-lock guard runs, holding no lock) so the tenant
			// delete lands between the worker parking and its guarded MkdirAll.
			realGuardFor := task.trackerMkdirGuard
			task.trackerMkdirGuard = func(s ShardLike) func(func() error) error {
				guard := realGuardFor(s)
				return func(mkdir func() error) error {
					hookOnce.Do(func() { close(inHook) })
					<-releaseHook
					return guard(mkdir)
				}
			}

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
