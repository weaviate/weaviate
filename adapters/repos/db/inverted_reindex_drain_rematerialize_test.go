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
	runDrainRematerialize(t, rematerializeProbe{
		classPrefix: "ModeADrain_",
		watchedDir:  func(idx *Index, _ *Shard) string { return idx.path() },
		del:         func(idx *Index, _ *Shard) error { return idx.drop() },
		what:        "class dir",
		deleteBy:    "DELETE renamed it away",
	})
}

// TestDropShardsDrainRematerialize is the per-shard-tenant-delete companion of
// TestModeADrainRematerialize (weaviate/0-weaviate-issues#288): dropShards
// runs under closeLock.RLock and never sets i.closed, so the close guard alone
// is blind to it — an unguarded tracker MkdirAll racing a tenant delete
// re-creates <class>/<shard>/lsm/.migrations/<mig>/ right after dropShards
// removed the shard dir, leaving an orphaned tenant shard dir on disk.
func TestDropShardsDrainRematerialize(t *testing.T) {
	runDrainRematerialize(t, rematerializeProbe{
		classPrefix: "DropShardsDrain_",
		watchedDir:  func(idx *Index, shard *Shard) string { return shardPath(idx.path(), shard.Name()) },
		del:         func(idx *Index, shard *Shard) error { return idx.dropShards([]string{shard.Name()}) },
		what:        "tenant shard dir",
		deleteBy:    "dropShards removed it",
	})
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

// rematerializeProbe parameterizes the shared drain-rematerialize driver by
// deletion primitive and the directory that must stay deleted.
type rematerializeProbe struct {
	classPrefix string
	watchedDir  func(idx *Index, shard *Shard) string
	del         func(idx *Index, shard *Shard) error
	what        string
	deleteBy    string
}

func runDrainRematerialize(t *testing.T, probe rematerializeProbe) {
	for _, tc := range drainRematerializeCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := probe.classPrefix + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)

			watched := probe.watchedDir(idx, shard)
			require.DirExists(t, watched, "watched dir must exist before the delete")

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)

			// The worker signals inHook at the top of the first tracker init,
			// then blocks on releaseHook until the delete has completed — no
			// sleeps.
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

			// Worker parked pre-MkdirAll; drive the delete to completion.
			<-inHook
			require.NoError(t, probe.del(idx, shard))
			require.NoFileExists(t, watched,
				"the delete must have removed the watched dir before the worker proceeds")

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
			_, statErr := os.Stat(watched)
			assert.Truef(t, os.IsNotExist(statErr),
				"draining reindex goroutine re-materialized %s %q after %s (stat err: %v)",
				probe.what, watched, probe.deleteBy, statErr)
		})
	}
}
