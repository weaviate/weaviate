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

package reindex_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestModeADrainRematerialize pins the race where a draining reindex
// goroutine's tracker MkdirAll re-creates the class dir a concurrent DELETE
// (Index.drop) just renamed away.
func TestModeADrainRematerialize(t *testing.T) {
	cases := []struct {
		name  string
		drive func(ctx context.Context, task *reindex.ShardReindexTaskGeneric, shard *db.Shard) error
	}{
		{
			name: "worker drain via OnAfterLsmInitAsync",
			drive: func(ctx context.Context, task *reindex.ShardReindexTaskGeneric, shard *db.Shard) error {
				_, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				return err
			},
		},
		{
			name: "DTM lifecycle via RunReindexOnlyOnShard",
			drive: func(ctx context.Context, task *reindex.ShardReindexTaskGeneric, shard *db.Shard) error {
				return task.RunReindexOnlyOnShard(ctx, shard)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ModeADrain_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"title"})

			shd, idx, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*db.Shard)

			// Mirrors the unexported (*db.Index).path().
			idxPath := filepath.Join(idx.Config.RootPath, idx.ID())
			require.DirExists(t, idxPath, "class dir must exist before drop")

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: reindex.MapToBlockmaxStrategy{Generation: 1}}
			task := newTestTask(f.Logger(), strategy)

			inHook := make(chan struct{})
			releaseHook := make(chan struct{})
			var hookOnce sync.Once

			// Interpose a barrier at the tracker's pre-MkdirAll point (before
			// the real close-lock guard runs, holding no lock) so the DELETE
			// lands between the worker parking and its guarded MkdirAll.
			realGuardFor := task.TrackerMkdirGuard()
			task.SetTrackerMkdirGuard(func(s reindex.ShardLike) func(func() error) error {
				guard := realGuardFor(s)
				return func(mkdir func() error) error {
					hookOnce.Do(func() { close(inHook) })
					<-releaseHook
					return guard(mkdir)
				}
			})

			var driveErr error
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				driveErr = tc.drive(ctx, task, shard)
			}()

			// Worker parked pre-MkdirAll; drive the DELETE to completion.
			// f.Drop forwards to the unexported (*db.Index).drop, which is
			// the exact production path this test pins: beginClose() is
			// what makes the tracker's guarded MkdirAll return
			// context.Canceled.
			<-inHook
			require.NoError(t, f.Drop())
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
