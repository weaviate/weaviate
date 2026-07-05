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
	cases := []struct {
		name  string
		drive func(ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error
	}{
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

	for _, tc := range cases {
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
