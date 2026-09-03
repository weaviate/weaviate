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
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestCompletionRetriesAfterASchemaEffectFailure(t *testing.T) {
	viaSwap := func(ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
		return task.RunSwapOnShard(ctx, shard)
	}
	viaLoad := func(ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
		_, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		return err
	}

	tests := []struct {
		name         string
		reenter      func(context.Context, *ShardReindexTaskGeneric, *Shard) error
		destroyFirst bool
	}{
		{name: "the swap phase runs again after the schema effect failed", reenter: viaSwap},
		{name: "the load path picks the migration up again", reenter: viaLoad},
		{name: "a later load already deleted the promoted directory", reenter: viaSwap, destroyFirst: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := testCtx()
			propName := filterableToRangeablePropName
			class := newFilterableToRangeableTestClass("EnableRangeableRetry_" + uuid.NewString()[:8])
			off := false
			class.Properties[0].IndexRangeFilters = &off

			refuse, committed := true, 0
			effect := func() error {
				if refuse {
					return errors.New("schema effect refused")
				}
				committed++
				on := true
				class.Properties[0].IndexRangeFilters = &on
				return nil
			}
			newTask := func(idx *Index, unitID string) *ShardReindexTaskGeneric {
				task, wrapper := newFilterableToRangeableTask(t, idx, class.Class, propName, unitID)
				wrapper.onComplete = effect
				return task
			}

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			putRangeableTestObjects(t, ctx, shard, class.Class, 25)

			task := newTask(idx, shard.migrationUnit())
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.Error(t, task.RunSwapOnShard(ctx, shard),
				"fixture: the flip must run while its schema effect does not")

			shardName, lsmPath := shard.Name(), shard.pathLSM()
			canonical := helpers.BucketRangeableFromPropNameLSM(propName)
			reload := func(prev *Shard) *Shard {
				t.Helper()
				require.NoError(t, prev.Shutdown(ctx))
				simulateProcessRestartBucketCleanup(t, lsmPath)
				idx.shardReindexer = &noRecoveryTaskReindexer{}
				loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
				require.NoError(t, err)
				idx.shards.Store(shardName, loaded)
				return loaded.(*Shard)
			}

			post := reload(shard) // reconciliation promotes; the flag is still off
			require.DirExists(t, filepath.Join(lsmPath, canonical),
				"fixture: promotion must have renamed the staged directory onto the canonical name")
			require.Nil(t, post.store.Bucket(canonical),
				"fixture: the canonical bucket must be closed while the flag is off")

			if test.destroyFirst {
				post = reload(post)
				require.NoDirExists(t, filepath.Join(lsmPath, canonical),
					"fixture: the load must have deleted the promoted directory")
			}
			defer post.Shutdown(ctx)

			refuse = false
			err := test.reenter(ctx, newTask(idx, post.migrationUnit()), post)
			if test.destroyFirst {
				require.Error(t, err, "the migration was reported complete over a directory nothing serves")
				require.Zero(t, committed)
				return
			}
			require.NoError(t, err, "the retry that exists to commit the schema effect can never succeed")
			require.Equal(t, 1, committed)

			final := reload(post)
			defer final.Shutdown(ctx)
			total := 0
			for _, ids := range filterableToRangeableFingerprint(t, final.store.Bucket(canonical)) {
				total += len(ids)
			}
			require.Equal(t, 25, total,
				"the load deleted the promoted directory, so the property serves nothing")
		})
	}
}
