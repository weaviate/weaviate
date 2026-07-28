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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost pins
// weaviate/0-weaviate-issues#319 across a restart: the shard must re-open the
// promoted canonical bucket and re-arm the force-index overlay from the
// on-disk migration state, even though the live schema flag is still false.
func TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipRestart_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	// Restart happens inside the window: the flip hasn't happened yet.
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)
	defer shard2.Shutdown(ctx)

	bucket := shard2.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotNil(t, bucket, "canonical filterable bucket must be loaded after restart-finalize")
	require.NotEmpty(t, fingerprintRoaringSetBucket(t, bucket)["alpha"],
		"backfilled data must survive the restart")

	require.NoError(t, shard2.PutObject(ctx, objWithTitle(className, uuid.NewString(), "resttoken")),
		"post-restart in-window insert must not error")

	fp := fingerprintRoaringSetBucket(t, bucket)
	require.NotEmptyf(t, fp["resttoken"],
		"weaviate/0-weaviate-issues#319 restart residual: an insert after a restart inside the "+
			"post-swap pre-flip window must reach the migrated canonical bucket; got %v", fp)
}

// TestReindexPostSwapPreFlip_RepeatedRestartsInWindow pins the second-restart
// hazard. By then the migrated data sits at its canonical bucket name while
// the schema still calls the property unindexed — the exact shape the
// nonexistent-index sweep deletes on sight, and the sidecars it was built
// from are gone, so the deletion is unrecoverable.
func TestReindexPostSwapPreFlip_RepeatedRestartsInWindow(t *testing.T) {
	const propName = "title"
	restartTokens := []string{"resttokena", "resttokenb"}

	tests := []struct {
		name        string
		target      postSwapPreFlipTarget
		classPrefix string
	}{
		{
			name:        "enable-filterable",
			target:      filterableTarget(),
			classPrefix: "PostSwapPreFlipEfRestarts",
		},
		{
			name:        "enable-searchable",
			target:      searchableTarget(),
			classPrefix: "PostSwapPreFlipEsRestarts",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := tc.classPrefix + "_" + uuid.NewString()[:8]
			class := newNoIndexTestClass(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			shardName := shard.Name()

			var live *Shard
			t.Cleanup(func() {
				if live != nil {
					live.Shutdown(ctx)
				}
			})

			require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))
			tc.target.drive(t, shard, idx, className, propName)
			require.NoError(t, shard.Shutdown(ctx))

			for i, token := range restartTokens {
				shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
				require.NoErrorf(t, err, "restart %d must succeed", i+1)
				live = shd.(*Shard)
				idx.shards.Store(shardName, shd)

				bucket := tc.target.bucket(live, propName)
				require.NotNilf(t, bucket,
					"restart %d: canonical %s bucket must be loaded", i+1, tc.target.label)
				require.NotEmptyf(t, tc.target.fingerprint(t, bucket)["alpha"],
					"restart %d: backfilled data must survive a restart inside the "+
						"post-swap pre-flip window", i+1)

				require.NoErrorf(t, live.PutObject(ctx, objWithTitle(className, uuid.NewString(), token)),
					"restart %d: in-window insert must not error", i+1)
				fp := tc.target.fingerprint(t, bucket)
				require.NotEmptyf(t, fp[token],
					"weaviate/0-weaviate-issues#319: after restart %d inside the post-swap pre-flip "+
						"window an insert must reach the migrated canonical %s bucket; got %v",
					i+1, tc.target.label, fp)

				require.NoError(t, live.Shutdown(ctx))
				live = nil
			}
		})
	}
}
