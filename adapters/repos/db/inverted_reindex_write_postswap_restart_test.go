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

// TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost is a RED pin for
// the restart residual of weaviate/0-weaviate-issues#319 (see t.Skip for the
// mechanism). Fixing it needs a durable per-shard flip-pending marker, which
// the pre-commit staging design for weaviate/0-weaviate-issues#220
// (weaviate/weaviate#11995) already plans to add — un-skip when it lands.
func TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost(t *testing.T) {
	t.Skip("RED pin for weaviate/0-weaviate-issues#319 restart residual: a node restart inside " +
		"the post-swap pre-flip window loses the in-memory force-index overlay, does not re-load the " +
		"promoted canonical bucket (initPropertyBuckets keys on the pre-flip schema), and no durable " +
		"local state re-arms either before the shard accepts writes — convergence depends entirely on " +
		"the DTM replay re-running the migration. Needs a durable flip-pending marker; see the " +
		"pre-commit staging design (weaviate/0-weaviate-issues#220, weaviate/weaviate#11995). " +
		"Un-skip when that lands — the assertions below then go green.")

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
