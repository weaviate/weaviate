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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const editOpColdShard = "cold"

// TestEditOpBucketsForShardsHoldsShardAgainstUnload pins that an unload waits
// while EditOpBucketsForShards is between a lazy shard's lookup and its load.
// Otherwise the load rebuilds a shard that already left the map.
func TestEditOpBucketsForShardsHoldsShardAgainstUnload(t *testing.T) {
	ctx := testCtx()
	var idx *Index
	unloaded := make(chan error, 1)
	// probe runs inside the load, after the lookup.
	probe := func() {
		go func() { unloaded <- idx.UnloadLocalShard(ctx, editOpColdShard) }()
		assert.Never(t, func() bool { return idx.shards.Load(editOpColdShard) == nil },
			200*time.Millisecond, time.Millisecond, "an unload took the shard out of the map mid-load")
	}
	db, idx, _, cold := newEditOpTestIndex(t, ctx, loadProbeAllocChecker{memwatch.NewDummyMonitor(), probe})

	buckets, err := db.EditOpBucketsForShards(ctx, idx.Config.ClassName.String(), []string{editOpColdShard})
	require.NoError(t, err)
	require.Empty(t, buckets, "the probe refuses the load")

	require.NoError(t, <-unloaded)
	require.Nil(t, idx.shards.Load(editOpColdShard))
	require.False(t, cold.isLoaded())
}

// TestEditOpBucketsForShardsOnShutDownIndex pins that no shard is loaded once
// the index has shut down. Index.Shutdown has already passed over every shard
// in the map, so nothing would close a shard loaded after it.
func TestEditOpBucketsForShardsOnShutDownIndex(t *testing.T) {
	ctx := testCtx()
	db, idx, loaded, cold := newEditOpTestIndex(t, ctx, nil)
	require.NoError(t, idx.Shutdown(ctx))

	buckets, err := db.EditOpBucketsForShards(ctx, idx.Config.ClassName.String(),
		[]string{loaded.Name(), editOpColdShard})
	require.NoError(t, err)
	require.Empty(t, buckets)
	require.False(t, cold.isLoaded())
}

func TestEditOpBucketsForLoadedShards(t *testing.T) {
	ctx := testCtx()
	db, idx, loaded, cold := newEditOpTestIndex(t, ctx, nil)

	buckets, err := db.EditOpBucketsForLoadedShards(idx.Config.ClassName.String(),
		[]string{loaded.Name(), editOpColdShard})
	require.NoError(t, err)
	require.Contains(t, buckets, loaded.Name())
	require.NotContains(t, buckets, editOpColdShard)
	require.False(t, cold.isLoaded())
}

// newEditOpTestIndex returns a DB over an index holding one loaded shard and the
// lazy shard editOpColdShard, not yet loaded, whose loads check allocChecker.
// A nil allocChecker takes the index's own.
func newEditOpTestIndex(t *testing.T, ctx context.Context, allocChecker memwatch.AllocChecker,
) (*DB, *Index, ShardLike, *LazyLoadShard) {
	t.Helper()
	class := newTestClassWithProps("EditOp_"+uuid.NewString()[:8], nil)
	loaded, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false,
		func(idx *Index) {
			idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
		})
	t.Cleanup(func() { loaded.Shutdown(context.Background()) })

	if allocChecker == nil {
		allocChecker = idx.allocChecker
	}
	cold := NewLazyLoadShard(ctx, nil, editOpColdShard, idx, class, idx.centralJobQueue,
		allocChecker, idx.shardLoadLimiter, idx.shardReindexer, false, idx.bitmapBufPool)
	idx.shards.Store(editOpColdShard, cold)

	db := &DB{logger: logrus.New(), indices: map[string]*Index{indexID(schema.ClassName(class.Class)): idx}}
	return db, idx, loaded, cold
}
