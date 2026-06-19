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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// setupColdObjectsCacheTest reuses testShardMultiTenant — the canonical
// MT Index+Shard helper — and initializes coldObjects, which the helper
// itself doesn't set up (it predates the field). Returns the live shard
// and the tenant name (the builder default).
func setupColdObjectsCacheTest(t *testing.T, className string) (*Index, ShardLike, string) {
	t.Helper()
	shard, idx := testShardMultiTenant(t, testCtx(), className)
	idx.coldObjects = newColdObjectCounts()
	return idx, shard, shard.Name()
}

// TestColdObjectsCache_HotColdLifecycleHooks drives a tenant through
// HOT→COLD→HOT via the real lifecycle methods Index.UnloadLocalShard
// and Index.LoadLocalShard. It verifies that the cache hooks
// (cacheColdCountFromShard on unload, dropColdObjectCount on load)
// stay wired into those paths; removing either hook in the future
// would cause this test to fail.
func TestColdObjectsCache_HotColdLifecycleHooks(t *testing.T) {
	const (
		className = "TestClass"
		nObjects  = 5
	)
	ctx := testCtx()
	idx, shard, tenant := setupColdObjectsCacheTest(t, className)

	insertObjects(t, shard, className, tenant, nObjects)
	flushObjectsBucket(t, shard)

	_, ok := idx.coldObjects.get(tenant)
	require.False(t, ok, "cache must start empty for HOT tenant")

	// HOT→COLD via the real unload path.
	require.NoError(t, idx.UnloadLocalShard(ctx, tenant))

	cached, ok := idx.coldObjects.get(tenant)
	require.True(t, ok, "unload must populate cold cache")
	require.Equal(t, int64(nObjects), cached)

	// While unloaded, localObjectCount must come from the cache.
	total, err := idx.localObjectCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(nObjects), total, "COLD count via cache")

	// COLD→HOT via the real load path.
	require.NoError(t, idx.LoadLocalShard(ctx, tenant, false))

	_, ok = idx.coldObjects.get(tenant)
	require.False(t, ok, "load must drop cold cache entry")

	total, err = idx.localObjectCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(nObjects), total, "HOT count after reload reads loaded shard")
}

// TestColdObjectsCache_DeleteWhileCold drives the tenant to COLD via
// the real UnloadLocalShard, then verifies that dropShards (the tenant
// delete path) removes the cache entry so the count disappears from
// LocalObjectCount.
func TestColdObjectsCache_DeleteWhileCold(t *testing.T) {
	const (
		className = "TestClass"
		nObjects  = 3
	)
	ctx := testCtx()
	idx, shard, tenant := setupColdObjectsCacheTest(t, className)

	insertObjects(t, shard, className, tenant, nObjects)
	flushObjectsBucket(t, shard)

	require.NoError(t, idx.UnloadLocalShard(ctx, tenant))
	cached, ok := idx.coldObjects.get(tenant)
	require.True(t, ok)
	require.Equal(t, int64(nObjects), cached)

	require.NoError(t, idx.dropShards([]string{tenant}))

	_, ok = idx.coldObjects.get(tenant)
	require.False(t, ok, "delete must remove the cache entry")
}

// TestColdObjectsCache_CountFromDisk verifies that
// cacheColdCountFromDisk — the function initAndStoreShards' COLD
// branch calls during startup walk — reads the count from on-disk LSM
// segments. We populate a tenant, flush so segments hit disk, drop the
// cache, then assert the disk read repopulates it.
func TestColdObjectsCache_CountFromDisk(t *testing.T) {
	const (
		className = "TestClass"
		nObjects  = 4
	)
	idx, shard, tenant := setupColdObjectsCacheTest(t, className)

	insertObjects(t, shard, className, tenant, nObjects)
	flushObjectsBucket(t, shard)

	idx.dropColdObjectCount(tenant)
	_, ok := idx.coldObjects.get(tenant)
	require.False(t, ok)

	idx.cacheColdCountFromDisk(tenant)

	cached, ok := idx.coldObjects.get(tenant)
	require.True(t, ok, "disk read must populate cache for COLD tenant")
	require.Equal(t, int64(nObjects), cached)
}

func insertObjects(t *testing.T, shard ShardLike, className, tenant string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		obj := testObject(className)
		obj.DocID = uint64(i)
		obj.Object.Tenant = tenant
		require.NoError(t, shard.PutObject(context.Background(), obj),
			"insert object %d into tenant %q", i, tenant)
	}
}

// flushObjectsBucket forces the shard's objects bucket to flush its
// memtable so ObjectCountAsync (which excludes the memtable) reflects
// the inserted objects.
func flushObjectsBucket(t *testing.T, shard ShardLike) {
	t.Helper()
	bucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
	require.NoError(t, bucket.FlushAndSwitch())
	c, err := shard.ObjectCountAsync(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, int64(0), c, "shard count must be non-zero after flush")
}
