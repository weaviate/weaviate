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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
)

const dimsMigrationVectorLen = 3

// dimsMigrationShard returns a loaded shard whose dimensions bucket uses the map
// strategy and tracks the returned ids, as a shard created before v1.34 has it.
func dimsMigrationShard(t *testing.T, ctx context.Context, objects int) (*Shard, *Index, *models.Class, []strfmt.UUID) {
	t.Helper()
	class := &models.Class{Class: "DimsMigration"}

	// without tracking the shard leaves the bucket dir alone
	shd, idx := testShard(t, ctx, class.Class)
	shard := shd.(*Shard)
	require.NoError(t, shard.Shutdown(ctx))

	// a bucket picks up the strategy of what it finds on disk
	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM), "",
		idx.logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection))
	require.NoError(t, err)
	require.NoError(t, b.MapSet([]byte("unused\x04\x00\x00\x00"), lsmkv.MapPair{Key: make([]byte, 8), Value: []byte{}}))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	idx.Config.TrackVectorDimensions = true
	shard = reloadDimsMigrationShard(t, ctx, idx, shard, class)
	require.Equal(t, lsmkv.StrategyMapCollection, shard.store.Bucket(helpers.DimensionsBucketLSM).Strategy())

	return shard, idx, class, putDimsMigrationObjects(t, ctx, shard, class, objects)
}

func putDimsMigrationObjects(t *testing.T, ctx context.Context, shard *Shard, class *models.Class, objects int) []strfmt.UUID {
	t.Helper()
	ids := make([]strfmt.UUID, objects)
	for i := range ids {
		obj := testObject(class.Class)
		obj.Vector = randVector(dimsMigrationVectorLen)
		require.NoError(t, shard.PutObject(ctx, obj))
		ids[i] = obj.ID()
	}
	return ids
}

// reloadDimsMigrationShard leaves shutting down the returned shard to the caller.
func reloadDimsMigrationShard(t *testing.T, ctx context.Context, idx *Index, shard *Shard, class *models.Class) *Shard {
	t.Helper()
	// a shard that is shut down already reports so
	_ = shard.Shutdown(ctx)

	shd, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shard.Name(), shd)
	return shd.(*Shard)
}

func requireDimensions(t *testing.T, ctx context.Context, shard *Shard, objects int) {
	t.Helper()
	dims, err := shard.Dimensions(ctx, "")
	require.NoError(t, err)
	require.Equal(t, objects*dimsMigrationVectorLen, dims)
}

func TestShard_MigrateDimensionsBucketToRoaringSet(t *testing.T) {
	ctx := testCtx()
	shard, idx, class, ids := dimsMigrationShard(t, ctx, 10)
	// not from within a subtest, the shard outlives it
	t.Cleanup(func() { _ = shard.Shutdown(context.Background()) })
	requireDimensions(t, ctx, shard, 10)

	t.Run("not enabled, the map bucket stays and is written to", func(t *testing.T) {
		shard = reloadDimsMigrationShard(t, ctx, idx, shard, class)
		require.Equal(t, lsmkv.StrategyMapCollection, shard.store.Bucket(helpers.DimensionsBucketLSM).Strategy())
		ids = append(ids, putDimsMigrationObjects(t, ctx, shard, class, 2)...)
		requireDimensions(t, ctx, shard, 12)
	})

	idx.Config.MigrateDimensionsToRoaringSet = true

	t.Run("enabled, the bucket is migrated when the shard loads", func(t *testing.T) {
		shard = reloadDimsMigrationShard(t, ctx, idx, shard, class)
		require.Equal(t, lsmkv.StrategyRoaringSet, shard.store.Bucket(helpers.DimensionsBucketLSM).Strategy())
		requireDimensions(t, ctx, shard, 12)
	})

	t.Run("writes after the migration are tracked", func(t *testing.T) {
		ids = append(ids, putDimsMigrationObjects(t, ctx, shard, class, 3)...)
		requireDimensions(t, ctx, shard, 15)

		require.NoError(t, shard.DeleteObject(ctx, ids[0], time.Time{}))
		requireDimensions(t, ctx, shard, 14)
	})

	t.Run("still enabled on the next start, nothing changes", func(t *testing.T) {
		shard = reloadDimsMigrationShard(t, ctx, idx, shard, class)
		require.Equal(t, lsmkv.StrategyRoaringSet, shard.store.Bucket(helpers.DimensionsBucketLSM).Strategy())
		requireDimensions(t, ctx, shard, 14)
	})
}

// A migration interrupted between its two renames has left the bucket dir missing.
// Loading the shard must put the migrated bucket in place rather than start an
// empty one, also when the migration has been turned off meanwhile.
func TestShard_RecoversInterruptedDimensionsMigration(t *testing.T) {
	ctx := testCtx()
	shard, idx, class, _ := dimsMigrationShard(t, ctx, 10)
	require.NoError(t, shard.Shutdown(ctx))

	migrated, err := shardusage.MigrateDimensionsBucketToRoaringSet(ctx, idx.logger, idx.path(), shard.Name())
	require.NoError(t, err)
	require.True(t, migrated)

	bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)
	require.NoError(t, os.Rename(bucketPath, bucketPath+"__to_roaringset_ready"))
	require.NoError(t, os.Mkdir(bucketPath+"___del", 0o700))

	require.False(t, idx.Config.MigrateDimensionsToRoaringSet)
	shard = reloadDimsMigrationShard(t, ctx, idx, shard, class)
	defer shard.Shutdown(ctx)

	assert.Equal(t, lsmkv.StrategyRoaringSet, shard.store.Bucket(helpers.DimensionsBucketLSM).Strategy())
	requireDimensions(t, ctx, shard, 10)
	require.NoDirExists(t, bucketPath+"__to_roaringset_ready")
	require.NoDirExists(t, bucketPath+"___del")
}

// A usage scan that takes the shard for unloaded opens the dimensions bucket by
// itself. lsmkv refuses to open a bucket dir twice, so the shard has to wait for
// the scan instead of failing to load.
func TestShard_DimensionsBucketLoadWaitsForUnloadedScan(t *testing.T) {
	ctx := testCtx()
	shard, idx, class, _ := dimsMigrationShard(t, ctx, 1)
	require.NoError(t, shard.Shutdown(ctx))

	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, idx.path(), shard.Name())
	require.NoError(t, err)

	loaded := make(chan error, 1)
	go func() {
		shd, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
		if err == nil {
			idx.shards.Store(shard.Name(), shd)
			t.Cleanup(func() { _ = shd.Shutdown(context.Background()) })
		}
		loaded <- err
	}()

	select {
	case err := <-loaded:
		unlock()
		require.FailNowf(t, "shard loaded while the dimensions bucket was in use", "err: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	unlock()
	select {
	case err := <-loaded:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "shard did not load after the dimensions bucket was released")
	}
}
