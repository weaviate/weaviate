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
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	lazyDropKeep    = "keep"
	lazyDropDropped = "to_drop"
)

// newLazyDropTenant loads a lazy tenant with two named vectors and enough
// writes that the dropped one's compressed bucket has a memtable to flush at
// shutdown. BQ is what gives that vector a bucket every insert writes through
// the store. reload opens the same tenant afresh.
func newLazyDropTenant(t *testing.T) (lazy *LazyLoadShard, reload func() *LazyLoadShard) {
	t.Helper()
	ctx := t.Context()

	const (
		tenant  = "lazy-drop"
		dims    = 64
		objects = 50
	)

	keepCfg := hnsw.NewDefaultUserConfig()
	dropCfg := hnsw.NewDefaultUserConfig()
	dropCfg.BQ = hnsw.BQConfig{Enabled: true}

	class := &models.Class{
		Class: "LazyDropVectorIndex",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{{
			Name:         "label",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
		}},
		VectorConfig: map[string]models.VectorConfig{
			lazyDropKeep:    {VectorIndexType: keepCfg.IndexType(), VectorIndexConfig: keepCfg},
			lazyDropDropped: {VectorIndexType: dropCfg.IndexType(), VectorIndexConfig: dropCfg},
		},
	}
	_, idx := testShardWithSettings(t, ctx, class, hnsw.UserConfig{Distance: common.DefaultDistanceMetric},
		false, true, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				lazyDropKeep:    keepCfg,
				lazyDropDropped: dropCfg,
			}
		})

	reload = func() *LazyLoadShard {
		return NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
			idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
			false, idx.bitmapBufPool)
	}
	vector := func(i, offset int) []float32 {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(i + j + offset)
		}
		return v
	}

	lazy = reload()
	require.NoError(t, lazy.Load(ctx))
	for i := 0; i < objects; i++ {
		require.NoError(t, lazy.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String()),
				Class:      class.Class,
				Properties: map[string]interface{}{"label": fmt.Sprintf("obj-%d", i)},
			},
			Vectors: map[string][]float32{lazyDropKeep: vector(i, 0), lazyDropDropped: vector(i, 1000)},
		}))
	}
	return lazy, reload
}

// holdShutdownPending lets a deactivation time out on a held reference, which
// leaves the shard loaded with its shutdown pending. Releasing the reference
// runs that teardown on the releasing goroutine.
func holdShutdownPending(t *testing.T, lazy *LazyLoadShard) (release func()) {
	t.Helper()
	release, err := lazy.shard.preventShutdown()
	require.NoError(t, err)
	require.ErrorIs(t, lazy.Shutdown(t.Context()), errShardStillInUse)
	require.True(t, lazy.isLoaded(), "precondition: a timed-out deactivation leaves the shard loaded")
	return release
}

// requireTenantReloads asserts the tenant came through its teardown intact. A
// failed teardown keeps its buckets registered, so a fresh load of the same
// tenant fails until the process restarts.
func requireTenantReloads(t *testing.T, lazy *LazyLoadShard, reload func() *LazyLoadShard) {
	t.Helper()
	require.NoError(t, shardTeardownError(lazy),
		"the teardown failed: the drop removed the vector's files from disk while the store "+
			"still held them, so its flush wrote into directories that were gone")
	again := reload()
	require.NoError(t, again.Load(t.Context()), "the tenant cannot be loaded again")
	require.NoError(t, again.Shutdown(t.Context()))
}

// TestLazyDropVectorIndex_PendingShutdownGoesThroughTheShard pins a drop that
// lands before the pending teardown starts. The store is open, so the vector's
// files have to go through it.
func TestLazyDropVectorIndex_PendingShutdownGoesThroughTheShard(t *testing.T) {
	ctx := t.Context()
	lazy, reload := newLazyDropTenant(t)
	release := holdShutdownPending(t, lazy)

	require.NoError(t, lazy.DropVectorIndex(ctx, lazyDropDropped))

	release()
	require.Eventually(t, lazy.shard.shut.Load, 10*time.Second, 50*time.Millisecond,
		"precondition: releasing the last reference completes the pending shutdown")
	requireTenantReloads(t, lazy, reload)
}

// TestLazyDropVectorIndex_WaitsOutATeardownInProgress pins a drop that lands
// while the pending teardown is already running. A teardown marks the shard
// shut before it flushes anything, so a drop that took that mark as a closed
// store would delete the vector's files mid-flush.
func TestLazyDropVectorIndex_WaitsOutATeardownInProgress(t *testing.T) {
	ctx := t.Context()
	lazy, reload := newLazyDropTenant(t)

	compressed := helpers.GetCompressedBucketName(lazyDropDropped)
	compressedDir := filepath.Join(lazy.shard.path(), "lsm", compressed)

	// The pin stalls the store's shutdown at this bucket, before it is flushed.
	bucket, unpinBucket := lazy.shard.store.AcquireBucketForRead(compressed)
	require.NotNil(t, bucket, "precondition: the dropped vector has a compressed bucket")
	unpin := sync.OnceFunc(unpinBucket)
	t.Cleanup(unpin)

	release := holdShutdownPending(t, lazy)
	teardownDone := make(chan struct{})
	go func() {
		defer close(teardownDone)
		release()
	}()
	require.Eventually(t, lazy.shard.shut.Load, 10*time.Second, 10*time.Millisecond,
		"precondition: the teardown has started")

	dropErr := make(chan error, 1)
	go func() { dropErr <- lazy.DropVectorIndex(ctx, lazyDropDropped) }()

	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-dropErr:
		t.Fatalf("the drop returned (err=%v) while the teardown was still flushing the store", err)
	default:
	}
	require.DirExists(t, compressedDir,
		"the drop deleted the vector's files while the teardown was still flushing them")

	unpin()
	<-teardownDone
	require.NoError(t, <-dropErr)
	require.NoDirExists(t, compressedDir, "once the teardown finished, the drop still has to remove the files")
	requireTenantReloads(t, lazy, reload)
}
