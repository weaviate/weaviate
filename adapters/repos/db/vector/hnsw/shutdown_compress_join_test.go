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

package hnsw

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// buildCompressReadyIndex builds an hnsw index seeded with vectors and with the
// cache marked prefilled, so a subsequent UpdateUserConfig enabling compression
// runs the rebuild immediately on the Upgrade goroutine.
func buildCompressReadyIndex(t *testing.T, vectors [][]float32, thunk common.VectorForID[float32]) (*hnsw, ent.UserConfig) {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	uc := ent.UserConfig{VectorCacheMaxObjects: 1e12, MaxConnections: 8, EFConstruction: 64, EF: 64}
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "shutdown-compress",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk:      thunk,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors)), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	}))
	index.cachePrefilled.Store(true)
	return index, uc
}

func newCompressReadyIndex(t *testing.T) (*hnsw, ent.UserConfig) {
	t.Helper()
	dim, n := 32, 2000
	vectors, _ := testinghelpers.RandomVecs(n, 0, dim)
	return buildCompressReadyIndex(t, vectors, func(ctx context.Context, id uint64) ([]float32, error) {
		// compress() samples over the cache's padded length, which exceeds n;
		// return NotFound for ids past the inserted range so it skips them.
		if int(id) >= len(vectors) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		}
		return vectors[int(id)], nil
	})
}

func enableSQ(uc ent.UserConfig) ent.UserConfig {
	uc.SQ = ent.SQConfig{Enabled: true, RescoreLimit: 20, TrainingLimit: 100}
	return uc
}

// Shutdown must join the async compression goroutine Upgrade spawns before
// tearing down; otherwise its cache.Drop()/commit-log teardown races the
// in-flight compression. Run with -race.
func TestShutdown_JoinsAsyncCompression(t *testing.T) {
	index, uc := newCompressReadyIndex(t)
	require.NoError(t, index.UpdateUserConfig(enableSQ(uc), func() {}))
	require.NoError(t, index.Shutdown(context.Background()))
}

// Drop discards the compression result, so it must abort an in-flight upgrade
// rather than wait for it. The compression goroutine is parked deterministically
// inside a cache miss that blocks on the caller's ctx — compress()'s sampling
// loop passes dropCtx, so only Drop's cancel can release it. abortObserved then
// proves Drop joined the goroutine rather than merely cancelling it. Run with
// -race.
func TestDrop_AbortsAsyncCompression(t *testing.T) {
	dim, n := 32, 2000
	vectors, _ := testinghelpers.RandomVecs(n, 0, dim)

	var blockMisses, abortObserved atomic.Bool
	var enteredOnce sync.Once
	entered := make(chan struct{})
	index, uc := buildCompressReadyIndex(t, vectors, func(ctx context.Context, id uint64) ([]float32, error) {
		if blockMisses.Load() {
			enteredOnce.Do(func() { close(entered) })
			<-ctx.Done()
			// The sleep separates cancel from join: a Drop that only cancelled
			// without joining would return before abortObserved is set.
			time.Sleep(100 * time.Millisecond)
			abortObserved.Store(true)
			return nil, ctx.Err()
		}
		if int(id) >= len(vectors) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		}
		return vectors[int(id)], nil
	})

	blockMisses.Store(true)
	// TrainingLimit > n forces the sampling loop to exhaust the padded cache
	// range, so it is guaranteed to hit an empty slot and park in the thunk.
	uc.PQ = ent.PQConfig{
		Enabled:       true,
		Segments:      8,
		Centroids:     256,
		TrainingLimit: 100000,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
	}
	require.NoError(t, index.UpdateUserConfig(uc, func() {}))

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("compression never reached a blocking cache miss")
	}

	dropDone := make(chan error, 1)
	go func() { dropDone <- index.Drop(context.Background(), false) }()
	select {
	case err := <-dropDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Drop did not return: in-flight compression was not aborted")
	}
	require.True(t, abortObserved.Load(), "Drop returned before joining the compression goroutine")
	require.False(t, index.compressed.Load(), "aborted upgrade must not mark the index compressed")
}

// The compression callback runs on the Upgrade goroutine; if it calls
// Drop/Shutdown (which Wait on compressWg), compressWg.Done() must already have
// run or the goroutine waits on itself forever.
func TestCompressCallback_DoesNotDeadlockOnShutdown(t *testing.T) {
	index, uc := newCompressReadyIndex(t)

	done := make(chan struct{})
	require.NoError(t, index.UpdateUserConfig(enableSQ(uc), func() {
		_ = index.Shutdown(context.Background())
		close(done)
	}))

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("compression callback -> Shutdown deadlocked waiting on compressWg")
	}
}
