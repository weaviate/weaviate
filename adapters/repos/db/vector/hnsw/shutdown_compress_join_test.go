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

// newCompressReadyIndex builds an hnsw index seeded with data and with the cache
// marked prefilled, so a subsequent UpdateUserConfig enabling SQ runs the
// compression rebuild immediately on the Upgrade goroutine.
func newCompressReadyIndex(t *testing.T) (*hnsw, ent.UserConfig) {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	dim, n := 32, 2000
	vectors, _ := testinghelpers.RandomVecs(n, 0, dim)
	uc := ent.UserConfig{VectorCacheMaxObjects: 1e12, MaxConnections: 8, EFConstruction: 64, EF: 64}
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "shutdown-compress",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			// compress() samples over the cache's padded length, which exceeds n;
			// return NotFound for ids past the inserted range so it skips them.
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(n), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	}))
	index.cachePrefilled.Store(true)
	return index, uc
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
