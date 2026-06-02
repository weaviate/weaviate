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
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// AddMultiBatch reads the compression trio in two spots — the PreloadMulti
// branch and growIndexToAccomodateNode (index growth) — now both taken under
// compressActionLock, like the rest of the insert/search paths.
//
// This runs many multivector inserts (which grow the index) concurrently with
// enabling compression, asserting liveness and deadlock-freedom under the added
// locking. The data race itself is gated by the atomic h.compressed flag, so
// -race does not deterministically reproduce it; the fix rests on lock
// discipline. BQ is used because it is training-free (no cache sampling).
func TestAddMultiBatch_ConcurrentWithCompression(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	doc := func(id uint64) [][]float32 {
		return testMultiVectors[id%uint64(len(testMultiVectors))]
	}

	uc := ent.UserConfig{
		VectorCacheMaxObjects: 1e12,
		MaxConnections:        8,
		EFConstruction:        64,
		EF:                    64,
		Multivector:           ent.MultivectorConfig{Enabled: true},
	}
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "multivec-compress-concurrency",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return doc(id), nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })

	ctx := context.Background()
	const seed = 500
	for id := uint64(0); id < seed; id++ {
		require.NoError(t, index.AddMulti(ctx, id, doc(id)))
	}
	index.cachePrefilled.Store(true)

	// Writers add new docs (growing the index) throughout the rebuild.
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(start uint64) {
			defer wg.Done()
			id := start
			for {
				select {
				case <-done:
					return
				default:
					_ = index.AddMulti(ctx, id, doc(id))
					id++
				}
			}
		}(uint64(seed + w*1_000_000))
	}

	// Enable BQ compression; Upgrade runs compress() on its goroutine.
	bq := uc
	bq.BQ = ent.BQConfig{Enabled: true}
	cbDone := make(chan struct{})
	require.NoError(t, index.UpdateUserConfig(bq, func() { close(cbDone) }))
	<-cbDone

	close(done)
	wg.Wait()
}
