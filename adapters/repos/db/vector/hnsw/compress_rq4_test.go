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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
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

// Enabling RQ with bits=4 on an HNSW index must compress the index and keep
// search recall high (the compressed candidates are rescored with full
// vectors). This exercises the full wiring: config validation, the compressor
// factory dispatch on bits, and the compressed search path.
func Test_CompressRQ4RecallAfterCompression(t *testing.T) {
	ctx := context.Background()
	dimensions := 256
	vectorsSize := 1000
	queriesSize := 20
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	provider := distancer.NewCosineDistanceProvider()
	logger, _ := test.NewNullLogger()

	truths := make([][]uint64, queriesSize)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, func(x, y []float32) float32 {
			dist, _ := provider.SingleDist(x, y)
			return dist
		})
	}

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 32
	uc.EFConstruction = 64
	uc.EF = 100
	uc.VectorCacheMaxObjects = 10e12

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "rq4-recall-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      provider,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		AllocChecker:      memwatch.NewDummyMonitor(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(context.Background())
	})
	index.PostStartup(ctx)

	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	}))

	uc.RQ = ent.RQConfig{
		Enabled:      true,
		Bits:         4,
		RescoreLimit: ent.DefaultRQRescoreLimit,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, index.UpdateUserConfig(uc, wg.Done))
	wg.Wait()
	require.True(t, index.Compressed(), "index should be compressed after enabling RQ with bits=4")

	stats := index.CompressionStats()
	assert.Equal(t, "rq", stats.CompressionType())
	rq4Stats, ok := stats.(compressionhelpers.RQ4Stats)
	require.True(t, ok, "expected RQ4Stats, got %T", stats)
	assert.Equal(t, uint32(4), rq4Stats.Bits)

	var relevant, retrieved int
	for i := range queries {
		results, _, err := index.SearchByVector(ctx, queries[i], k, nil)
		require.NoError(t, err)
		retrieved += k
		relevant += int(testinghelpers.MatchesInLists(truths[i], results))
	}
	recall := float32(relevant) / float32(retrieved)
	assert.Greater(t, recall, float32(0.8), "recall %f too low for 4-bit RQ with rescoring", recall)
}

func Test_CompressRQ4CenteredDeferredActivation(t *testing.T) {
	ctx := context.Background()
	dimensions := 128
	vectorsSize := 1000
	queriesSize := 20
	trainingLimit := 500
	k := 10

	// Mean-dominated ("cone") data: the regime centering is designed for.
	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	for _, v := range vectors {
		for i := range v {
			v[i] += 2.0
		}
	}
	for _, q := range queries {
		for i := range q {
			q[i] += 2.0
		}
	}
	provider := distancer.NewDotProductProvider()
	logger, _ := test.NewNullLogger()

	truths := make([][]uint64, queriesSize)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, func(x, y []float32) float32 {
			dist, _ := provider.SingleDist(x, y)
			return dist
		})
	}

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 32
	uc.EFConstruction = 64
	uc.EF = 100
	uc.VectorCacheMaxObjects = 10e12
	uc.RQ = ent.RQConfig{
		Enabled:       true,
		Bits:          4,
		RescoreLimit:  ent.DefaultRQRescoreLimit,
		Centering:     true,
		TrainingLimit: trainingLimit,
	}

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "rq4-centered-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      provider,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		AllocChecker:      memwatch.NewDummyMonitor(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(context.Background())
	})
	index.PostStartup(ctx)

	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	}))

	// Unlike uncentered RQ, inserting data must not have compressed the index.
	require.False(t, index.Compressed(),
		"centered RQ must not compress on insert; it needs the deferred training pass")

	// The async-indexing queue drives activation through ShouldUpgrade.
	shouldUpgrade, upgradeAt := index.ShouldUpgrade()
	require.True(t, shouldUpgrade)
	require.Equal(t, trainingLimit, upgradeAt)

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, index.Upgrade(wg.Done))
	wg.Wait()
	require.True(t, index.Compressed(), "index should be compressed after the deferred upgrade")

	stats := index.CompressionStats()
	rq4Stats, ok := stats.(compressionhelpers.RQ4Stats)
	require.True(t, ok, "expected RQ4Stats, got %T", stats)
	assert.Equal(t, uint32(4), rq4Stats.Bits)
	assert.True(t, rq4Stats.Centering, "stats must report centering")

	var relevant, retrieved int
	for i := range queries {
		results, _, err := index.SearchByVector(ctx, queries[i], k, nil)
		require.NoError(t, err)
		retrieved += k
		relevant += int(testinghelpers.MatchesInLists(truths[i], results))
	}
	recall := float32(relevant) / float32(retrieved)
	assert.Greater(t, recall, float32(0.8), "recall %f too low for centered 4-bit RQ with rescoring", recall)
}
