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
	"math"
	"math/rand/v2"
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

// biasedUnitVecs generates unit vectors sharing a strong common component —
// the anisotropic case centering exists for. Uncentered 1-bit codes lose
// badly here; centered codes must not.
func biasedUnitVecs(n, dim int, seed uint64) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, 31))
	bias := make([]float64, dim)
	var bn float64
	for i := range bias {
		bias[i] = rng.NormFloat64()
		bn += bias[i] * bias[i]
	}
	bn = math.Sqrt(bn)
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, dim)
		var norm float64
		for j := range v {
			x := 0.5*bias[j]/bn + 0.5*rng.NormFloat64()/math.Sqrt(float64(dim))
			v[j] = float32(x)
			norm += x * x
		}
		norm = math.Sqrt(norm)
		for j := range v {
			v[j] = float32(float64(v[j]) / norm)
		}
		out[i] = v
	}
	return out
}

// Enabling RQ with bits=1 and centering must train the mean over the
// deferred-activation sample, compress the index, and keep recall high.
// This exercises the full wiring: config validation (bits ∈ {1,4} for
// centering), the deferred compress() arm, the centered rq1 factory, and
// the compressed search path with rescoring.
func Test_CompressCenteredRQ1RecallAfterCompression(t *testing.T) {
	ctx := context.Background()
	dimensions := 256
	vectorsSize := 1000
	queriesSize := 20
	k := 10

	all := biasedUnitVecs(vectorsSize+queriesSize, dimensions, 7)
	vectors, queries := all[:vectorsSize], all[vectorsSize:]
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
		ID:                    "rq1-centered-recall-test",
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
		Enabled:       true,
		Bits:          1,
		Centering:     true,
		TrainingLimit: 500,
		RescoreLimit:  100,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, index.UpdateUserConfig(uc, wg.Done))
	wg.Wait()
	require.True(t, index.Compressed(), "index must be compressed after enabling centered rq1")

	stats := index.CompressionStats()
	assert.Equal(t, "rq", stats.CompressionType())

	var hits, wanted int
	for i, q := range queries {
		ids, _, err := index.SearchByVector(ctx, q, k, nil)
		require.NoError(t, err)
		truth := map[uint64]bool{}
		for _, id := range truths[i] {
			truth[id] = true
		}
		for _, id := range ids {
			if truth[id] {
				hits++
			}
		}
		wanted += k
	}
	recall := float64(hits) / float64(wanted)
	t.Logf("centered rq1 recall@%d after compression: %.4f", k, recall)
	assert.Greater(t, recall, 0.85, "centered rq1 with rescore must keep recall high on biased data")
}
