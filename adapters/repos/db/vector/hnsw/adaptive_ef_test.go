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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestComputeScore(t *testing.T) {
	t.Run("empty distances returns 0", func(t *testing.T) {
		score := ComputeScore([]float32{1, 0, 0}, nil, []float64{0.5, 0.5, 0.5}, []float64{0.1, 0.1, 0.1})
		assert.Equal(t, float32(0), score)
	})

	t.Run("empty query returns 0", func(t *testing.T) {
		score := ComputeScore(nil, []float32{0.1, 0.2}, []float64{}, []float64{})
		assert.Equal(t, float32(0), score)
	})

	t.Run("zero variance returns 0", func(t *testing.T) {
		query := []float32{1, 0, 0}
		distances := []float32{0.5, 0.6, 0.7}
		meanVec := []float64{0.5, 0.5, 0.5}
		varianceVec := []float64{0, 0, 0}

		score := ComputeScore(query, distances, meanVec, varianceVec)
		assert.Equal(t, float32(0), score)
	})

	t.Run("all distances far from mean gives low score", func(t *testing.T) {
		// Query near the mean, distances near mean -> none in left tail
		query := []float32{0.577, 0.577, 0.577} // ~unit vector
		meanVec := []float64{0.577, 0.577, 0.577}
		varianceVec := []float64{0.01, 0.01, 0.01}

		// Mean distance = 1 - dot(q, mean) ≈ 1 - 1 = 0
		// Distances close to 0 (near mean) should be near the expected mean
		// distance. Thresholds are far below, so nothing falls in bins.
		distances := make([]float32, 100)
		for i := range distances {
			distances[i] = 0.001 // close to 0 = close to expected mean distance
		}

		score := ComputeScore(query, distances, meanVec, varianceVec)
		// These distances are near the expected mean (0), not in the left tail
		// so score should be low or zero
		assert.True(t, score < 10, "score should be low, got %f", score)
	})

	t.Run("distances in left tail give higher score", func(t *testing.T) {
		// Create a scenario where some distances fall below the threshold
		query := []float32{1, 0, 0}
		meanVec := []float64{0, 0, 0} // mean at origin
		varianceVec := []float64{0.1, 0.1, 0.1}

		// Expected mean distance = 1 - dot([1,0,0], [0,0,0]) = 1
		// Variance = 1^2 * 0.1 = 0.1, std = sqrt(0.1) ≈ 0.316
		// Threshold[0] = 1 + (-3.09) * 0.316 ≈ 1 - 0.976 ≈ 0.024
		// Any distance below 0.024 falls into bin 0

		distances := make([]float32, 100)
		for i := range distances {
			distances[i] = 0.01 // Well below threshold[0] ≈ 0.024
		}

		score := ComputeScore(query, distances, meanVec, varianceVec)
		// All 100 distances are in bin 0 with weight 100
		// score ≈ (100/100) * 100 = 100
		assert.True(t, score > 90, "score should be high when all distances are in the tightest bin, got %f", score)
	})
}

func TestEstimateEF(t *testing.T) {
	cfg := &AdaptiveEFConfig{
		TargetRecall: 0.95,
		WAE:          50,
		Table: []EFTableEntry{
			{Score: 10, EFRecalls: []EFRecall{{EF: 20, Recall: 0.8}, {EF: 50, Recall: 0.96}}},
			{Score: 50, EFRecalls: []EFRecall{{EF: 30, Recall: 0.85}, {EF: 100, Recall: 0.97}}},
			{Score: 90, EFRecalls: []EFRecall{{EF: 10, Recall: 0.99}}},
		},
	}
	cfg.buildSketch()

	t.Run("low score maps to higher ef", func(t *testing.T) {
		ef := cfg.EstimateEF(10)
		// Score 10 needs ef=50 to hit 0.96 >= 0.95 target
		assert.True(t, ef >= 50, "ef should be at least 50, got %d", ef)
	})

	t.Run("high score maps to lower ef", func(t *testing.T) {
		ef := cfg.EstimateEF(90)
		// Score 90 needs ef=10 to hit 0.99 >= 0.95 target,
		// but WAE=50 is the floor
		assert.True(t, ef >= 50, "ef should be at least WAE=50, got %d", ef)
	})

	t.Run("boundary scores", func(t *testing.T) {
		ef0 := cfg.EstimateEF(0)
		assert.True(t, ef0 > 0, "ef at score 0 should be positive")

		ef100 := cfg.EstimateEF(100)
		assert.True(t, ef100 > 0, "ef at score 100 should be positive")
	})
}

func TestSketch(t *testing.T) {
	cfg := &AdaptiveEFConfig{
		Table: []EFTableEntry{
			{Score: 0},
			{Score: 25},
			{Score: 50},
			{Score: 75},
			{Score: 100},
		},
	}
	cfg.buildSketch()

	// Score 0 should map to entry index 0
	assert.Equal(t, 0, cfg.Links[0])
	// Score 25 should map to entry index 1
	assert.Equal(t, 1, cfg.Links[25])
	// Score 50 should map to entry index 2
	assert.Equal(t, 2, cfg.Links[50])
	// Score 12 should map to entry index 0 or 1 (nearest)
	assert.True(t, cfg.Links[12] == 0 || cfg.Links[12] == 1)
	// Score 100 should map to entry index 4
	assert.Equal(t, 4, cfg.Links[100])
}

func TestComputeRecall(t *testing.T) {
	t.Run("perfect recall", func(t *testing.T) {
		result := []uint64{1, 2, 3, 4, 5}
		gt := []uint64{1, 2, 3, 4, 5}
		recall := computeRecall(result, gt, 5)
		assert.Equal(t, float32(1.0), recall)
	})

	t.Run("50% recall", func(t *testing.T) {
		result := []uint64{1, 2, 6, 7, 8}
		gt := []uint64{1, 2, 3, 4}
		recall := computeRecall(result, gt, 4)
		assert.Equal(t, float32(0.5), recall)
	})

	t.Run("zero recall", func(t *testing.T) {
		result := []uint64{10, 20, 30}
		gt := []uint64{1, 2, 3}
		recall := computeRecall(result, gt, 3)
		assert.Equal(t, float32(0), recall)
	})
}

func TestStatisticsLength(t *testing.T) {
	// For M0=32: 1 + 32 + 31*32 = 1 + 32 + 992 = 1025
	assert.Equal(t, 1025, StatisticsLength(32))

	// For M0=64: 1 + 64 + 63*64 = 1 + 64 + 4032 = 4097
	assert.Equal(t, 4097, StatisticsLength(64))
}

func TestAdaptiveSearchEndToEnd(t *testing.T) {
	// Create a small index with random vectors and verify adaptive search works
	dims := 32
	numVectors := 500
	rng := rand.New(rand.NewSource(42))

	// Generate random normalized vectors
	vectors := make([][]float32, numVectors)
	for i := range vectors {
		v := make([]float32, dims)
		var norm float64
		for d := range v {
			v[d] = rng.Float32()*2 - 1
			norm += float64(v[d]) * float64(v[d])
		}
		norm = math.Sqrt(norm)
		for d := range v {
			v[d] /= float32(norm)
		}
		vectors[i] = v
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "adaptive-ef-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    64,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)

	// Insert vectors
	ctx := context.Background()
	for i := 0; i < numVectors; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Compute dataset statistics
	meanVec := make([]float64, dims)
	for _, v := range vectors {
		for d, val := range v {
			meanVec[d] += float64(val)
		}
	}
	for d := range meanVec {
		meanVec[d] /= float64(numVectors)
	}

	varianceVec := make([]float64, dims)
	for _, v := range vectors {
		for d, val := range v {
			diff := float64(val) - meanVec[d]
			varianceVec[d] += diff * diff
		}
	}
	for d := range varianceVec {
		varianceVec[d] /= float64(numVectors)
	}

	// Sample queries (use first 20 vectors as queries)
	numQueries := 20
	sampleQueries := vectors[:numQueries]

	// Compute brute-force ground truth
	k := 10
	groundTruth := make([][]uint64, numQueries)
	for qi := 0; qi < numQueries; qi++ {
		dist := distancer.NewCosineDistanceProvider().New(sampleQueries[qi])
		type idDist struct {
			id   uint64
			dist float32
		}
		var topK []idDist
		for vi := 0; vi < numVectors; vi++ {
			d, _ := dist.Distance(vectors[vi])
			if len(topK) < k {
				topK = append(topK, idDist{uint64(vi), d})
				// Sort
				for j := len(topK) - 1; j > 0; j-- {
					if topK[j].dist < topK[j-1].dist {
						topK[j], topK[j-1] = topK[j-1], topK[j]
					}
				}
			} else if d < topK[k-1].dist {
				topK[k-1] = idDist{uint64(vi), d}
				for j := k - 1; j > 0; j-- {
					if topK[j].dist < topK[j-1].dist {
						topK[j], topK[j-1] = topK[j-1], topK[j]
					}
				}
			}
		}
		gt := make([]uint64, len(topK))
		for i, item := range topK {
			gt[i] = item.id
		}
		groundTruth[qi] = gt
	}

	// Build adaptive ef table
	err = index.BuildAdaptiveEFTable(ctx, sampleQueries, groundTruth, k, 0.9, meanVec, varianceVec)
	require.NoError(t, err)

	// Verify adaptive ef config is loaded
	cfg := index.adaptiveEf.Load()
	require.NotNil(t, cfg, "adaptive ef config should be loaded")
	assert.True(t, len(cfg.Table) > 0, "table should have entries")
	assert.True(t, cfg.WAE > 0, "WAE should be positive")

	_, adaptive := index.searchTimeEF(k)
	assert.True(t, adaptive, "searchTimeEF should indicate adaptive mode when adaptive ef is loaded")

	// Do actual searches and verify they return results
	for qi := 0; qi < numQueries; qi++ {
		ids, dists, err := index.SearchByVector(ctx, sampleQueries[qi], k, nil)
		require.NoError(t, err)
		assert.True(t, len(ids) > 0, "search should return results")
		assert.Equal(t, len(ids), len(dists), "ids and dists should have same length")

		// Check recall against ground truth
		recall := computeRecall(ids, groundTruth[qi], k)
		assert.True(t, recall >= 0.5, "recall should be reasonable (>= 0.5), got %f for query %d", recall, qi)
	}
}
