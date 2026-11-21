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

package flat

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// TestFlatRescoreCandidates checks how many of the best compressed candidates
// a quantized flat search rescores, max(k, rescoreLimit,
// ceil(k * rescoreFactor)), for each quantizer, both when the index is built
// and after a live config update. The automatic factor is 1, which keeps the
// old max(k, rescoreLimit).
func TestFlatRescoreCandidates(t *testing.T) {
	quantizers := []struct {
		name      string
		configure func(uc *flatent.UserConfig, limit int, factor float64)
	}{
		{"bq", func(uc *flatent.UserConfig, limit int, factor float64) {
			uc.BQ.Enabled = true
			uc.BQ.RescoreLimit = limit
			uc.BQ.RescoreFactor = factor
		}},
		{"rq-1", func(uc *flatent.UserConfig, limit int, factor float64) {
			uc.RQ.Enabled = true
			uc.RQ.Bits = 1
			uc.RQ.RescoreLimit = limit
			uc.RQ.RescoreFactor = factor
		}},
		{"rq-8", func(uc *flatent.UserConfig, limit int, factor float64) {
			uc.RQ.Enabled = true
			uc.RQ.Bits = 8
			uc.RQ.RescoreLimit = limit
			uc.RQ.RescoreFactor = factor
		}},
	}

	auto := common.RescoreFactorAuto
	shapes := []struct {
		name     string
		limit    int
		factor   float64
		k        int
		expected int
	}{
		{name: "automatic limit and factor rescore k", limit: flatent.DefaultCompressionRescore, factor: auto, k: 10, expected: 10},
		{name: "limit above k with automatic factor", limit: 50, factor: auto, k: 10, expected: 50},
		{name: "limit below k with automatic factor", limit: 5, factor: auto, k: 10, expected: 10},
		{name: "limit 0 rescores k", limit: 0, factor: auto, k: 10, expected: 10},
		{name: "zero factor is automatic", limit: 0, factor: 0, k: 10, expected: 10},
		{name: "factor scales k", limit: flatent.DefaultCompressionRescore, factor: 4, k: 10, expected: 40},
		{name: "fractional factor rounds up", limit: 0, factor: 1.5, k: 15, expected: 23},
		{name: "limit above scaled k", limit: 100, factor: 2, k: 10, expected: 100},
		{name: "scaled k above limit", limit: 20, factor: 3, k: 10, expected: 30},
	}

	for _, q := range quantizers {
		for _, shape := range shapes {
			t.Run(fmt.Sprintf("%s %s", q.name, shape.name), func(t *testing.T) {
				uc := flatent.NewDefaultUserConfig()
				q.configure(&uc, shape.limit, shape.factor)
				index, err := New(Config{
					ID:                "id",
					RootPath:          t.TempDir(),
					DistanceProvider:  distancer.NewCosineDistanceProvider(),
					MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
				}, uc, testinghelpers.NewDummyStore(t))
				require.NoError(t, err)
				assert.Equal(t, shape.expected, index.searchTimeRescore(shape.k), "built")

				// a live update reaches searches, and a second one replaces it
				updated := flatent.NewDefaultUserConfig()
				q.configure(&updated, 1, 7)
				require.NoError(t, index.UpdateUserConfig(updated, func() {}))
				assert.Equal(t, 7*shape.k, index.searchTimeRescore(shape.k), "updated")

				require.NoError(t, index.UpdateUserConfig(uc, func() {}))
				assert.Equal(t, shape.expected, index.searchTimeRescore(shape.k), "reverted")
			})
		}
	}
}

// TestFlatRescoreBudgetDoesNotPreallocate searches a small quantized index
// with a rescore budget far larger than the index. The budget only bounds how
// many candidates are kept, so the search must allocate by the candidates it
// finds, not by the budget.
func TestFlatRescoreBudgetDoesNotPreallocate(t *testing.T) {
	const (
		objects = 50
		dims    = 16
		k       = 10
		// 10M candidates is ~320MB of heap items if allocated up front
		budget = 10_000_000
	)
	vectors := make([][]float32, objects)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		vectors[i][i%dims] = 1
		vectors[i][(i+1)%dims] = float32(i) / objects
	}

	cases := []struct {
		name      string
		configure func(uc *flatent.UserConfig)
	}{
		{"bq large factor", func(uc *flatent.UserConfig) {
			uc.BQ.Enabled = true
			uc.BQ.RescoreFactor = budget / k
		}},
		{"bq large limit", func(uc *flatent.UserConfig) {
			uc.BQ.Enabled = true
			uc.BQ.RescoreLimit = budget
		}},
		{"rq-8 large factor", func(uc *flatent.UserConfig) {
			uc.RQ.Enabled = true
			uc.RQ.Bits = 8
			uc.RQ.RescoreFactor = budget / k
		}},
		{"rq-8 large limit", func(uc *flatent.UserConfig) {
			uc.RQ.Enabled = true
			uc.RQ.Bits = 8
			uc.RQ.RescoreLimit = budget
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc := flatent.NewDefaultUserConfig()
			tc.configure(&uc)
			index, err := New(Config{
				ID:                "id",
				RootPath:          t.TempDir(),
				DistanceProvider:  distancer.NewCosineDistanceProvider(),
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, uc, testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			for i, v := range vectors {
				require.NoError(t, index.Add(t.Context(), uint64(i), v))
			}
			require.True(t, index.Compressed())
			require.Equal(t, budget, index.searchTimeRescore(k))

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			ids, _, err := index.SearchByVector(t.Context(), vectors[7], k, nil)
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Len(t, ids, k)
			assert.Equal(t, uint64(7), ids[0])

			allocated := after.TotalAlloc - before.TotalAlloc
			assert.Less(t, allocated, uint64(32<<20), "search allocated %d bytes for %d objects", allocated, objects)
		})
	}
}

// TestFlatRescoreSearch runs quantized searches with a rescoreFactor whose
// candidate count exceeds the indexed objects, so every object is rescored
// and the results are the exact top k.
func TestFlatRescoreSearch(t *testing.T) {
	const (
		objects = 50
		dims    = 16
		k       = 5
	)
	vectors := make([][]float32, objects)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		vectors[i][i%dims] = 1
		vectors[i][(i+1)%dims] = float32(i) / objects
	}
	query := vectors[7]

	for _, name := range []string{"bq", "rq-8"} {
		t.Run(name, func(t *testing.T) {
			uc := flatent.NewDefaultUserConfig()
			if name == "bq" {
				uc.BQ.Enabled = true
				uc.BQ.RescoreFactor = objects
			} else {
				uc.RQ.Enabled = true
				uc.RQ.Bits = 8
				uc.RQ.RescoreFactor = objects
			}
			index, err := New(Config{
				ID:                "id",
				RootPath:          t.TempDir(),
				DistanceProvider:  distancer.NewCosineDistanceProvider(),
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, uc, testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			for i, v := range vectors {
				require.NoError(t, index.Add(t.Context(), uint64(i), v))
			}
			require.True(t, index.Compressed())

			ids, dists, err := index.SearchByVector(t.Context(), query, k, nil)
			require.NoError(t, err)
			require.Len(t, ids, k)
			assert.Equal(t, uint64(7), ids[0], "the query vector itself is the exact nearest")
			assert.InDelta(t, 0, dists[0], 1e-5, "rescored distances are exact")
		})
	}
}
