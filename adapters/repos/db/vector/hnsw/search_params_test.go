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
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/searchparams"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func Test_searchTimeEF_Override(t *testing.T) {
	dynamic := ent.UserConfig{
		VectorCacheMaxObjects: 10,
		EF:                    -1,
		DynamicEFMin:          100,
		DynamicEFMax:          500,
		DynamicEFFactor:       8,
	}
	fixed := ent.UserConfig{VectorCacheMaxObjects: 10, EF: 64}

	tests := []struct {
		name     string
		config   ent.UserConfig
		k        int
		params   *searchparams.VectorIndexParams
		expected int
	}{
		{"nil params, dynamic ef", dynamic, 10, nil, 100},
		{"zero ef, dynamic ef", dynamic, 10, &searchparams.VectorIndexParams{}, 100},
		{"nil params, fixed ef", fixed, 10, nil, 64},
		{"override above k", fixed, 10, &searchparams.VectorIndexParams{EF: 800}, 800},
		{"override below collection ef", fixed, 10, &searchparams.VectorIndexParams{EF: 20}, 20},
		{"override below k", fixed, 100, &searchparams.VectorIndexParams{EF: 10}, 100},
		{"override with dynamic ef", dynamic, 10, &searchparams.VectorIndexParams{EF: 33}, 33},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "search-params-ef-test",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return nil, errors.Errorf("not implemented")
				},
				GetViewThunk: func() common.BucketView { return &dynamicEFNoopBucketView{} },
			}, tt.config, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)

			assert.Equal(t, tt.expected, index.searchTimeEF(tt.k, tt.params))
			require.NoError(t, index.Drop(context.Background()))
		})
	}
}

func Test_SearchByVector_RequestEFMatchesConfiguredEF(t *testing.T) {
	// Whether a request ef lands on the same search as the configured ef does
	// not depend on the graph size, so the two graphs stay small: 400 inserts
	// each is what the neighbouring tests use under -race.
	const (
		size    = 400
		queries = 20
		dims    = 16
		k       = 10
	)
	ctx := context.Background()
	vectors, queryVecs := testinghelpers.RandomVecsFixedSeed(size, queries, dims)

	build := func(t *testing.T, cfg ent.UserConfig) *hnsw {
		store := testinghelpers.NewDummyStore(t)
		t.Cleanup(func() { store.Shutdown(ctx) })
		index, err := New(Config{
			RootPath:              t.TempDir(),
			ID:                    "search-params-ef-behaviour",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			AllocChecker:          memwatch.NewDummyMonitor(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
			TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		}, cfg, cyclemanager.NewCallbackGroupNoop(), store)
		require.NoError(t, err)
		t.Cleanup(func() { index.Shutdown(ctx) })

		// identical graphs: deterministic levels and sequential insertion
		r := rand.New(rand.NewSource(42))
		index.randFunc = r.Float64
		for id, v := range vectors {
			require.NoError(t, index.Add(ctx, uint64(id), v))
		}
		return index
	}

	base := ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		VectorCacheMaxObjects: 100000,
	}
	cfgA := base
	cfgA.EF = -1 // dynamic ef defaults
	cfgA.DynamicEFMin, cfgA.DynamicEFMax, cfgA.DynamicEFFactor = 100, 500, 8
	cfgB := base
	cfgB.EF = 64

	idxA := build(t, cfgA)
	idxB := build(t, cfgB)

	t.Run("request ef equals configured ef", func(t *testing.T) {
		for i, q := range queryVecs {
			idsA, distsA, err := idxA.SearchByVector(ctx, q, k, nil, &searchparams.VectorIndexParams{EF: 64})
			require.NoError(t, err)
			idsB, distsB, err := idxB.SearchByVector(ctx, q, k, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, idsB, idsA, "query %d ids", i)
			assert.Equal(t, distsB, distsA, "query %d distances", i)
		}
	})

	t.Run("request ef changes the beam width", func(t *testing.T) {
		differs := 0
		for _, q := range queryVecs {
			idsDefault, _, err := idxA.SearchByVector(ctx, q, k, nil, nil)
			require.NoError(t, err)
			idsNarrow, _, err := idxA.SearchByVector(ctx, q, k, nil, &searchparams.VectorIndexParams{EF: k})
			require.NoError(t, err)
			if !assert.ObjectsAreEqual(idsDefault, idsNarrow) {
				differs++
			}
		}
		assert.Positive(t, differs, "ef=k and dynamic ef=100 should disagree on at least one query")
	})

	t.Run("ef below k still returns k results", func(t *testing.T) {
		for i, q := range queryVecs {
			ids, _, err := idxA.SearchByVector(ctx, q, k, nil, &searchparams.VectorIndexParams{EF: 3})
			require.NoError(t, err)
			assert.Len(t, ids, k, "query %d", i)
		}
	})

	// The slow-query log must name the requested ef on every path, but report
	// an effective beam only where a graph walk happened: brute force has none.
	t.Run("slow-query annotations", func(t *testing.T) {
		q := queryVecs[0]
		params := &searchparams.VectorIndexParams{EF: 64}
		search := func(allow helpers.AllowList, params *searchparams.VectorIndexParams) map[string]any {
			ctx := helpers.InitSlowQueryDetails(context.Background())
			_, _, err := idxA.SearchByVector(ctx, q, k, allow, params)
			require.NoError(t, err)
			return helpers.ExtractSlowQueryDetails(ctx)
		}

		details := search(nil, nil)
		assert.NotContains(t, details, "hnsw_ef_requested", "default path must not annotate")
		assert.NotContains(t, details, "hnsw_ef_effective", "default path must not annotate")

		details = search(nil, params)
		assert.Equal(t, 64, details["hnsw_ef_requested"])
		assert.Equal(t, 64, details["hnsw_ef_effective"])
		assert.Equal(t, false, details["hnsw_flat_search"])

		prev := atomic.LoadInt64(&idxA.flatSearchCutoff)
		atomic.StoreInt64(&idxA.flatSearchCutoff, 1000)
		defer atomic.StoreInt64(&idxA.flatSearchCutoff, prev)
		details = search(helpers.NewAllowList(1, 2, 3, 4, 5), params)
		assert.Equal(t, 64, details["hnsw_ef_requested"])
		assert.NotContains(t, details, "hnsw_ef_effective", "brute force walks no graph")
		assert.Equal(t, true, details["hnsw_flat_search"])
	})

	t.Run("search by distance applies the request ef", func(t *testing.T) {
		sctx := helpers.InitSlowQueryDetails(context.Background())
		ids, _, err := idxA.SearchByVectorDistance(sctx, queryVecs[0], 1e9, 50, nil, &searchparams.VectorIndexParams{EF: 300})
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		details := helpers.ExtractSlowQueryDetails(sctx)
		assert.Equal(t, 300, details["hnsw_ef_requested"])
		assert.Equal(t, 300, details["hnsw_ef_effective"], "every step searches max(ef, step limit) with step limits below 300")
	})

	// A multi-target request searches several indexes on one context, each
	// with its own merged params: every value must survive, not the last one.
	t.Run("slow-query annotations keep every target's value", func(t *testing.T) {
		q := queryVecs[0]
		ctx := helpers.InitSlowQueryDetails(context.Background())
		for _, p := range []*searchparams.VectorIndexParams{
			{EF: 128, FilterStrategy: searchparams.FilterStrategySweeping},
			{EF: 64, FilterStrategy: searchparams.FilterStrategyAcorn},
			{EF: 128, FilterStrategy: searchparams.FilterStrategySweeping},
		} {
			_, _, err := idxA.SearchByVector(ctx, q, k, nil, p)
			require.NoError(t, err)
		}
		details := helpers.ExtractSlowQueryDetails(ctx)
		assert.Equal(t, []int{64, 128}, details["hnsw_ef_requested"])
		assert.Equal(t, []int{64, 128}, details["hnsw_ef_effective"])
		assert.Equal(t, []string{"acorn", "sweeping"}, details["hnsw_filter_strategy_requested"])

		ctx = helpers.InitSlowQueryDetails(context.Background())
		for range 2 {
			_, _, err := idxA.SearchByVector(ctx, q, k, nil, &searchparams.VectorIndexParams{EF: 64})
			require.NoError(t, err)
		}
		details = helpers.ExtractSlowQueryDetails(ctx)
		assert.Equal(t, 64, details["hnsw_ef_requested"], "one distinct value is reported as a scalar")
	})
}

func Test_effectiveFilterStrategy(t *testing.T) {
	index := &hnsw{}

	tests := []struct {
		name       string
		configured FilterStrategy
		params     *searchparams.VectorIndexParams
		expected   FilterStrategy
	}{
		{"nil params, configured sweeping", SWEEPING, nil, SWEEPING},
		{"nil params, configured acorn", ACORN, nil, ACORN},
		{"nil params, configured pathseer", PATHSEER, nil, PATHSEER},
		{"empty strategy keeps configured", ACORN, &searchparams.VectorIndexParams{EF: 10}, ACORN},
		{"request sweeping over acorn", ACORN, &searchparams.VectorIndexParams{FilterStrategy: searchparams.FilterStrategySweeping}, SWEEPING},
		{"request acorn over sweeping", SWEEPING, &searchparams.VectorIndexParams{FilterStrategy: searchparams.FilterStrategyAcorn}, ACORN},
		{"request pathseer over acorn", ACORN, &searchparams.VectorIndexParams{FilterStrategy: searchparams.FilterStrategyPathseer}, PATHSEER},
		{"request same as configured", PATHSEER, &searchparams.VectorIndexParams{FilterStrategy: searchparams.FilterStrategyPathseer}, PATHSEER},
		// a newer coordinator may name a strategy this node does not know yet
		// (rolling upgrade); the request is not downgraded to sweeping behind
		// the caller's back, the collection's configured strategy runs instead
		{"unknown request strategy keeps configured", ACORN, &searchparams.VectorIndexParams{FilterStrategy: "pruned"}, ACORN},
		{"unknown request strategy keeps configured pathseer", PATHSEER, &searchparams.VectorIndexParams{FilterStrategy: "pruned"}, PATHSEER},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index.configuredFilterStrategy.Store(int32(tt.configured))
			assert.Equal(t, tt.expected, index.effectiveFilterStrategy(tt.params))
		})
	}
}

func Test_FilterStrategy_String(t *testing.T) {
	tests := []struct {
		strategy FilterStrategy
		expected string
	}{
		{SWEEPING, "sweeping"},
		{ACORN, "acorn"},
		{RRE, "rre"},
		{PATHSEER, "pathseer"},
		{FilterStrategy(99), "unknown"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.strategy.String())
	}
}

// Test_SearchByVector_FilterStrategyOverride checks that a per-request filter
// strategy replaces the collection's configured one for that search only, that
// the slow-query log names both the requested strategy and the one that ran
// (after fallbacks), and that the default path does not annotate at all.
func Test_SearchByVector_FilterStrategyOverride(t *testing.T) {
	const (
		size = 300
		dims = 8
		k    = 5
	)
	ctx := context.Background()
	vectors, queryVecs := testinghelpers.RandomVecsFixedSeed(size, 1, dims)
	query := queryVecs[0]

	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "search-params-filter-strategy",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		AcornFilterRatio:      0.4,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    size, // beam covers the whole graph: every strategy is exact
		VectorCacheMaxObjects: 100000,
		FilterStrategy:        ent.FilterStrategyAcorn,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })
	r := rand.New(rand.NewSource(42))
	index.randFunc = r.Float64
	for id, v := range vectors {
		require.NoError(t, index.Add(ctx, uint64(id), v))
	}
	require.Equal(t, ACORN, FilterStrategy(index.configuredFilterStrategy.Load()))

	allowed := []uint64{3, 17, 42, 77, 101, 150, 199, 230, 261, 299}
	allowList := helpers.NewAllowList(allowed...)

	// Ground truth for the filtered query by brute force over the allow-list.
	l2 := distancer.NewL2SquaredProvider()
	type cand struct {
		id   uint64
		dist float32
	}
	cands := make([]cand, 0, len(allowed))
	for _, id := range allowed {
		d, err := l2.SingleDist(query, vectors[id])
		require.NoError(t, err)
		cands = append(cands, cand{id, d})
	}
	for i := range cands {
		for j := i + 1; j < len(cands); j++ {
			if cands[j].dist < cands[i].dist {
				cands[i], cands[j] = cands[j], cands[i]
			}
		}
	}
	wantFiltered := make([]uint64, k)
	for i := range wantFiltered {
		wantFiltered[i] = cands[i].id
	}
	wantUnfiltered, _, err := index.SearchByVector(ctx, query, k, nil, nil)
	require.NoError(t, err)

	strategy := func(s string) *searchparams.VectorIndexParams {
		return &searchparams.VectorIndexParams{FilterStrategy: s}
	}

	tests := []struct {
		name          string
		configured    FilterStrategy
		flatCutoff    int64
		allowList     helpers.AllowList
		params        *searchparams.VectorIndexParams
		wantRequested string // "" => key must be absent
		wantEffective string // "" => key must be absent
		wantFlat      bool
	}{
		{
			name:       "nil params keeps configured acorn and does not annotate",
			configured: ACORN, flatCutoff: 1, allowList: allowList,
		},
		{
			name:       "empty strategy does not annotate",
			configured: ACORN, flatCutoff: 1, allowList: allowList,
			params: &searchparams.VectorIndexParams{EF: size},
		},
		{
			name:       "sweeping overrides configured acorn",
			configured: ACORN, flatCutoff: 1, allowList: allowList,
			params:        strategy(searchparams.FilterStrategySweeping),
			wantRequested: "sweeping", wantEffective: "sweeping",
		},
		{
			name:       "pathseer overrides configured acorn",
			configured: ACORN, flatCutoff: 1, allowList: allowList,
			params:        strategy(searchparams.FilterStrategyPathseer),
			wantRequested: "pathseer", wantEffective: "pathseer",
		},
		{
			name:       "acorn overrides configured sweeping",
			configured: SWEEPING, flatCutoff: 1, allowList: allowList,
			params:        strategy(searchparams.FilterStrategyAcorn),
			wantRequested: "acorn", wantEffective: "acorn",
		},
		{
			name:       "acorn without a filter runs sweeping",
			configured: SWEEPING, flatCutoff: 1, allowList: nil,
			params:        strategy(searchparams.FilterStrategyAcorn),
			wantRequested: "acorn", wantEffective: "sweeping",
		},
		{
			name:       "flat search reports the request but no effective strategy",
			configured: ACORN, flatCutoff: 1000, allowList: allowList,
			params:        strategy(searchparams.FilterStrategyPathseer),
			wantRequested: "pathseer", wantFlat: true,
		},
	}

	prevCutoff := atomic.LoadInt64(&index.flatSearchCutoff)
	t.Cleanup(func() { atomic.StoreInt64(&index.flatSearchCutoff, prevCutoff) })
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index.configuredFilterStrategy.Store(int32(tt.configured))
			atomic.StoreInt64(&index.flatSearchCutoff, tt.flatCutoff)

			sctx := helpers.InitSlowQueryDetails(context.Background())
			ids, _, err := index.SearchByVector(sctx, query, k, tt.allowList, tt.params)
			require.NoError(t, err)
			details := helpers.ExtractSlowQueryDetails(sctx)

			if tt.allowList == nil {
				assert.Equal(t, wantUnfiltered, ids)
			} else {
				assert.Equal(t, wantFiltered, ids)
			}
			assert.Equal(t, tt.wantFlat, details["hnsw_flat_search"])
			if tt.wantRequested == "" {
				assert.NotContains(t, details, "hnsw_filter_strategy_requested")
			} else {
				assert.Equal(t, tt.wantRequested, details["hnsw_filter_strategy_requested"])
			}
			if tt.wantEffective == "" {
				assert.NotContains(t, details, "hnsw_filter_strategy_effective")
			} else {
				assert.Equal(t, tt.wantEffective, details["hnsw_filter_strategy_effective"])
			}
			// the override is per request: the collection config is untouched
			assert.Equal(t, tt.configured, FilterStrategy(index.configuredFilterStrategy.Load()))
		})
	}
}

// Test_SearchByVectorDistance_FilterStrategyOverride checks that the distance
// search entry point honours the per-request filter strategy too, and
// annotates the slow-query log with the requested and effective strategy.
func Test_SearchByVectorDistance_FilterStrategyOverride(t *testing.T) {
	const (
		size = 300
		dims = 8
	)
	ctx := context.Background()
	vectors, queryVecs := testinghelpers.RandomVecsFixedSeed(size, 1, dims)
	query := queryVecs[0]

	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "search-params-filter-strategy-distance",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		AcornFilterRatio:      0.4,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    size,
		VectorCacheMaxObjects: 100000,
		FilterStrategy:        ent.FilterStrategySweeping,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })
	r := rand.New(rand.NewSource(42))
	index.randFunc = r.Float64
	for id, v := range vectors {
		require.NoError(t, index.Add(ctx, uint64(id), v))
	}
	require.Equal(t, SWEEPING, FilterStrategy(index.configuredFilterStrategy.Load()))

	allowList := helpers.NewAllowList(3, 17, 42, 77, 101, 150, 199, 230, 261, 299)

	prevCutoff := atomic.LoadInt64(&index.flatSearchCutoff)
	atomic.StoreInt64(&index.flatSearchCutoff, 1) // never flat: the graph path runs
	t.Cleanup(func() { atomic.StoreInt64(&index.flatSearchCutoff, prevCutoff) })

	tests := []struct {
		name          string
		params        *searchparams.VectorIndexParams
		wantRequested string // "" => key must be absent
		wantEffective string // "" => key must be absent
	}{
		{name: "nil params does not annotate"},
		{
			name:          "acorn override on distance search",
			params:        &searchparams.VectorIndexParams{FilterStrategy: searchparams.FilterStrategyAcorn},
			wantRequested: "acorn", wantEffective: "acorn",
		},
		{
			name:          "pathseer override on distance search",
			params:        &searchparams.VectorIndexParams{FilterStrategy: searchparams.FilterStrategyPathseer},
			wantRequested: "pathseer", wantEffective: "pathseer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sctx := helpers.InitSlowQueryDetails(context.Background())
			ids, _, err := index.SearchByVectorDistance(sctx, query, 1e9, -1, allowList, tt.params)
			require.NoError(t, err)
			details := helpers.ExtractSlowQueryDetails(sctx)

			// every allowed id is within the huge distance, none outside it
			assert.ElementsMatch(t, []uint64{3, 17, 42, 77, 101, 150, 199, 230, 261, 299}, ids)
			if tt.wantRequested == "" {
				assert.NotContains(t, details, "hnsw_filter_strategy_requested")
			} else {
				assert.Equal(t, tt.wantRequested, details["hnsw_filter_strategy_requested"])
			}
			if tt.wantEffective == "" {
				assert.NotContains(t, details, "hnsw_filter_strategy_effective")
			} else {
				assert.Equal(t, tt.wantEffective, details["hnsw_filter_strategy_effective"])
			}
			assert.Equal(t, SWEEPING, FilterStrategy(index.configuredFilterStrategy.Load()))
		})
	}
}
