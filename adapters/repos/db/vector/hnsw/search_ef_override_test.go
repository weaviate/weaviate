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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/searchparams"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestEffectiveSearchEF(t *testing.T) {
	t.Parallel()

	index := newSearchEFIndex(t, ent.UserConfig{
		VectorCacheMaxObjects: 10,
		EF:                    -1,
		DynamicEFMin:          100,
		DynamicEFMax:          500,
		DynamicEFFactor:       8,
	})

	// No override: dynamic ef from the index config (k * factor, clamped).
	assert.Equal(t, 184, index.effectiveSearchEF(context.Background(), 23))

	// Search-time ef replaces dynamic ef.
	ctx := searchparams.WithHNSWSearch(context.Background(), hnswEF(64))
	assert.Equal(t, 64, index.effectiveSearchEF(ctx, 23))

	// Raised to k so the result list is not truncated.
	assert.Equal(t, 80, index.effectiveSearchEF(ctx, 80))

	fixed := newSearchEFIndex(t, ent.UserConfig{
		VectorCacheMaxObjects: 10,
		EF:                    78,
	})
	assert.Equal(t, 78, fixed.effectiveSearchEF(context.Background(), 5))
	assert.Equal(t, 16, fixed.effectiveSearchEF(searchparams.WithHNSWSearch(context.Background(), hnswEF(16)), 5))
}

func TestSearchByVectorUsesSearchTimeEF(t *testing.T) {
	t.Parallel()

	index := newSearchEFIndex(t, ent.UserConfig{
		VectorCacheMaxObjects: 10,
		EF:                    200,
	})

	ctx := helpers.InitSlowQueryDetails(context.Background())
	ctx = searchparams.WithHNSWSearch(ctx, hnswEF(32))

	_, _, err := index.SearchByVector(ctx, []float32{0.1, 0.2}, 4, nil)
	require.NoError(t, err)

	details := helpers.ExtractSlowQueryDetails(ctx)
	require.NotNil(t, details)
	assert.Equal(t, 32, details["hnsw_ef"])
}

func newSearchEFIndex(t *testing.T, cfg ent.UserConfig) *hnsw {
	t.Helper()
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    t.Name(),
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, errors.Errorf("not implemented")
		},
		GetViewThunk: func() common.BucketView { return &dynamicEFNoopBucketView{} },
	}, cfg, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, index.Drop(context.Background(), false))
	})
	return index
}

func hnswEF(ef int) *searchparams.HNSW {
	return &searchparams.HNSW{EF: &ef}
}

func TestSearchEFIsBoundedByGraphSize(t *testing.T) {
	t.Parallel()

	index := newSearchEFIndex(t, ent.UserConfig{
		VectorCacheMaxObjects: 100,
		MaxConnections:        8,
		EFConstruction:        16,
		EF:                    16,
	})
	ctx := context.Background()
	vecs := [][]float32{{1, 0}, {0.2, 0.9}, {0, 1}}
	for i, vec := range vecs {
		require.NoError(t, index.Add(ctx, uint64(i), vec))
	}

	huge := math.MaxInt32
	searchCtx := helpers.InitSlowQueryDetails(searchparams.WithHNSWSearch(ctx, &searchparams.HNSW{EF: &huge}))
	ids, _, err := index.SearchByVector(searchCtx, []float32{1, 0}, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	got, ok := helpers.ExtractSlowQueryDetails(searchCtx)["hnsw_ef"].(int)
	require.True(t, ok)
	nodes := index.searchableNodeCount()
	require.Equal(t, len(vecs), nodes)
	assert.Equal(t, nodes, got)
	assert.Less(t, got, math.MaxInt32)
}

func TestMultiVectorSearchRefillsUntilKDocuments(t *testing.T) {
	t.Parallel()

	// Four near-duplicate vectors on document 0, then one far vector on
	// document 1. A candidate list of width k=2 is filled by document 0
	// alone; the search has to widen ef to surface document 1.
	docs := [][][]float32{
		{{0, 0}, {0.05, 0}, {0, 0.05}, {0.05, 0.05}},
		{{10, 10}},
	}
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    t.Name(),
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, errors.Errorf("single vector lookup is not used")
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return docs[id], nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		GetViewThunk:      func() common.BucketView { return &dynamicEFNoopBucketView{} },
	}, ent.UserConfig{
		VectorCacheMaxObjects: 1000,
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    64,
		Multivector:           ent.MultivectorConfig{Enabled: true},
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, index.Drop(context.Background(), false))
	})

	ctx := context.Background()
	for id, vecs := range docs {
		require.NoError(t, index.AddMulti(ctx, uint64(id), vecs))
	}

	searchCtx := searchparams.WithHNSWSearch(ctx, hnswEF(1))
	ids, _, err := index.SearchByMultiVector(searchCtx, [][]float32{{0, 0}}, 2, nil)
	require.NoError(t, err)
	require.Len(t, ids, 2)
	assert.Equal(t, uint64(0), ids[0])
	assert.Equal(t, uint64(1), ids[1])
}
