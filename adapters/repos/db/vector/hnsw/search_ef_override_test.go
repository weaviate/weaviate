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
