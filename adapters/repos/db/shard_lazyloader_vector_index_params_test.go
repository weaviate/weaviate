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

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestLazyLoadShard_ObjectVectorSearchForwardsVectorIndexParams pins that the
// lazy-load wrapper (every multi-tenant shard) hands the request's params to
// the loaded shard, per-target map included, instead of dropping them on the
// way through.
func TestLazyLoadShard_ObjectVectorSearchForwardsVectorIndexParams(t *testing.T) {
	ctx := context.Background()
	shardLike, _ := testShardWithSettings(t, ctx, &models.Class{Class: "test"}, enthnsw.UserConfig{}, false, false, func(idx *Index) {
		idx.vectorIndexUserConfig = nil
		idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			"a": enthnsw.NewDefaultUserConfig(),
		}
	})
	shard := underlyingShard(t, shardLike)

	real, release, ok := shard.AcquireVectorIndex("a")
	require.True(t, ok)
	release()
	t.Cleanup(func() { real.Shutdown(ctx) })

	m := NewMockVectorIndex(t)
	m.On("Type").Return(common.IndexType(common.IndexTypeHNSW)).Maybe()
	m.On("Multivector").Return(false).Maybe()
	m.On("SearchByVector", mock.Anything, mock.Anything, 10, mock.Anything, mock.Anything).
		Return([]uint64{}, []float32{}, nil).Once()
	m.On("Shutdown", mock.Anything).Return(nil).Maybe()
	require.True(t, shard.vectors.Replace("a", m))

	lazy := &LazyLoadShard{}
	lazy.shard.Store(shard)

	params := &searchparams.VectorIndexParams{
		EF:        64,
		PerTarget: map[string]searchparams.VectorIndexParams{"a": {EF: 128, FilterStrategy: searchparams.FilterStrategyAcorn}},
	}
	_, _, err := lazy.ObjectVectorSearch(ctx, []models.Vector{[]float32{1, 2}}, []string{"a"}, 0, 10,
		nil, nil, nil, additional.Properties{}, nil, nil, params)
	require.NoError(t, err)

	var got *searchparams.VectorIndexParams
	for _, c := range m.Calls {
		if c.Method == "SearchByVector" {
			got, _ = c.Arguments.Get(4).(*searchparams.VectorIndexParams)
		}
	}
	// the shard resolves the per-target entry before calling the index
	assert.Equal(t, &searchparams.VectorIndexParams{EF: 128, FilterStrategy: searchparams.FilterStrategyAcorn}, got)
}
