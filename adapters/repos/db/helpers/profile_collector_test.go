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

package helpers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

func TestInitProfileCollector_Empty(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	profiles := ExtractProfiles(ctx)
	require.NotNil(t, profiles)
	assert.Empty(t, profiles)
}

func TestAddShardProfile(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", map[string]any{
		"filters_build_allow_list_took": 10 * time.Millisecond,
		"vector_search_took":           20 * time.Millisecond,
		"objects_took":                 5 * time.Millisecond,
		"filters_ids_matched":          42,
		"hnsw_flat_search":             true,
	})
	AddShardProfile(ctx, "shard-2", map[string]any{
		"sort_took":             3 * time.Millisecond,
		"knn_search_rescore_took": 7 * time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 2)

	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.Equal(t, int64(10_000_000), profiles[0].FilterNs)
	assert.Equal(t, int64(20_000_000), profiles[0].VectorSearchNs)
	assert.Equal(t, int64(5_000_000), profiles[0].ObjectRetrievalNs)
	assert.Equal(t, int32(42), profiles[0].FilterIdsMatched)
	assert.True(t, profiles[0].HnswFlatSearch)

	assert.Equal(t, "shard-2", profiles[1].Name)
	assert.Equal(t, int64(3_000_000), profiles[1].SortNs)
	assert.Equal(t, int64(7_000_000), profiles[1].RescoreNs)
}

func TestAddShardProfile_ConcurrentAccess(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	var wg sync.WaitGroup
	n := 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddShardProfile(ctx, "shard", map[string]any{
				"vector_search_took": time.Duration(idx) * time.Microsecond,
			})
		}(i)
	}
	wg.Wait()

	profiles := ExtractProfiles(ctx)
	assert.Len(t, profiles, n)
}

func TestAddShardProfile_NilContext(t *testing.T) {
	// no collector in context — should not panic
	ctx := context.Background()
	AddShardProfile(ctx, "shard-1", map[string]any{
		"vector_search_took": 10 * time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	assert.Nil(t, profiles)
}

func TestAddShardProfile_DurationConversion(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", map[string]any{
		"filters_build_allow_list_took": 1*time.Second + 500*time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.Equal(t, int64(1_500_000_000), profiles[0].FilterNs)
}

func TestAddShardProfile_EmptyDetails(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", map[string]any{})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.Zero(t, profiles[0].FilterNs)
	assert.Zero(t, profiles[0].VectorSearchNs)
}

func TestAddShardProfile_NilDetails(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", nil)

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.Equal(t, "shard-1", profiles[0].Name)
}

func TestAttachProfileToResults(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", map[string]any{
		"vector_search_took": 10 * time.Millisecond,
	})

	results := search.Results{
		{Schema: map[string]interface{}{"name": "test"}},
		{Schema: map[string]interface{}{"name": "test2"}},
	}

	results = AttachProfileToResults(ctx, results)

	require.NotNil(t, results[0].AdditionalProperties)
	profiles, ok := results[0].AdditionalProperties["profile"].([]ShardProfile)
	require.True(t, ok)
	require.Len(t, profiles, 1)
	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.Equal(t, int64(10_000_000), profiles[0].VectorSearchNs)

	// second result should not have profile data
	assert.Nil(t, results[1].AdditionalProperties)
}

func TestAttachProfileToResults_EmptyResults(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", map[string]any{})

	results := AttachProfileToResults(ctx, search.Results{})
	assert.Empty(t, results)
}

func TestAttachProfileToResults_NoProfiles(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	results := search.Results{
		{Schema: map[string]interface{}{"name": "test"}},
	}

	results = AttachProfileToResults(ctx, results)
	assert.Nil(t, results[0].AdditionalProperties)
}

func TestAttachProfileToResults_ExistingAdditionalProperties(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", map[string]any{
		"vector_search_took": 5 * time.Millisecond,
	})

	results := search.Results{
		{
			Schema:               map[string]interface{}{"name": "test"},
			AdditionalProperties: models.AdditionalProperties{"existing": "value"},
		},
	}

	results = AttachProfileToResults(ctx, results)
	assert.Equal(t, "value", results[0].AdditionalProperties["existing"])
	profiles, ok := results[0].AdditionalProperties["profile"].([]ShardProfile)
	require.True(t, ok)
	require.Len(t, profiles, 1)
}
