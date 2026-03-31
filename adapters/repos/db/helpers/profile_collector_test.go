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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

func TestInitQueryProfileCollector_Empty(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	queryProfiles := ExtractQueryProfiles(ctx)
	assert.Nil(t, queryProfiles)
}

func TestInitQueryProfileCollector_Idempotent(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, nil)

	ctx2 := InitQueryProfileCollector(ctx)
	AddShardQueryProfile(ctx2, "shard-2", "node-1", "vector", 2*time.Millisecond, nil)

	// Both queryProfiles should be in the same collector since InitQueryProfileCollector is idempotent.
	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 2)
	assert.Equal(t, "shard-1", queryProfiles[0].Name)
	assert.Equal(t, "node-1", queryProfiles[0].Node)
	assert.Equal(t, "shard-2", queryProfiles[1].Name)
	assert.Equal(t, "node-1", queryProfiles[1].Node)
}

func TestAddShardQueryProfile(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 35*time.Millisecond, map[string]any{
		"filters_build_allow_list_took": 10 * time.Millisecond,
		"vector_search_took":            20 * time.Millisecond,
		"objects_took":                  5 * time.Millisecond,
		"filters_ids_matched":           42,
		"hnsw_flat_search":              true,
	})
	AddShardQueryProfile(ctx, "shard-2", "node-1", "keyword", 10*time.Millisecond, map[string]any{
		"sort_took":               3 * time.Millisecond,
		"knn_search_rescore_took": 7 * time.Millisecond,
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 2)

	assert.Equal(t, "shard-1", queryProfiles[0].Name)
	d1 := queryProfiles[0].Searches["vector"].Details
	assert.Equal(t, "35ms", d1["total_took"])
	assert.Equal(t, "10ms", d1["filters_build_allow_list_took"])
	assert.Equal(t, "20ms", d1["vector_search_took"])
	assert.Equal(t, "5ms", d1["objects_took"])
	assert.Equal(t, "42", d1["filters_ids_matched"])
	assert.Equal(t, "true", d1["hnsw_flat_search"])

	assert.Equal(t, "shard-2", queryProfiles[1].Name)
	d2 := queryProfiles[1].Searches["keyword"].Details
	assert.Equal(t, "10ms", d2["total_took"])
	assert.Equal(t, "3ms", d2["sort_took"])
	assert.Equal(t, "7ms", d2["knn_search_rescore_took"])
}

func TestAddShardQueryProfile_HybridGroupsByShard(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	// Simulate hybrid: same shard gets both vector and keyword entries.
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 5*time.Millisecond, map[string]any{
		"vector_search_took": 3 * time.Millisecond,
	})
	AddShardQueryProfile(ctx, "shard-1", "node-1", "keyword", 8*time.Millisecond, map[string]any{
		"kwd_time": 6 * time.Millisecond,
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1, "same shard should be grouped into one profile")
	assert.Equal(t, "shard-1", queryProfiles[0].Name)
	require.Len(t, queryProfiles[0].Searches, 2)

	assert.Equal(t, "3ms", queryProfiles[0].Searches["vector"].Details["vector_search_took"])
	assert.Equal(t, "6ms", queryProfiles[0].Searches["keyword"].Details["kwd_time"])
}

func TestAddShardQueryProfile_ConcurrentAccess(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	var wg sync.WaitGroup
	n := 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddShardQueryProfile(ctx, fmt.Sprintf("shard-%d", idx), "node-1", "vector", time.Duration(idx)*time.Microsecond, map[string]any{
				"vector_search_took": time.Duration(idx) * time.Microsecond,
			})
		}(i)
	}
	wg.Wait()

	queryProfiles := ExtractQueryProfiles(ctx)
	assert.Len(t, queryProfiles, n)
}

func TestAddShardQueryProfile_NilContext(t *testing.T) {
	ctx := context.Background()
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 10*time.Millisecond, map[string]any{
		"vector_search_took": 10 * time.Millisecond,
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	assert.Nil(t, queryProfiles)
}

func TestAddShardQueryProfile_DurationConversion(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 2*time.Second, map[string]any{
		"filters_build_allow_list_took": 1*time.Second + 500*time.Millisecond,
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	d := queryProfiles[0].Searches["vector"].Details
	assert.Equal(t, "1.5s", d["filters_build_allow_list_took"])
	assert.Equal(t, "2s", d["total_took"])
}

func TestAddShardQueryProfile_EmptyDetails(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	assert.Equal(t, "shard-1", queryProfiles[0].Name)
	assert.Equal(t, "1ms", queryProfiles[0].Searches["vector"].Details["total_took"])
}

func TestAddShardQueryProfile_NilDetails(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, nil)

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	assert.Equal(t, "shard-1", queryProfiles[0].Name)
	assert.Equal(t, "1ms", queryProfiles[0].Searches["vector"].Details["total_took"])
}

func TestAddShardQueryProfile_SkipStringDuplicates(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"vector_search_took":        10 * time.Millisecond,
		"vector_search_took_string": "10ms",
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	d := queryProfiles[0].Searches["vector"].Details
	assert.Equal(t, "10ms", d["vector_search_took"])
	_, hasString := d["vector_search_took_string"]
	assert.False(t, hasString)
}

func TestAddShardQueryProfile_TypeConversions(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"str_val":     "hello",
		"int32_val":   int32(42),
		"int64_val":   int64(999),
		"float64_val": 3.14,
		"slice_val":   []string{"a", "b"},
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	d := queryProfiles[0].Searches["vector"].Details

	assert.Equal(t, "hello", d["str_val"])
	assert.Equal(t, "42", d["int32_val"])
	assert.Equal(t, "999", d["int64_val"])
	assert.Equal(t, "3.14", d["float64_val"])
	assert.Equal(t, `["a","b"]`, d["slice_val"])
}

func TestAddShardQueryProfile_SkipIsCoordinator(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"is_coordinator":     true,
		"vector_search_took": 1 * time.Millisecond,
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	d := queryProfiles[0].Searches["vector"].Details
	_, hasCoordinator := d["is_coordinator"]
	assert.False(t, hasCoordinator)
	assert.Equal(t, "1ms", d["vector_search_took"])
}

func TestAddShardQueryProfile_StringSuffixWithoutBase(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"orphan_string": "some value",
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	assert.Equal(t, "some value", queryProfiles[0].Searches["vector"].Details["orphan_string"])
}

func TestAddShardQueryProfile_UnmarshalableFallback(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	ch := make(chan int)
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"channel_val": ch,
	})

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	assert.NotEmpty(t, queryProfiles[0].Searches["vector"].Details["channel_val"])
}

func TestExtractQueryProfiles_ReturnsCopy(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, nil)

	queryProfiles1 := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles1, 1)

	queryProfiles1[0].Name = "mutated"

	queryProfiles2 := ExtractQueryProfiles(ctx)
	assert.Equal(t, "shard-1", queryProfiles2[0].Name)
}

func TestExtractQueryProfiles_DetailsMapIsCopied(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"vector_search_took": 5 * time.Millisecond,
	})

	queryProfiles1 := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles1, 1)

	// Mutate the returned details map.
	queryProfiles1[0].Searches["vector"].Details["vector_search_took"] = "mutated"

	// A second extraction must return the original value.
	queryProfiles2 := ExtractQueryProfiles(ctx)
	assert.Equal(t, "5ms", queryProfiles2[0].Searches["vector"].Details["vector_search_took"])
}

func TestAttachQueryProfileToResults(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 10*time.Millisecond, map[string]any{
		"vector_search_took": 10 * time.Millisecond,
	})

	results := search.Results{
		{Schema: map[string]interface{}{"name": "test"}},
		{Schema: map[string]interface{}{"name": "test2"}},
	}

	results = AttachQueryProfileToResults(ctx, results)

	require.NotNil(t, results[0].AdditionalProperties)
	// GraphQL: JSON string
	profileStr, ok := results[0].AdditionalProperties["queryProfile"].(string)
	require.True(t, ok)
	assert.Contains(t, profileStr, "shard-1")
	assert.Contains(t, profileStr, "vector")

	// gRPC: raw queryProfiles
	queryProfiles, ok := results[0].AdditionalProperties["queryProfileRaw"].([]ShardQueryProfile)
	require.True(t, ok)
	require.Len(t, queryProfiles, 1)
	assert.Equal(t, "shard-1", queryProfiles[0].Name)
	assert.NotNil(t, queryProfiles[0].Searches["vector"])

	// second result should not have profile data
	assert.Nil(t, results[1].AdditionalProperties)
}

func TestAttachQueryProfileToResults_EmptyResults(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{})

	results := AttachQueryProfileToResults(ctx, search.Results{})
	assert.Empty(t, results)
}

func TestAttachQueryProfileToResults_NoProfiles(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	results := search.Results{
		{Schema: map[string]interface{}{"name": "test"}},
	}

	results = AttachQueryProfileToResults(ctx, results)
	assert.Nil(t, results[0].AdditionalProperties)
}

func TestAttachQueryProfileToResults_ExistingAdditionalProperties(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddShardQueryProfile(ctx, "shard-1", "node-1", "vector", 5*time.Millisecond, map[string]any{
		"vector_search_took": 5 * time.Millisecond,
	})

	results := search.Results{
		{
			Schema:               map[string]interface{}{"name": "test"},
			AdditionalProperties: models.AdditionalProperties{"existing": "value"},
		},
	}

	results = AttachQueryProfileToResults(ctx, results)
	assert.Equal(t, "value", results[0].AdditionalProperties["existing"])
	queryProfiles, ok := results[0].AdditionalProperties["queryProfileRaw"].([]ShardQueryProfile)
	require.True(t, ok)
	require.Len(t, queryProfiles, 1)
}

func TestAddRemoteQueryProfiles(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	// Add a local shard profile.
	AddShardQueryProfile(ctx, "local-shard", "node-1", "vector", 5*time.Millisecond, map[string]any{
		"vector_search_took": 3 * time.Millisecond,
	})

	// Add remote queryProfiles (as if received from another node).
	remoteProfiles := []ShardQueryProfile{
		{
			Name: "remote-shard-1",
			Node: "node-2",
			Searches: map[string]SearchQueryProfile{
				"vector": {Details: map[string]string{
					"total_took":         "10ms",
					"vector_search_took": "8ms",
				}},
			},
		},
		{
			Name: "remote-shard-2",
			Node: "node-3",
			Searches: map[string]SearchQueryProfile{
				"keyword": {Details: map[string]string{
					"total_took": "7ms",
					"sort_took":  "2ms",
				}},
			},
		},
	}
	AddRemoteQueryProfiles(ctx, remoteProfiles)

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 3)

	assert.Equal(t, "local-shard", queryProfiles[0].Name)
	assert.Equal(t, "node-1", queryProfiles[0].Node)
	assert.Equal(t, "3ms", queryProfiles[0].Searches["vector"].Details["vector_search_took"])

	assert.Equal(t, "remote-shard-1", queryProfiles[1].Name)
	assert.Equal(t, "node-2", queryProfiles[1].Node)
	assert.Equal(t, "8ms", queryProfiles[1].Searches["vector"].Details["vector_search_took"])

	assert.Equal(t, "remote-shard-2", queryProfiles[2].Name)
	assert.Equal(t, "node-3", queryProfiles[2].Node)
	assert.Equal(t, "2ms", queryProfiles[2].Searches["keyword"].Details["sort_took"])
}

func TestAddRemoteQueryProfiles_HybridMultipleSearchTypes(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	// Simulate a remote node returning hybrid profile data with both search types per shard.
	remoteProfiles := []ShardQueryProfile{
		{
			Name: "remote-shard-1",
			Node: "node-2",
			Searches: map[string]SearchQueryProfile{
				"vector":  {Details: map[string]string{"total_took": "10ms", "vector_search_took": "8ms"}},
				"keyword": {Details: map[string]string{"total_took": "6ms"}},
			},
		},
	}
	AddRemoteQueryProfiles(ctx, remoteProfiles)

	queryProfiles := ExtractQueryProfiles(ctx)
	require.Len(t, queryProfiles, 1)
	assert.Equal(t, "remote-shard-1", queryProfiles[0].Name)
	assert.Equal(t, "node-2", queryProfiles[0].Node)

	vecSearch, hasVec := queryProfiles[0].Searches["vector"]
	require.True(t, hasVec, "should have vector search profile")
	assert.Equal(t, "8ms", vecSearch.Details["vector_search_took"])

	kwdSearch, hasKwd := queryProfiles[0].Searches["keyword"]
	require.True(t, hasKwd, "should have keyword search profile")
	assert.Equal(t, "6ms", kwdSearch.Details["total_took"])
}

func TestAddRemoteQueryProfiles_NilContext(t *testing.T) {
	ctx := context.Background()
	// Should not panic when no collector is present.
	AddRemoteQueryProfiles(ctx, []ShardQueryProfile{{Name: "shard-1"}})
	queryProfiles := ExtractQueryProfiles(ctx)
	assert.Nil(t, queryProfiles)
}

func TestAddRemoteQueryProfiles_Empty(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())
	AddRemoteQueryProfiles(ctx, nil)
	AddRemoteQueryProfiles(ctx, []ShardQueryProfile{})
	queryProfiles := ExtractQueryProfiles(ctx)
	assert.Nil(t, queryProfiles)
}

func TestConcurrentLocalAndRemoteProfiles(t *testing.T) {
	ctx := InitQueryProfileCollector(context.Background())

	var wg sync.WaitGroup
	localCount := 25
	remoteCount := 25

	// Concurrent local shard queryProfiles.
	for i := 0; i < localCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddShardQueryProfile(ctx, fmt.Sprintf("local-%d", idx), "node-1", "vector", time.Duration(idx)*time.Microsecond, map[string]any{
				"vector_search_took": time.Duration(idx) * time.Microsecond,
			})
		}(i)
	}

	// Concurrent remote profile merges.
	for i := 0; i < remoteCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddRemoteQueryProfiles(ctx, []ShardQueryProfile{
				{
					Name: fmt.Sprintf("remote-%d", idx),
					Searches: map[string]SearchQueryProfile{
						"keyword": {Details: map[string]string{
							"total_took": fmt.Sprintf("%dµs", idx),
						}},
					},
				},
			})
		}(i)
	}

	wg.Wait()

	queryProfiles := ExtractQueryProfiles(ctx)
	assert.Len(t, queryProfiles, localCount+remoteCount)

	// Verify all shards are present.
	names := make(map[string]bool, len(queryProfiles))
	for _, p := range queryProfiles {
		names[p.Name] = true
	}
	for i := 0; i < localCount; i++ {
		assert.True(t, names[fmt.Sprintf("local-%d", i)], "missing local-%d", i)
	}
	for i := 0; i < remoteCount; i++ {
		assert.True(t, names[fmt.Sprintf("remote-%d", i)], "missing remote-%d", i)
	}
}
