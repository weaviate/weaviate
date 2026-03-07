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

func TestInitProfileCollector_Empty(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	profiles := ExtractProfiles(ctx)
	assert.Nil(t, profiles)
}

func TestInitProfileCollector_Idempotent(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, nil)

	ctx2 := InitProfileCollector(ctx)
	AddShardProfile(ctx2, "shard-2", "node-1", "vector", 2*time.Millisecond, nil)

	// Both profiles should be in the same collector since InitProfileCollector is idempotent.
	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 2)
	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.Equal(t, "node-1", profiles[0].Node)
	assert.Equal(t, "shard-2", profiles[1].Name)
	assert.Equal(t, "node-1", profiles[1].Node)
}

func TestAddShardProfile(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", "node-1", "vector", 35*time.Millisecond, map[string]any{
		"filters_build_allow_list_took": 10 * time.Millisecond,
		"vector_search_took":            20 * time.Millisecond,
		"objects_took":                  5 * time.Millisecond,
		"filters_ids_matched":           42,
		"hnsw_flat_search":              true,
	})
	AddShardProfile(ctx, "shard-2", "node-1", "keyword", 10*time.Millisecond, map[string]any{
		"sort_took":               3 * time.Millisecond,
		"knn_search_rescore_took": 7 * time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 2)

	assert.Equal(t, "shard-1", profiles[0].Name)
	d1 := profiles[0].Searches["vector"].Details
	assert.Equal(t, "35ms", d1["total_took"])
	assert.Equal(t, "10ms", d1["filters_build_allow_list_took"])
	assert.Equal(t, "20ms", d1["vector_search_took"])
	assert.Equal(t, "5ms", d1["objects_took"])
	assert.Equal(t, "42", d1["filters_ids_matched"])
	assert.Equal(t, "true", d1["hnsw_flat_search"])

	assert.Equal(t, "shard-2", profiles[1].Name)
	d2 := profiles[1].Searches["keyword"].Details
	assert.Equal(t, "10ms", d2["total_took"])
	assert.Equal(t, "3ms", d2["sort_took"])
	assert.Equal(t, "7ms", d2["knn_search_rescore_took"])
}

func TestAddShardProfile_HybridGroupsByShard(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	// Simulate hybrid: same shard gets both vector and keyword entries.
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 5*time.Millisecond, map[string]any{
		"vector_search_took": 3 * time.Millisecond,
	})
	AddShardProfile(ctx, "shard-1", "node-1", "keyword", 8*time.Millisecond, map[string]any{
		"kwd_time": 6 * time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1, "same shard should be grouped into one profile")
	assert.Equal(t, "shard-1", profiles[0].Name)
	require.Len(t, profiles[0].Searches, 2)

	assert.Equal(t, "3ms", profiles[0].Searches["vector"].Details["vector_search_took"])
	assert.Equal(t, "6ms", profiles[0].Searches["keyword"].Details["kwd_time"])
}

func TestAddShardProfile_ConcurrentAccess(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	var wg sync.WaitGroup
	n := 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddShardProfile(ctx, fmt.Sprintf("shard-%d", idx), "node-1", "vector", time.Duration(idx)*time.Microsecond, map[string]any{
				"vector_search_took": time.Duration(idx) * time.Microsecond,
			})
		}(i)
	}
	wg.Wait()

	profiles := ExtractProfiles(ctx)
	assert.Len(t, profiles, n)
}

func TestAddShardProfile_NilContext(t *testing.T) {
	ctx := context.Background()
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 10*time.Millisecond, map[string]any{
		"vector_search_took": 10 * time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	assert.Nil(t, profiles)
}

func TestAddShardProfile_DurationConversion(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", "node-1", "vector", 2*time.Second, map[string]any{
		"filters_build_allow_list_took": 1*time.Second + 500*time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	d := profiles[0].Searches["vector"].Details
	assert.Equal(t, "1.5s", d["filters_build_allow_list_took"])
	assert.Equal(t, "2s", d["total_took"])
}

func TestAddShardProfile_EmptyDetails(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.Equal(t, "1ms", profiles[0].Searches["vector"].Details["total_took"])
}

func TestAddShardProfile_NilDetails(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, nil)

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.Equal(t, "1ms", profiles[0].Searches["vector"].Details["total_took"])
}

func TestAddShardProfile_SkipStringDuplicates(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"vector_search_took":        10 * time.Millisecond,
		"vector_search_took_string": "10ms",
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	d := profiles[0].Searches["vector"].Details
	assert.Equal(t, "10ms", d["vector_search_took"])
	_, hasString := d["vector_search_took_string"]
	assert.False(t, hasString)
}

func TestAddShardProfile_TypeConversions(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"str_val":     "hello",
		"int32_val":   int32(42),
		"int64_val":   int64(999),
		"float64_val": 3.14,
		"slice_val":   []string{"a", "b"},
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	d := profiles[0].Searches["vector"].Details

	assert.Equal(t, "hello", d["str_val"])
	assert.Equal(t, "42", d["int32_val"])
	assert.Equal(t, "999", d["int64_val"])
	assert.Equal(t, "3.14", d["float64_val"])
	assert.Equal(t, `["a","b"]`, d["slice_val"])
}

func TestAddShardProfile_SkipIsCoordinator(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"is_coordinator":     true,
		"vector_search_took": 1 * time.Millisecond,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	d := profiles[0].Searches["vector"].Details
	_, hasCoordinator := d["is_coordinator"]
	assert.False(t, hasCoordinator)
	assert.Equal(t, "1ms", d["vector_search_took"])
}

func TestAddShardProfile_StringSuffixWithoutBase(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"orphan_string": "some value",
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.Equal(t, "some value", profiles[0].Searches["vector"].Details["orphan_string"])
}

func TestAddShardProfile_UnmarshalableFallback(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	ch := make(chan int)
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{
		"channel_val": ch,
	})

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 1)
	assert.NotEmpty(t, profiles[0].Searches["vector"].Details["channel_val"])
}

func TestExtractProfiles_ReturnsCopy(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, nil)

	profiles1 := ExtractProfiles(ctx)
	require.Len(t, profiles1, 1)

	profiles1[0].Name = "mutated"

	profiles2 := ExtractProfiles(ctx)
	assert.Equal(t, "shard-1", profiles2[0].Name)
}

func TestAttachProfileToResults(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 10*time.Millisecond, map[string]any{
		"vector_search_took": 10 * time.Millisecond,
	})

	results := search.Results{
		{Schema: map[string]interface{}{"name": "test"}},
		{Schema: map[string]interface{}{"name": "test2"}},
	}

	results = AttachProfileToResults(ctx, results)

	require.NotNil(t, results[0].AdditionalProperties)
	// GraphQL: JSON string
	profileStr, ok := results[0].AdditionalProperties["profile"].(string)
	require.True(t, ok)
	assert.Contains(t, profileStr, "shard-1")
	assert.Contains(t, profileStr, "vector")

	// gRPC: raw profiles
	profiles, ok := results[0].AdditionalProperties["profileRaw"].([]ShardProfile)
	require.True(t, ok)
	require.Len(t, profiles, 1)
	assert.Equal(t, "shard-1", profiles[0].Name)
	assert.NotNil(t, profiles[0].Searches["vector"])

	// second result should not have profile data
	assert.Nil(t, results[1].AdditionalProperties)
}

func TestAttachProfileToResults_EmptyResults(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 1*time.Millisecond, map[string]any{})

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
	AddShardProfile(ctx, "shard-1", "node-1", "vector", 5*time.Millisecond, map[string]any{
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
	profiles, ok := results[0].AdditionalProperties["profileRaw"].([]ShardProfile)
	require.True(t, ok)
	require.Len(t, profiles, 1)
}

func TestAddRemoteProfiles(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	// Add a local shard profile.
	AddShardProfile(ctx, "local-shard", "node-1", "vector", 5*time.Millisecond, map[string]any{
		"vector_search_took": 3 * time.Millisecond,
	})

	// Add remote profiles (as if received from another node).
	remoteProfiles := []ShardProfile{
		{
			Name: "remote-shard-1",
			Node: "node-2",
			Searches: map[string]SearchProfile{
				"vector": {Details: map[string]string{
					"total_took":         "10ms",
					"vector_search_took": "8ms",
				}},
			},
		},
		{
			Name: "remote-shard-2",
			Node: "node-3",
			Searches: map[string]SearchProfile{
				"keyword": {Details: map[string]string{
					"total_took": "7ms",
					"sort_took":  "2ms",
				}},
			},
		},
	}
	AddRemoteProfiles(ctx, remoteProfiles)

	profiles := ExtractProfiles(ctx)
	require.Len(t, profiles, 3)

	assert.Equal(t, "local-shard", profiles[0].Name)
	assert.Equal(t, "node-1", profiles[0].Node)
	assert.Equal(t, "3ms", profiles[0].Searches["vector"].Details["vector_search_took"])

	assert.Equal(t, "remote-shard-1", profiles[1].Name)
	assert.Equal(t, "node-2", profiles[1].Node)
	assert.Equal(t, "8ms", profiles[1].Searches["vector"].Details["vector_search_took"])

	assert.Equal(t, "remote-shard-2", profiles[2].Name)
	assert.Equal(t, "node-3", profiles[2].Node)
	assert.Equal(t, "2ms", profiles[2].Searches["keyword"].Details["sort_took"])
}

func TestAddRemoteProfiles_NilContext(t *testing.T) {
	ctx := context.Background()
	// Should not panic when no collector is present.
	AddRemoteProfiles(ctx, []ShardProfile{{Name: "shard-1"}})
	profiles := ExtractProfiles(ctx)
	assert.Nil(t, profiles)
}

func TestAddRemoteProfiles_Empty(t *testing.T) {
	ctx := InitProfileCollector(context.Background())
	AddRemoteProfiles(ctx, nil)
	AddRemoteProfiles(ctx, []ShardProfile{})
	profiles := ExtractProfiles(ctx)
	assert.Nil(t, profiles)
}

func TestConcurrentLocalAndRemoteProfiles(t *testing.T) {
	ctx := InitProfileCollector(context.Background())

	var wg sync.WaitGroup
	localCount := 25
	remoteCount := 25

	// Concurrent local shard profiles.
	for i := 0; i < localCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddShardProfile(ctx, fmt.Sprintf("local-%d", idx), "node-1", "vector", time.Duration(idx)*time.Microsecond, map[string]any{
				"vector_search_took": time.Duration(idx) * time.Microsecond,
			})
		}(i)
	}

	// Concurrent remote profile merges.
	for i := 0; i < remoteCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			AddRemoteProfiles(ctx, []ShardProfile{
				{
					Name: fmt.Sprintf("remote-%d", idx),
					Searches: map[string]SearchProfile{
						"keyword": {Details: map[string]string{
							"total_took": fmt.Sprintf("%dµs", idx),
						}},
					},
				},
			})
		}(i)
	}

	wg.Wait()

	profiles := ExtractProfiles(ctx)
	assert.Len(t, profiles, localCount+remoteCount)

	// Verify all shards are present.
	names := make(map[string]bool, len(profiles))
	for _, p := range profiles {
		names[p.Name] = true
	}
	for i := 0; i < localCount; i++ {
		assert.True(t, names[fmt.Sprintf("local-%d", i)], "missing local-%d", i)
	}
	for i := 0; i < remoteCount; i++ {
		assert.True(t, names[fmt.Sprintf("remote-%d", i)], "missing remote-%d", i)
	}
}
