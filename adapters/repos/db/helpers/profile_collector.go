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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

type contextKey string

const profileCollectorKey contextKey = "profile_collector"

// SearchProfile holds the profiling details for a single search type within a shard.
type SearchProfile struct {
	// Details contains human-readable profiling metrics keyed by metric name
	// (e.g., "total_took", "vector_search_took", "filters_build_allow_list_took").
	Details map[string]string `json:"details"`
}

// ShardProfile holds the profiling details for a single shard's contribution to a search query.
// For hybrid queries a shard may have multiple search types (e.g., "vector" and "keyword").
type ShardProfile struct {
	// Name is the identifier of the shard that was searched.
	Name string `json:"name"`
	// Searches maps search type (e.g., "vector", "keyword") to its profiling details.
	Searches map[string]SearchProfile `json:"searches"`
}

// shardEntry is an internal record for a single search operation within a shard.
type shardEntry struct {
	shardName  string
	searchType string
	details    map[string]string
}

// ProfileCollector aggregates per-shard profiling data across concurrent shard searches.
// It is stored in the context and is safe for concurrent use.
type ProfileCollector struct {
	mu      sync.Mutex
	entries []shardEntry
}

// InitProfileCollector stores a new [ProfileCollector] in the context.
// Call this before shard searches so that [AddShardProfile] can record timing data.
// It is idempotent: if a collector already exists in ctx, the context is returned as-is.
// This allows hybrid search to initialize a single shared collector that both
// sub-searches (vector and keyword) write to.
func InitProfileCollector(ctx context.Context) context.Context {
	if ctx.Value(profileCollectorKey) != nil {
		return ctx
	}
	return context.WithValue(ctx, profileCollectorKey, &ProfileCollector{})
}

// AddShardProfile records profiling data for a single shard search.
// searchType identifies the kind of search (e.g., "vector", "keyword").
// It converts the raw slow-query details map into human-readable strings.
// Safe for concurrent use from multiple shard search goroutines.
func AddShardProfile(ctx context.Context, shardName string, searchType string, totalTook time.Duration, details map[string]any) {
	val := ctx.Value(profileCollectorKey)
	if val == nil {
		return
	}

	collector, ok := val.(*ProfileCollector)
	if !ok {
		return
	}

	d := make(map[string]string, len(details)+1)
	d["total_took"] = totalTook.String()

	for k, v := range details {
		// Skip _string suffix keys — they duplicate the duration values
		// that are already formatted from their time.Duration counterparts.
		if strings.HasSuffix(k, "_string") {
			if base := strings.TrimSuffix(k, "_string"); details[base] != nil {
				continue
			}
		}
		// Skip internal metadata that doesn't add profiling value.
		if k == "is_coordinator" {
			continue
		}
		switch val := v.(type) {
		case time.Duration:
			d[k] = val.String()
		case string:
			d[k] = val
		case bool:
			d[k] = strconv.FormatBool(val)
		case int:
			d[k] = strconv.Itoa(val)
		case int32:
			d[k] = strconv.FormatInt(int64(val), 10)
		case int64:
			d[k] = strconv.FormatInt(val, 10)
		case float64:
			d[k] = strconv.FormatFloat(val, 'f', -1, 64)
		default:
			if b, err := json.Marshal(v); err == nil {
				d[k] = string(b)
			} else {
				d[k] = fmt.Sprint(v)
			}
		}
	}

	collector.mu.Lock()
	collector.entries = append(collector.entries, shardEntry{
		shardName:  shardName,
		searchType: searchType,
		details:    d,
	})
	collector.mu.Unlock()
}

// AttachProfileToResults calls [ExtractProfiles] and attaches the collected
// [ShardProfile] entries to the first search result's AdditionalProperties.
// Profile data is per-query (not per-object), so it is only attached to results[0].
// The data is stored in two formats: "profileRaw" ([][ShardProfile] for gRPC)
// and "profile" (JSON string for GraphQL).
func AttachProfileToResults(ctx context.Context, results search.Results) search.Results {
	profiles := ExtractProfiles(ctx)
	if len(profiles) == 0 || len(results) == 0 {
		return results
	}
	if results[0].AdditionalProperties == nil {
		results[0].AdditionalProperties = make(models.AdditionalProperties)
	}
	// Store raw profiles for gRPC consumption.
	results[0].AdditionalProperties["profileRaw"] = profiles
	// Store JSON string for GraphQL consumption.
	if b, err := json.Marshal(profiles); err == nil {
		results[0].AdditionalProperties["profile"] = string(b)
	}
	return results
}

// ExtractProfiles groups all collected entries by shard name and returns one
// [ShardProfile] per shard, each containing a map of search type to [SearchProfile].
// Safe for concurrent use. Returns nil if no collector is present or no entries were recorded.
func ExtractProfiles(ctx context.Context) []ShardProfile {
	val := ctx.Value(profileCollectorKey)
	if val == nil {
		return nil
	}

	collector, ok := val.(*ProfileCollector)
	if !ok {
		return nil
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	if len(collector.entries) == 0 {
		return nil
	}

	// Group entries by shard name, preserving insertion order.
	order := make([]string, 0)
	grouped := make(map[string]map[string]SearchProfile)
	for _, e := range collector.entries {
		if _, exists := grouped[e.shardName]; !exists {
			order = append(order, e.shardName)
			grouped[e.shardName] = make(map[string]SearchProfile)
		}
		grouped[e.shardName][e.searchType] = SearchProfile{Details: e.details}
	}

	result := make([]ShardProfile, len(order))
	for i, name := range order {
		result[i] = ShardProfile{
			Name:     name,
			Searches: grouped[name],
		}
	}
	return result
}
