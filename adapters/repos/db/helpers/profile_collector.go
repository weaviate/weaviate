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

// ShardProfile holds the profiling details for a single shard's contribution to a search query.
type ShardProfile struct {
	Name    string            `json:"name"`
	Details map[string]string `json:"details,omitempty"`
}

// ProfileCollector aggregates per-shard profiling data across concurrent shard searches.
// It is stored in the context and is safe for concurrent use.
type ProfileCollector struct {
	mu       sync.Mutex
	profiles []ShardProfile
}

// InitProfileCollector stores a new [ProfileCollector] in the context.
// Call this before shard searches so that [AddShardProfile] can record timing data.
func InitProfileCollector(ctx context.Context) context.Context {
	return context.WithValue(ctx, profileCollectorKey, &ProfileCollector{})
}

// AddShardProfile records profiling data for a single shard search.
// It converts the raw slow-query details map into human-readable strings.
// Safe for concurrent use from multiple shard search goroutines.
func AddShardProfile(ctx context.Context, shardName string, totalTook time.Duration, details map[string]any) {
	val := ctx.Value(profileCollectorKey)
	if val == nil {
		return
	}

	collector, ok := val.(*ProfileCollector)
	if !ok {
		return
	}

	profile := ShardProfile{
		Name:    shardName,
		Details: make(map[string]string, len(details)+1),
	}

	profile.Details["total_took"] = totalTook.String()

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
			profile.Details[k] = val.String()
		case string:
			profile.Details[k] = val
		case bool:
			profile.Details[k] = strconv.FormatBool(val)
		case int:
			profile.Details[k] = strconv.Itoa(val)
		case int32:
			profile.Details[k] = strconv.FormatInt(int64(val), 10)
		case int64:
			profile.Details[k] = strconv.FormatInt(val, 10)
		case float64:
			profile.Details[k] = strconv.FormatFloat(val, 'f', -1, 64)
		default:
			if b, err := json.Marshal(v); err == nil {
				profile.Details[k] = string(b)
			} else {
				profile.Details[k] = fmt.Sprint(v)
			}
		}
	}

	collector.mu.Lock()
	collector.profiles = append(collector.profiles, profile)
	collector.mu.Unlock()
}

// AttachProfileToResults extracts collected profiles and attaches them to the first
// search result's AdditionalProperties. Profile data is per-query (not per-object),
// so it is only attached to results[0]. The data is stored in two formats:
// "profileRaw" ([]ShardProfile for gRPC) and "profile" (JSON string for GraphQL).
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

// ExtractProfiles returns a copy of all collected shard profiles from the context.
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

	result := make([]ShardProfile, len(collector.profiles))
	copy(result, collector.profiles)
	return result
}
