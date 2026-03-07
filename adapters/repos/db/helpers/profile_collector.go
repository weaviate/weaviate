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
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

type contextKey string

const profileCollectorKey contextKey = "profile_collector"

type ShardProfile struct {
	Name              string
	FilterNs          int64
	VectorSearchNs    int64
	ObjectRetrievalNs int64
	SortNs            int64
	RescoreNs         int64
	FilterIdsMatched  int32
	HnswFlatSearch    bool
}

type ProfileCollector struct {
	mu       sync.Mutex
	profiles []ShardProfile
}

func InitProfileCollector(ctx context.Context) context.Context {
	return context.WithValue(ctx, profileCollectorKey, &ProfileCollector{})
}

func AddShardProfile(ctx context.Context, shardName string, details map[string]any) {
	val := ctx.Value(profileCollectorKey)
	if val == nil {
		return
	}

	collector, ok := val.(*ProfileCollector)
	if !ok {
		return
	}

	profile := ShardProfile{Name: shardName}

	if v, ok := details["filters_build_allow_list_took"].(time.Duration); ok {
		profile.FilterNs = v.Nanoseconds()
	}
	if v, ok := details["vector_search_took"].(time.Duration); ok {
		profile.VectorSearchNs = v.Nanoseconds()
	}
	if v, ok := details["objects_took"].(time.Duration); ok {
		profile.ObjectRetrievalNs = v.Nanoseconds()
	}
	if v, ok := details["sort_took"].(time.Duration); ok {
		profile.SortNs = v.Nanoseconds()
	}
	if v, ok := details["knn_search_rescore_took"].(time.Duration); ok {
		profile.RescoreNs = v.Nanoseconds()
	}
	if v, ok := details["filters_ids_matched"].(int); ok {
		profile.FilterIdsMatched = int32(v)
	}
	if v, ok := details["hnsw_flat_search"].(bool); ok {
		profile.HnswFlatSearch = v
	}

	collector.mu.Lock()
	collector.profiles = append(collector.profiles, profile)
	collector.mu.Unlock()
}

func AttachProfileToResults(ctx context.Context, results search.Results) search.Results {
	profiles := ExtractProfiles(ctx)
	if len(profiles) == 0 || len(results) == 0 {
		return results
	}
	if results[0].AdditionalProperties == nil {
		results[0].AdditionalProperties = make(models.AdditionalProperties)
	}
	results[0].AdditionalProperties["profile"] = profiles
	return results
}

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
