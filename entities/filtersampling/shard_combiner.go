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

package filtersampling

import (
	"sort"
)

// ShardCombiner merges filter sampling results from multiple shards
type ShardCombiner struct{}

// NewShardCombiner creates a new ShardCombiner
func NewShardCombiner() *ShardCombiner {
	return &ShardCombiner{}
}

// Do merges results from multiple shards and returns a combined result
// limited to the specified sampleCount
func (sc *ShardCombiner) Do(results []*Result, sampleCount int) *Result {
	if len(results) == 0 {
		return &Result{}
	}

	// Map to aggregate cardinalities by value
	valueMap := make(map[any]uint64)
	var totalObjects uint64

	for _, result := range results {
		if result == nil {
			continue
		}
		totalObjects += result.TotalObjects
		for _, sample := range result.Samples {
			valueMap[sample.Value] += sample.Cardinality
		}
	}

	// Convert map to slice
	combined := make([]Sample, 0, len(valueMap))
	for value, cardinality := range valueMap {
		combined = append(combined, Sample{
			Value:       value,
			Cardinality: cardinality,
		})
	}

	// Sort by cardinality descending
	sort.Slice(combined, func(i, j int) bool {
		return combined[i].Cardinality > combined[j].Cardinality
	})

	// Limit to sampleCount
	if len(combined) > sampleCount {
		combined = combined[:sampleCount]
	}

	// Calculate estimated percent covered
	var totalCovered uint64
	for _, sample := range combined {
		totalCovered += sample.Cardinality
	}

	percentCovered := 0.0
	if totalObjects > 0 {
		percentCovered = (float64(totalCovered) / float64(totalObjects)) * 100
	}

	return &Result{
		Samples:                 combined,
		TotalObjects:            totalObjects,
		EstimatedPercentCovered: percentCovered,
	}
}
