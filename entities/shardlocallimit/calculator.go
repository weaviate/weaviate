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

package shardlocallimit

import "math"

// DefaultSafetyMargin is the default safety margin (20%) applied on top of the
// raw local limit to account for variance in data distribution across shards.
const DefaultSafetyMargin = 0.2

// CalculateLocalLimit computes per-shard local limit with safety margin.
// Returns (localLimitWithMargin, rawLocalLimit).
//
// The safetyMargin is a percentage (0.0-1.0) added on top of the raw local limit
// to account for variance in data distribution. A typical value is 0.2 (20%).
//
// If shardCount <= 1, returns (globalLimit, globalLimit) since no optimization applies.
// If globalLimit <= 0, returns (0, 0).
func CalculateLocalLimit(shardCount, globalLimit int, safetyMargin float64) (int, int) {
	if shardCount <= 1 || globalLimit <= 0 {
		return globalLimit, globalLimit
	}

	// Get raw local limit from LUT
	rawLocalLimit := LocalLimit(shardCount, globalLimit)
	if rawLocalLimit <= 0 {
		// Fallback: divide by shard count with ceiling
		rawLocalLimit = int(math.Ceil(float64(globalLimit) / float64(shardCount)))
	}

	// Apply safety margin with ceiling
	localLimitWithMargin := int(math.Ceil(float64(rawLocalLimit) * (1.0 + safetyMargin)))

	// Cap at global limit (no point fetching more than what's requested)
	if localLimitWithMargin > globalLimit {
		localLimitWithMargin = globalLimit
	}

	return localLimitWithMargin, rawLocalLimit
}

// EstimateTrafficSavings calculates percentage saved vs fetching globalLimit from each shard.
// Returns a value between 0.0 and 1.0 (0% to 100% savings).
func EstimateTrafficSavings(shardCount, globalLimit, localLimit int) float64 {
	if shardCount <= 0 || globalLimit <= 0 || localLimit <= 0 {
		return 0
	}

	// Without optimization: shardCount * globalLimit
	baseline := float64(shardCount * globalLimit)

	// With optimization: shardCount * localLimit
	optimized := float64(shardCount * localLimit)

	// Savings = (baseline - optimized) / baseline
	if baseline <= 0 {
		return 0
	}

	savings := (baseline - optimized) / baseline
	if savings < 0 {
		return 0
	}
	if savings > 1 {
		return 1
	}
	return savings
}
