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

import "sort"

//go:generate go run ./cmd/lutgen -out local_limit_lut_gen.go

// ShardBuckets are the supported shard “bucket” configurations.
// Mapping rule at runtime: choose the largest bucket <= shards (floor).
var ShardBuckets = []int{
	1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 15, 20, 25, 30, 40, 50, 75, 100,
}

// LimitBuckets are the supported global limit buckets.
// Mapping rule at runtime: choose the smallest bucket >= limit (ceiling).
var LimitBuckets = []int{
	10, 25, 50, 75, 100, 200, 400, 800, 1000, 2500, 5000, 7500, 10000,
}

// LocalLimit returns the per-shard local limit (k) from the precomputed LUT.
//
// The LUT is generated for a fixed target success probability (99.999%),
// and provides the minimum k satisfying the generator’s probability criterion
// for the bucketed (shards, limit).
//
// Runtime behavior:
// - shards bucketed by floor (largest bucket <= shards)
// - limit bucketed by ceil (smallest bucket >= limit)
//
// If limit exceeds the largest bucket, it is clamped to the largest bucket.
// If shards exceeds the largest bucket, it is clamped to the largest bucket.
// If shards is <= 0 or limit <= 0, returns 0.
func LocalLimit(shards, limit int) int {
	if shards <= 0 || limit <= 0 {
		return 0
	}

	si := shardBucketIndex(shards)
	li := limitBucketIndex(limit)

	// localLimitLUT is generated in local_limit_lut_gen.go
	return int(localLimitLUT[si][li])
}

func shardBucketIndex(shards int) int {
	// floor: largest bucket <= shards
	// Find first bucket > shards, then step back.
	i := sort.Search(len(ShardBuckets), func(i int) bool {
		return ShardBuckets[i] > shards
	})
	if i == 0 {
		return 0
	}
	if i >= len(ShardBuckets) {
		return len(ShardBuckets) - 1
	}
	return i - 1
}

func limitBucketIndex(limit int) int {
	// ceil: smallest bucket >= limit
	i := sort.SearchInts(LimitBuckets, limit)
	if i >= len(LimitBuckets) {
		return len(LimitBuckets) - 1
	}
	return i
}
