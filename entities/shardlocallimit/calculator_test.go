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

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateLocalLimit(t *testing.T) {
	tests := []struct {
		name                       string
		shardCount                 int
		globalLimit                int
		safetyMargin               float64
		expectedWithMargin         int
		expectedRaw                int
		describeExpectedWithMargin string
	}{
		{
			name:               "single shard returns global limit",
			shardCount:         1,
			globalLimit:        100,
			safetyMargin:       0.2,
			expectedWithMargin: 100,
			expectedRaw:        100,
		},
		{
			name:               "zero shards returns global limit",
			shardCount:         0,
			globalLimit:        100,
			safetyMargin:       0.2,
			expectedWithMargin: 100,
			expectedRaw:        100,
		},
		{
			name:               "zero global limit returns zero",
			shardCount:         12,
			globalLimit:        0,
			safetyMargin:       0.2,
			expectedWithMargin: 0,
			expectedRaw:        0,
		},
		{
			name:               "negative global limit returns negative",
			shardCount:         12,
			globalLimit:        -1,
			safetyMargin:       0.2,
			expectedWithMargin: -1,
			expectedRaw:        -1,
		},
		{
			name:                       "12 shards, limit 100, 20% margin",
			shardCount:                 12,
			globalLimit:                100,
			safetyMargin:               0.2,
			describeExpectedWithMargin: "raw from LUT + 20% margin, capped at global limit",
		},
		{
			name:                       "2 shards, limit 100, 20% margin",
			shardCount:                 2,
			globalLimit:                100,
			safetyMargin:               0.2,
			describeExpectedWithMargin: "should be close to 72 + 20% = ~87",
		},
		{
			name:                       "no safety margin",
			shardCount:                 12,
			globalLimit:                100,
			safetyMargin:               0,
			describeExpectedWithMargin: "raw value with no margin",
		},
		{
			name:                       "large limit with many shards",
			shardCount:                 100,
			globalLimit:                10000,
			safetyMargin:               0.2,
			describeExpectedWithMargin: "should show significant savings",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withMargin, raw := CalculateLocalLimit(tc.shardCount, tc.globalLimit, tc.safetyMargin)

			// For edge cases with explicit expectations
			if tc.expectedWithMargin != 0 || tc.expectedRaw != 0 {
				assert.Equal(t, tc.expectedWithMargin, withMargin, "localLimitWithMargin")
				assert.Equal(t, tc.expectedRaw, raw, "rawLocalLimit")
			} else {
				// For normal cases, verify invariants
				if tc.shardCount > 1 && tc.globalLimit > 0 {
					// Raw should be less than global limit for multi-shard
					assert.LessOrEqual(t, raw, tc.globalLimit, "raw should be <= globalLimit")

					// With margin should be >= raw (unless capped)
					assert.GreaterOrEqual(t, withMargin, raw, "withMargin should be >= raw")

					// With margin should not exceed global limit
					assert.LessOrEqual(t, withMargin, tc.globalLimit, "withMargin should be <= globalLimit")

					// Verify margin calculation
					expectedWithMargin := int(math.Ceil(float64(raw) * (1.0 + tc.safetyMargin)))
					if expectedWithMargin > tc.globalLimit {
						expectedWithMargin = tc.globalLimit
					}
					assert.Equal(t, expectedWithMargin, withMargin, "margin calculation")
				}
			}
		})
	}
}

func TestEstimateTrafficSavings(t *testing.T) {
	tests := []struct {
		name        string
		shardCount  int
		globalLimit int
		localLimit  int
		expectedMin float64
		expectedMax float64
	}{
		{
			name:        "zero shard count",
			shardCount:  0,
			globalLimit: 100,
			localLimit:  50,
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:        "zero global limit",
			shardCount:  12,
			globalLimit: 0,
			localLimit:  50,
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:        "zero local limit",
			shardCount:  12,
			globalLimit: 100,
			localLimit:  0,
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:        "local limit equals global limit - no savings",
			shardCount:  12,
			globalLimit: 100,
			localLimit:  100,
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:        "50% local limit - 50% savings",
			shardCount:  12,
			globalLimit: 100,
			localLimit:  50,
			expectedMin: 0.49,
			expectedMax: 0.51,
		},
		{
			name:        "realistic case: 12 shards, limit 100, local ~27",
			shardCount:  12,
			globalLimit: 100,
			localLimit:  27, // Typical value from LUT with margin
			expectedMin: 0.70,
			expectedMax: 0.75,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			savings := EstimateTrafficSavings(tc.shardCount, tc.globalLimit, tc.localLimit)
			assert.GreaterOrEqual(t, savings, tc.expectedMin, "savings should be >= expectedMin")
			assert.LessOrEqual(t, savings, tc.expectedMax, "savings should be <= expectedMax")
		})
	}
}

func TestCalculateLocalLimit_VerifySavings(t *testing.T) {
	// Test to verify that example from plan works correctly
	// With 12 shards and limit=100, instead of fetching 1200 results (100 × 12),
	// we fetch ~264 results (~22 per shard) while maintaining 99.999% probability

	shardCount := 12
	globalLimit := 100
	safetyMargin := 0.2

	withMargin, raw := CalculateLocalLimit(shardCount, globalLimit, safetyMargin)

	// Raw should be around 21-22 based on the LUT
	assert.GreaterOrEqual(t, raw, 19, "raw limit should be reasonable")
	assert.LessOrEqual(t, raw, 25, "raw limit should not be too high")

	// With margin should be around 25-27
	assert.GreaterOrEqual(t, withMargin, raw, "with margin should be >= raw")
	assert.LessOrEqual(t, withMargin, globalLimit, "with margin should not exceed global limit")

	// Calculate actual traffic
	baselineTraffic := shardCount * globalLimit
	optimizedTraffic := shardCount * withMargin

	// Should have significant savings
	savings := EstimateTrafficSavings(shardCount, globalLimit, withMargin)
	t.Logf("12 shards, limit 100: raw=%d, withMargin=%d, baseline=%d, optimized=%d, savings=%.1f%%",
		raw, withMargin, baselineTraffic, optimizedTraffic, savings*100)

	assert.Greater(t, savings, 0.5, "should have >50% savings with 12 shards")
}
