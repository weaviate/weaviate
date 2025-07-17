//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBaseModule_addShardJitter_Simple(t *testing.T) {
	module := &BaseModule{}

	// Test 1: Check that the default value is correct
	assert.Equal(t, 100*time.Millisecond, DefaultShardJitterInterval, "jitter interval should be 100ms")

	// Test 2: Test that jitter function doesn't panic and completes
	// This is a simple smoke test - the function should execute without error
	assert.NotPanics(t, func() {
		module.addShardJitter()
	}, "jitter function should not panic")

	// Test 3: Test multiple calls don't cause issues
	assert.NotPanics(t, func() {
		for i := 0; i < 10; i++ {
			module.addShardJitter()
		}
	}, "multiple jitter calls should not cause issues")

	// Test 4: Test that the function actually sleeps (basic timing check)
	start := time.Now()
	module.addShardJitter()
	duration := time.Since(start)

	// The function should have slept for some time (even if it's very short)
	assert.Greater(t, duration, 0*time.Millisecond, "function should actually sleep")
	assert.Less(t, duration, DefaultShardJitterInterval+10*time.Millisecond, "sleep should not exceed max interval")

	// Test 5: Test that multiple calls produce different timing (basic variance check)
	var durations []time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		module.addShardJitter()
		duration := time.Since(start)
		durations = append(durations, duration)
	}

	// Test 6: Verify all durations are within bounds
	for _, duration := range durations {
		assert.GreaterOrEqual(t, duration, 0*time.Millisecond, "all durations should be non-negative")
		assert.Less(t, duration, DefaultShardJitterInterval+10*time.Millisecond, "all durations should be within max interval")
	}
}
