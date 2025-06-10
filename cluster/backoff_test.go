//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
)

func TestBackoffConfig(t *testing.T) {
	tests := []struct {
		name            string
		electionTimeout time.Duration
		wantInitial     time.Duration
		wantMax         time.Duration
	}{
		{
			name:            "1 second election timeout",
			electionTimeout: time.Second,
			wantInitial:     time.Millisecond * 50, // 1/20th of 1s
			wantMax:         time.Second,           // same as election timeout
		},
		{
			name:            "2 second election timeout",
			electionTimeout: time.Second * 2,
			wantInitial:     time.Millisecond * 100, // 1/20th of 2s
			wantMax:         time.Second * 2,        // same as election timeout
		},
		{
			name:            "500ms election timeout",
			electionTimeout: time.Millisecond * 500,
			wantInitial:     time.Millisecond * 25,  // 1/20th of 500ms
			wantMax:         time.Millisecond * 500, // same as election timeout
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			bo := backoffConfig(ctx, tt.electionTimeout)

			// Test initial interval with larger delta to account for randomization
			firstInterval := bo.NextBackOff()
			// Allow up to 50% variation for initial interval
			assert.InDelta(t, float64(tt.wantInitial), float64(firstInterval), float64(tt.wantInitial)*0.5)

			// Collect all intervals
			var intervals []time.Duration
			for i := 0; i < 15; i++ { // Try more than max retries to ensure we get all intervals
				next := bo.NextBackOff()
				if next == backoff.Stop {
					break
				}
				intervals = append(intervals, next)
			}

			// Verify we got at least 9 intervals (the backoff library might stop at 9)
			assert.GreaterOrEqual(t, len(intervals), 9)
			assert.LessOrEqual(t, len(intervals), 10)

			// Verify max interval is within 50% of expected
			maxInterval := intervals[len(intervals)-1]
			assert.InDelta(t, float64(tt.wantMax), float64(maxInterval), float64(tt.wantMax)*0.5)

			// Verify overall pattern:
			// 1. All intervals should be between initial and max
			// 2. Later intervals should generally be larger than earlier ones
			minAllowed := float64(tt.wantInitial) * 0.5      // Allow 50% below initial
			maxAllowed := float64(tt.wantMax) * float64(1.5) // Allow 50% above max

			// Track the running average to detect general increase
			var sum float64
			var count int

			for i, interval := range intervals {
				duration := float64(interval)

				// Check bounds
				assert.GreaterOrEqual(t, duration, minAllowed, "interval %d too small", i)
				assert.LessOrEqual(t, duration, maxAllowed, "interval %d too large", i)

				// Update running average
				sum += duration
				count++
				avg := sum / float64(count)

				// After first few intervals, check that we're generally increasing
				if i > 2 {
					// Current interval should be at least 80% of the average so far
					// This allows for some variation while ensuring general increase
					assert.GreaterOrEqual(t, duration, avg*0.8,
						"interval %d (%v) too small compared to average so far (%v)",
						i, interval, time.Duration(avg))
				}
			}

			// Verify context is properly set
			assert.Equal(t, ctx, bo.Context())
		})
	}
}

func TestBackoffBehavior(t *testing.T) {
	ctx := context.Background()
	electionTimeout := time.Second
	bo := backoffConfig(ctx, electionTimeout)

	// Test that intervals increase exponentially but are capped at max interval
	intervals := make([]time.Duration, 0)
	for i := 0; i < 15; i++ { // Try more than max retries to verify capping
		next := bo.NextBackOff()
		if next == backoff.Stop {
			break
		}
		intervals = append(intervals, next)
	}

	// Should have between 9 and 10 retries
	assert.GreaterOrEqual(t, len(intervals), 9)
	assert.LessOrEqual(t, len(intervals), 10)

	// Verify overall pattern using the same approach as TestBackoffConfig
	minAllowed := float64(time.Millisecond * 25)      // 50% of initial (50ms)
	maxAllowed := float64(time.Second) * float64(1.5) // 50% above max (1s)

	var sum float64
	var count int

	for i, interval := range intervals {
		duration := float64(interval)

		// Check bounds
		assert.GreaterOrEqual(t, duration, minAllowed, "interval %d too small", i)
		assert.LessOrEqual(t, duration, maxAllowed, "interval %d too large", i)

		// Update running average
		sum += duration
		count++
		avg := sum / float64(count)

		// After first few intervals, check that we're generally increasing
		if i > 2 {
			assert.GreaterOrEqual(t, duration, avg*0.8,
				"interval %d (%v) too small compared to average so far (%v)",
				i, interval, time.Duration(avg))
		}
	}

	// Verify total time is within reasonable bounds
	totalTime := time.Duration(0)
	for _, interval := range intervals {
		totalTime += interval
	}
	expectedTotal := time.Millisecond * 5550 // 50 + 100 + 200 + 400 + 800 + 1000*5
	// Allow 50% variation for total time due to randomization
	assert.InDelta(t, float64(expectedTotal), float64(totalTime), float64(expectedTotal)*0.5)
}
