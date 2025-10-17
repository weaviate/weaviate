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

package hnsw

import (
	"math/rand"
	"testing"
)

// BenchmarkMemoryTrackerSamplingRates measures the performance impact of memory tracking
// across different sampling rates from 0% (no sampling) to 100% (always sample).
//
// This benchmark demonstrates why the default sampling rate is set to 20%. It provides
// observability while minimizing the overhead from runtime.ReadMemStats stop-the-world pauses.
// The tracker is generic and can be used for any operation requiring memory estimation validation.
//
// Run with: go test -bench=BenchmarkMemoryTrackerSamplingRates -benchtime=10s
func BenchmarkMemoryTrackerSamplingRates(b *testing.B) {
	samplingRates := []struct {
		name string
		rate float64
	}{
		{"rate_0.0", 0.0},
		{"rate_0.1", 0.1},
		{"rate_0.2", 0.2},
		{"rate_0.3", 0.3},
		{"rate_0.4", 0.4},
		{"rate_0.5", 0.5},
		{"rate_0.6", 0.6},
		{"rate_0.7", 0.7},
		{"rate_0.8", 0.8},
		{"rate_0.9", 0.9},
		{"rate_1.0", 1.0},
	}

	for _, samplingRate := range samplingRates {
		b.Run(samplingRate.name, func(b *testing.B) {
			promMetrics := createTestPrometheusMetrics()
			hnswMetrics := NewMetrics(promMetrics, "BenchClass", "BenchShard")
			realReader := &runtimeMemStatsReader{}

			// Use actual random number generator to get realistic sampling behavior
			rng := rand.New(rand.NewSource(42))
			tracker := newMemoryAllocationTracker(
				hnswMetrics,
				samplingRate.rate,
				rng.Float64,
				realReader,
			)

			estimatedMemory := int64(1024 * 1024)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tracker.BeginTracking(estimatedMemory)
				// Simulate some work being done during batch processing
				_ = make([]byte, 1024)
				tracker.EndTracking()
			}
		})
	}
}
