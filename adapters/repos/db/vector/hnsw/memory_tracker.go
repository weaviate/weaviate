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
	"runtime"
	"sync"
)

const (
	// defaultMemoryMetricSamplingRate is the default probability (20%) that an operation
	// will be sampled for memory tracking. This balances observability with performance overhead
	// from runtime.ReadMemStats stop-the-world pauses.
	defaultMemoryMetricSamplingRate = 0.2

	// alwaysSampleMemoryMetrics indicates that all operations should be sampled (100%).
	// Used in tests to ensure deterministic behavior.
	alwaysSampleMemoryMetrics = 1.0
)

// memStatsReader provides an interface for reading runtime memory statistics.
type memStatsReader interface {
	ReadMemStats(m *runtime.MemStats)
}

// memoryAllocationTracker tracks memory allocation for operations with statistical sampling.
//
// Since runtime.ReadMemStats causes a stop-the-world pause that can impact performance,
// only a configurable percentage of operations are sampled based on the samplingRate parameter.
// This maintains observability while minimizing performance overhead.
//
// The tracker compares actual memory allocated during an operation against the
// estimated memory to produce a ratio metric. This ratio helps identify whether memory
// estimation is accurate, too conservative, or too aggressive.
//
// The tracker is generic and can be used for any operation where memory estimation needs
// to be validated against actual allocation.
type memoryAllocationTracker struct {
	metrics         *Metrics
	samplingRate    float64
	randFunc        func() float64
	memStatsReader  memStatsReader
	memBefore       runtime.MemStats
	estimatedMemory int64
	shouldTrack     bool
	mu              *sync.Mutex
}

// newMemoryAllocationTracker creates a new memory allocation tracker.
//
// The tracker is intended to be created once and reused across multiple operations.
// It uses the provided randFunc to determine which operations to sample based on the
// provided samplingRate.
//
// Parameters:
//   - metrics: The metrics instance to report memory estimation ratios to
//   - samplingRate: The probability (0.0 to 1.0) that an operation will be sampled for memory tracking
//   - randFunc: A function returning random float64 values in [0.0, 1.0) used for sampling decisions
//   - reader: A reader for runtime.MemStats that can be mocked for testing
//
// Returns a tracker that can be reused for multiple operations via BeginTracking/EndTracking.
func newMemoryAllocationTracker(metrics *Metrics, samplingRate float64, randFunc func() float64, reader memStatsReader) *memoryAllocationTracker {
	return &memoryAllocationTracker{
		metrics:        metrics,
		samplingRate:   samplingRate,
		randFunc:       randFunc,
		memStatsReader: reader,
		mu:             &sync.Mutex{},
	}
}

// BeginTracking initiates memory tracking for an operation.
//
// This method should be called at the beginning of an operation, after the estimated
// memory usage has been calculated but before any memory-allocating work begins.
//
// The method uses statistical sampling to decide whether to track this particular operation.
// If sampled, it captures the current memory statistics as a baseline. If not sampled,
// it returns immediately with minimal overhead.
//
// Parameters:
//   - estimatedMemory: The estimated memory usage in bytes for the operation
func (t *memoryAllocationTracker) BeginTracking(estimatedMemory int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.estimatedMemory = estimatedMemory
	t.shouldTrack = t.randFunc() < t.samplingRate
	if !t.shouldTrack {
		return
	}

	t.memStatsReader.ReadMemStats(&t.memBefore)
}

// EndTracking completes memory tracking and reports the estimation ratio metric.
//
// This method should be called after the operation completes. If the operation was
// sampled (determined by BeginTracking), it captures the current memory statistics,
// calculates the actual memory allocated during the operation, and reports the ratio
// of actual to estimated memory.
//
// The ratio metric helps identify memory estimation accuracy:
//   - Ratio > 1.0: Estimation was too low (risk of OOM)
//   - Ratio ≈ 1.0: Estimation was accurate
//   - Ratio < 1.0: Estimation was too conservative (wasted memory headroom)
//
// If the operation was not sampled, this method returns immediately with no overhead.
func (t *memoryAllocationTracker) EndTracking() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.shouldTrack {
		return
	}

	var memAfter runtime.MemStats
	t.memStatsReader.ReadMemStats(&memAfter)
	actualMemory := int64(memAfter.Alloc - t.memBefore.Alloc)

	if t.estimatedMemory > 0 && actualMemory > 0 {
		ratio := float64(actualMemory) / float64(t.estimatedMemory)
		t.metrics.SetBatchMemoryEstimationRatio(ratio)
	}
}
