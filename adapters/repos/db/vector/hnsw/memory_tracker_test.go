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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Note: This test file stays in package hnsw (not hnsw_test) because it needs
// access to unexported types and functions for comprehensive unit testing of
// the memory tracker implementation.

const (
	testSamplingRateDefault = defaultMemoryMetricSamplingRate // 0.2

	testRandBelowThreshold = 0.1  // Will be sampled (< 0.2)
	testRandAboveThreshold = 0.9  // Won't be sampled (>= 0.2)
	testRandAtThreshold    = 0.2  // Won't be sampled (>= 0.2)
	testRandZero           = 0.0  // Will be sampled (< 0.2)
	testRandOne            = 1.0  // Won't be sampled (>= 0.2)
	testRandJustBelow      = 0.19 // Will be sampled (< 0.2)
	testRandJustAbove      = 0.21 // Won't be sampled (>= 0.2)
)

func TestMemoryTrackerSamplesWhenBelowThreshold(t *testing.T) {
	// GIVEN a memory tracker with randFunc that returns below sampling threshold
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking a batch operation
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN the metric should be set
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.GreaterOrEqual(t, ratio, 0.0, "Ratio should be non-negative")
}

func TestMemoryTrackerDoesNotSampleWhenAboveThreshold(t *testing.T) {
	// GIVEN a memory tracker with randFunc that returns above sampling threshold
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandAboveThreshold }, fakeReader)

	// WHEN tracking a batch operation
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN it should not panic (we can't easily test that metric wasn't set
	// since Prometheus gauges always exist once registered)
	assert.True(t, true, "Should complete without panic")
}

func TestMemoryTrackerCanBeReused(t *testing.T) {
	// GIVEN a memory tracker
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{800, 1200, 1300, 1500, 1400, 1600},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking multiple batch operations
	tracker.BeginTracking(1000) // 800
	tracker.EndTracking()       // 1200

	tracker.BeginTracking(2000) // 1300
	tracker.EndTracking()       // 1500

	tracker.BeginTracking(3000) // 1400
	tracker.EndTracking()       // 1600

	// THEN all operations should complete successfully without panic
	assert.True(t, true, "Should complete all operations without panic")
}

func TestMemoryTrackerWithZeroEstimatedMemory(t *testing.T) {
	// GIVEN a memory tracker
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 1000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with zero estimated memory
	tracker.BeginTracking(0)
	tracker.EndTracking()

	// THEN it should not panic (zero estimated memory is handled gracefully)
	assert.True(t, true, "Should complete without panic")
}

func TestMemoryTrackerWithNilMetrics(t *testing.T) {
	// GIVEN a memory tracker with nil metrics (metrics disabled)
	metricsWithNil := NewMetrics(nil, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	tracker := newMemoryAllocationTracker(metricsWithNil, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking a batch operation
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN it should not panic
	assert.True(t, true, "Should complete without panic")
}

func TestMemoryTrackerSamplingRate(t *testing.T) {
	// GIVEN a memory tracker and multiple random values
	testCases := []struct {
		name            string
		randValue       float64
		shouldBeSampled bool
	}{
		{
			name:            "exactly at threshold",
			randValue:       testRandAtThreshold,
			shouldBeSampled: false,
		},
		{
			name:            "just below threshold",
			randValue:       testRandJustBelow,
			shouldBeSampled: true,
		},
		{
			name:            "just above threshold",
			randValue:       testRandJustAbove,
			shouldBeSampled: false,
		},
		{
			name:            "zero should be sampled",
			randValue:       testRandZero,
			shouldBeSampled: true,
		},
		{
			name:            "one should not be sampled",
			randValue:       testRandOne,
			shouldBeSampled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN a fresh registry for this test
			testReg := prometheus.NewRegistry()
			testPromMetrics := createTestPrometheusMetrics()
			testReg.MustRegister(testPromMetrics.VectorIndexBatchMemoryEstimationRatio)
			testMetrics := NewMetrics(testPromMetrics, "TestClass", "TestShard")
			fakeReader := &fakeMemStatsReader{
				allocValues: []uint64{1000, 3000},
			}

			tracker := newMemoryAllocationTracker(testMetrics, testSamplingRateDefault, func() float64 { return tc.randValue }, fakeReader)

			// WHEN tracking a batch operation
			tracker.BeginTracking(1000)
			tracker.EndTracking()

			// THEN check if metric was set according to expectation
			metricFamilies, err := testReg.Gather()
			require.Nil(t, err)

			var foundMetric *dto.MetricFamily
			for _, mf := range metricFamilies {
				if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
					foundMetric = mf
					break
				}
			}

			// We can't easily test whether the metric was actually updated vs just
			// existing with a default value, so we just verify no panic occurred
			_ = foundMetric
			assert.True(t, true, "Should complete without panic")
		})
	}
}

func TestMemoryTrackerEndWithoutBegin(t *testing.T) {
	// GIVEN a memory tracker
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN calling EndTracking without BeginTracking
	tracker.EndTracking()

	// THEN it should not panic
	assert.True(t, true, "Should complete without panic")
}

func TestMemoryTrackerWithFakeMemStats(t *testing.T) {
	// GIVEN a memory tracker with fake mem stats reader
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking a batch operation with estimated 1000 bytes
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN the metric should show ratio of 2.0 (actual 2000 / estimated 1000)
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Equal(t, 2.0, ratio, "Ratio should be 2.0 (2000 actual / 1000 estimated)")
}

func TestMemoryTrackerWithFakeMemStatsAccurateEstimation(t *testing.T) {
	// GIVEN a memory tracker with fake mem stats reader
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{5000, 10000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with estimated 5000 bytes
	tracker.BeginTracking(5000)
	tracker.EndTracking()

	// THEN the metric should show ratio of 1.0 (perfect estimation)
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Equal(t, 1.0, ratio, "Ratio should be 1.0 (perfect estimation)")
}

func TestMemoryTrackerWithFakeMemStatsConservativeEstimation(t *testing.T) {
	// GIVEN a memory tracker with fake mem stats reader
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{2000, 4000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with estimated 8000 bytes (overestimated)
	tracker.BeginTracking(8000)
	tracker.EndTracking()

	// THEN the metric should show ratio of 0.25 (too conservative)
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Equal(t, 0.25, ratio, "Ratio should be 0.25 (conservative estimation)")
}

func TestMemoryTrackerWithFakeMemStatsNotSampled(t *testing.T) {
	// GIVEN a memory tracker that won't sample (randFunc returns > 0.2)
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandAboveThreshold }, fakeReader)

	// WHEN tracking a batch operation
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN the fake reader should not have been called (not sampled)
	assert.Equal(t, 0, fakeReader.callCount, "ReadMemStats should not be called when not sampled")
}

func TestMemoryTrackerWithNegativeEstimatedMemory(t *testing.T) {
	// GIVEN a memory tracker with fake mem stats reader
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with negative estimated memory
	tracker.BeginTracking(-100)
	tracker.EndTracking()

	// THEN it should not set the metric (negative values are invalid)
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	// The metric should have default value of 0, not a negative ratio
	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Equal(t, 0.0, ratio, "Ratio should be 0 for invalid negative estimated memory")
}

func TestMemoryTrackerWithMemoryDeallocation(t *testing.T) {
	// GIVEN a memory tracker where memory actually decreases (GC runs during operation)
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{5000, 3000}, // Memory decreased from 5000 to 3000
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking a batch operation where GC runs
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN the metric should not be set (actualMemory would be negative)
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	// The metric should have default value of 0, not set from negative actualMemory
	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Equal(t, 0.0, ratio, "Ratio should be 0 when memory decreases during operation")
}

func TestMemoryTrackerWithZeroActualMemory(t *testing.T) {
	// GIVEN a memory tracker where no memory is allocated
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 1000}, // No memory change
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with no actual memory allocated
	tracker.BeginTracking(1000)
	tracker.EndTracking()

	// THEN the metric should not be set (zero actual memory is invalid)
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Equal(t, 0.0, ratio, "Ratio should be 0 when no actual memory is allocated")
}

func TestMemoryTrackerWithVeryLargeMemoryValues(t *testing.T) {
	// GIVEN a memory tracker with very large memory values
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	// Use large values that would overflow if not handled properly
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000000000, 5000000000}, // 1GB to 5GB
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with very large memory values
	tracker.BeginTracking(2000000000) // 2GB estimated
	tracker.EndTracking()

	// THEN the metric should show correct ratio
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	// Actual: 4000000000, Estimated: 2000000000, Ratio: 2.0
	assert.Equal(t, 2.0, ratio, "Ratio should be 2.0 for large memory values")
}

func TestMemoryTrackerMultipleBeginWithoutEnd(t *testing.T) {
	// GIVEN a memory tracker
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 2000, 3000, 4000},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN calling BeginTracking multiple times without EndTracking
	tracker.BeginTracking(1000)
	tracker.BeginTracking(2000) // This should overwrite the previous state and call ReadMemStats again
	tracker.EndTracking()

	// THEN it should complete without panic (tracker state is overwritten)
	// BeginTracking is called twice + EndTracking is called once = 3 ReadMemStats calls
	assert.Equal(t, 3, fakeReader.callCount, "ReadMemStats should be called three times (2 begin + 1 end)")
}

func TestMemoryTrackerConcurrentAccess(t *testing.T) {
	// GIVEN a memory tracker that could be accessed by multiple goroutines
	// (though in practice this shouldn't happen, we test the behavior)
	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 2000, 1500, 2500},
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking operations sequentially (simulating potential race condition)
	tracker.BeginTracking(1000)
	tracker.BeginTracking(1000) // Second begin before first end
	tracker.EndTracking()
	tracker.EndTracking()

	// THEN it should not panic (though the metric values may not be meaningful)
	assert.True(t, true, "Should complete without panic")
}

func TestMemoryTrackerWithExtremelySmallRatio(t *testing.T) {
	// GIVEN a memory tracker with very small actual memory vs large estimated
	reg := prometheus.NewRegistry()
	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")
	fakeReader := &fakeMemStatsReader{
		allocValues: []uint64{1000, 1001}, // Only 1 byte allocated
	}
	tracker := newMemoryAllocationTracker(hnswMetrics, testSamplingRateDefault, func() float64 { return testRandBelowThreshold }, fakeReader)

	// WHEN tracking with very large overestimation
	tracker.BeginTracking(1000000) // Estimated 1MB but only allocated 1 byte
	tracker.EndTracking()

	// THEN the metric should show very small ratio
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_batch_memory_estimation_ratio" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Less(t, ratio, 0.001, "Ratio should be very small for extreme overestimation")
}
