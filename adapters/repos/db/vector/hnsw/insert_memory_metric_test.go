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
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// Note: This test file stays in package hnsw (not hnsw_test) because it needs
// to set unexported fields on the hnsw index for comprehensive integration testing
// of memory metrics behavior.

func TestBatchMemoryEstimationMetricIsSetAfterAddBatch(t *testing.T) {
	// GIVEN an HNSW index with metrics enabled and sampling forced to 100%
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = hnswMetrics

	// Force sampling to always happen by returning 0.1 (< 0.2 sampling rate)
	// but still valid for level calculation
	forceSamplingRandFunc := func() float64 { return 0.1 }
	index.randFunc = forceSamplingRandFunc
	index.memoryTracker = newMemoryAllocationTracker(hnswMetrics, alwaysSampleMemoryMetrics, forceSamplingRandFunc, &fakeMemStatsReader{
		allocValues: []uint64{1000, 3000},
	})

	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
		{0.7, 0.8, 0.9},
	}
	ids := []uint64{1, 2, 3}

	// WHEN adding a batch of vectors
	err = index.AddBatch(ctx, ids, vectors)
	require.Nil(t, err)

	// THEN the memory estimation ratio metric should be set
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
	assert.Greater(t, ratio, 0.0, "Ratio should be positive")

	labels := foundMetric.GetMetric()[0].GetLabel()
	var className, shardName string
	for _, label := range labels {
		if label.GetName() == "class_name" {
			className = label.GetValue()
		}
		if label.GetName() == "shard_name" {
			shardName = label.GetValue()
		}
	}
	assert.Equal(t, "TestClass", className)
	assert.Equal(t, "TestShard", shardName)
}

func TestBatchMemoryEstimationMetricNotSetWhenMetricsDisabled(t *testing.T) {
	// GIVEN an HNSW index with metrics disabled
	ctx := context.Background()
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = NewMetrics(nil, "TestClass", "TestShard")

	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}
	ids := []uint64{1, 2}

	// WHEN adding a batch of vectors
	err = index.AddBatch(ctx, ids, vectors)

	// THEN it should not panic and should succeed
	require.Nil(t, err)
}

func TestBatchMemoryEstimationMetricNotSetForEmptyBatch(t *testing.T) {
	// GIVEN an HNSW index with metrics enabled
	ctx := context.Background()

	promMetrics := createTestPrometheusMetrics()
	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = hnswMetrics

	// WHEN trying to add an empty batch
	err = index.AddBatch(ctx, []uint64{}, [][]float32{})

	// THEN it should return an error before setting the metric
	require.NotNil(t, err, "Empty batch should return error")
}

func TestEstimateBatchMemoryForSingleVector(t *testing.T) {
	// GIVEN a single vector with 3 dimensions
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
	}

	// WHEN estimating batch memory
	estimated := estimateBatchMemory(vectors)

	// THEN it should calculate: 3 floats * 4 bytes + 30 bytes overhead = 42 bytes
	expected := int64(3*bytesPerFloat32 + overheadPerVector)
	assert.Equal(t, expected, estimated)
}

func TestEstimateBatchMemoryForMultipleVectors(t *testing.T) {
	// GIVEN multiple vectors with different dimensions
	vectors := [][]float32{
		{0.1, 0.2, 0.3},      // 3 * 4 + 30 = 42 bytes
		{0.4, 0.5, 0.6, 0.7}, // 4 * 4 + 30 = 46 bytes
	}

	// WHEN estimating batch memory
	estimated := estimateBatchMemory(vectors)

	// THEN it should sum both vector estimates
	expected := int64((3*bytesPerFloat32 + overheadPerVector) + (4*bytesPerFloat32 + overheadPerVector))
	assert.Equal(t, expected, estimated)
}

func TestEstimateBatchMemoryForEmptyVectorList(t *testing.T) {
	// GIVEN an empty vector list
	vectors := [][]float32{}

	// WHEN estimating batch memory
	estimated := estimateBatchMemory(vectors)

	// THEN it should return 0
	assert.Equal(t, int64(0), estimated)
}

func TestMemoryAllocationRejectedMetricIncrementsOnCheckAllocFailure(t *testing.T) {
	// GIVEN an HNSW index with a memory checker that will reject allocations
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexMemoryAllocationRejected)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	// Create a memory monitor that will reject all allocations
	rejectingMonitor := &rejectingAllocChecker{}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          rejectingMonitor,
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = hnswMetrics

	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}
	ids := []uint64{1, 2}

	// WHEN adding a batch that will be rejected due to memory
	err = index.AddBatch(ctx, ids, vectors)

	// THEN the operation should fail
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "not enough memory")

	// THEN the memory allocation rejected metric should be incremented
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_memory_allocation_rejected_total" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	require.Len(t, foundMetric.GetMetric(), 1, "Should have one metric")

	count := foundMetric.GetMetric()[0].GetCounter().GetValue()
	assert.Equal(t, 1.0, count, "Counter should be incremented once")

	labels := foundMetric.GetMetric()[0].GetLabel()
	var className, shardName string
	for _, label := range labels {
		if label.GetName() == "class_name" {
			className = label.GetValue()
		}
		if label.GetName() == "shard_name" {
			shardName = label.GetValue()
		}
	}
	assert.Equal(t, "TestClass", className)
	assert.Equal(t, "TestShard", shardName)
}

// rejectingAllocChecker is a test implementation that always rejects allocations
type rejectingAllocChecker struct{}

func (r *rejectingAllocChecker) CheckAlloc(sizeInBytes int64) error {
	return enterrors.ErrNotEnoughMemory
}

func (r *rejectingAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (r *rejectingAllocChecker) Refresh(updateMappings bool) {}

func TestAddBatchFailsWithVectorsOfDifferentDimensions(t *testing.T) {
	// GIVEN an HNSW index
	ctx := context.Background()
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	// WHEN adding a batch with vectors of different dimensions
	vectors := [][]float32{
		{0.1, 0.2},           // 2 dimensions
		{0.3, 0.4, 0.5},      // 3 dimensions
		{0.6, 0.7, 0.8, 0.9}, // 4 dimensions
	}
	ids := []uint64{1, 2, 3}

	err = index.AddBatch(ctx, ids, vectors)

	// THEN it should return an error
	require.NotNil(t, err, "AddBatch should fail with vectors of different dimensions")
	assert.Contains(t, err.Error(), "different lengths")
}

func TestMemoryAllocationRejectedMultipleTimes(t *testing.T) {
	// GIVEN an HNSW index with a rejecting memory checker
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexMemoryAllocationRejected)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	rejectingMonitor := &rejectingAllocChecker{}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          rejectingMonitor,
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = hnswMetrics

	vectors := [][]float32{
		{0.1, 0.2, 0.3},
	}
	ids := []uint64{1}

	// WHEN adding multiple batches that all get rejected
	err = index.AddBatch(ctx, ids, vectors)
	require.NotNil(t, err)

	err = index.AddBatch(ctx, []uint64{2}, vectors)
	require.NotNil(t, err)

	err = index.AddBatch(ctx, []uint64{3}, vectors)
	require.NotNil(t, err)

	// THEN the counter should be incremented three times
	metricFamilies, err := reg.Gather()
	require.Nil(t, err)

	var foundMetric *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "test_vector_index_memory_allocation_rejected_total" {
			foundMetric = mf
			break
		}
	}

	require.NotNil(t, foundMetric, "Metric should be present")
	count := foundMetric.GetMetric()[0].GetCounter().GetValue()
	assert.Equal(t, 3.0, count, "Counter should be incremented three times")
}

func TestEstimateBatchMemoryWithNilVector(t *testing.T) {
	// GIVEN a batch with nil vector
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		nil,
		{0.4, 0.5, 0.6},
	}

	// WHEN estimating batch memory
	estimated := estimateBatchMemory(vectors)

	// THEN it should handle nil gracefully (treating it as zero-length)
	expected := int64((3*bytesPerFloat32 + overheadPerVector) + overheadPerVector + (3*bytesPerFloat32 + overheadPerVector))
	assert.Equal(t, expected, estimated)
}

func TestBatchMemoryEstimationWithSingleVector(t *testing.T) {
	// GIVEN an HNSW index with metrics enabled
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = hnswMetrics

	forceSamplingRandFunc := func() float64 { return 0.1 }
	index.randFunc = forceSamplingRandFunc
	index.memoryTracker = newMemoryAllocationTracker(hnswMetrics, alwaysSampleMemoryMetrics, forceSamplingRandFunc, &fakeMemStatsReader{
		allocValues: []uint64{1000, 2000},
	})

	vectors := [][]float32{
		{0.1, 0.2, 0.3},
	}
	ids := []uint64{1}

	// WHEN adding a single-vector batch
	err = index.AddBatch(ctx, ids, vectors)
	require.Nil(t, err)

	// THEN the metric should still be set
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
	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Greater(t, ratio, 0.0, "Ratio should be positive for single vector batch")
}

func TestBatchMemoryEstimationWithLargeBatch(t *testing.T) {
	// GIVEN an HNSW index with metrics enabled
	ctx := context.Background()
	reg := prometheus.NewRegistry()

	promMetrics := createTestPrometheusMetrics()
	reg.MustRegister(promMetrics.VectorIndexBatchMemoryEstimationRatio)

	hnswMetrics := NewMetrics(promMetrics, "TestClass", "TestShard")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{0.1, 0.2, 0.3}, nil
	}

	cfg := Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
		AllocChecker:          memwatch.NewDummyMonitor(),
	}

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.metrics = hnswMetrics

	forceSamplingRandFunc := func() float64 { return 0.1 }
	index.randFunc = forceSamplingRandFunc
	index.memoryTracker = newMemoryAllocationTracker(hnswMetrics, alwaysSampleMemoryMetrics, forceSamplingRandFunc, &fakeMemStatsReader{
		allocValues: []uint64{10000, 50000},
	})

	// Create a larger batch
	vectors := make([][]float32, 100)
	ids := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		vectors[i] = []float32{0.1, 0.2, 0.3}
		ids[i] = uint64(i + 1)
	}

	// WHEN adding a large batch
	err = index.AddBatch(ctx, ids, vectors)
	require.Nil(t, err)

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
	ratio := foundMetric.GetMetric()[0].GetGauge().GetValue()
	assert.Greater(t, ratio, 0.0, "Ratio should be positive for large batch")
}
