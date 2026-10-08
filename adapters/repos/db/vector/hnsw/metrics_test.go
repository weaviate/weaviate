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

package hnsw

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// rejectingAllocChecker always rejects allocations
type rejectingAllocChecker struct{}

func (r rejectingAllocChecker) CheckAlloc(sizeInBytes int64) error {
	return fmt.Errorf("allocation rejected: insufficient memory for %d bytes", sizeInBytes)
}

func (r rejectingAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (r rejectingAllocChecker) Refresh(updateMappings bool) {}

func TestHFreshModeDisablesVectorIndexSize(t *testing.T) {
	metrics := monitoring.GetMetrics()

	className := "HFreshMetricsTest"
	shardName := "hfresh-shard"

	sizeGauge := metrics.VectorIndexSize.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	sizeGauge.Set(0)
	require.Equal(t, float64(0), testutil.ToFloat64(sizeGauge))

	// Create metrics in HFresh mode
	// SetSize should be a no-op
	m := newMetrics(metrics, className, shardName, true)
	m.SetSize(42)
	require.Equal(t, float64(0), testutil.ToFloat64(sizeGauge),
		"VectorIndexSize should not change when HFreshMode is true")

	// SetSize should work
	m2 := newMetrics(metrics, className, shardName, false)
	m2.SetSize(42)
	require.Equal(t, float64(42), testutil.ToFloat64(sizeGauge),
		"VectorIndexSize should change when HFreshMode is false")
}

func TestAddBatchMemoryAllocationMetric(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
		{0.7, 0.8, 0.9},
	}
	ids := []uint64{0, 1, 2}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	metrics := monitoring.GetMetrics()

	// Get initial metric value
	initialValue := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "metrics-test-rejected",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, fmt.Errorf("vector not found")
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		AllocChecker:      rejectingAllocChecker{}, // This will reject all allocations
		PrometheusMetrics: metrics,
	}, ent.UserConfig{
		MaxConnections: 10,
		EFConstruction: 64,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	defer index.Shutdown(context.TODO())

	// Attempt to add batch - this should be rejected
	err = index.AddBatch(ctx, ids, vectors)
	require.NotNil(t, err, "AddBatch should fail due to allocation rejection")
	require.Contains(t, err.Error(), "allocation rejected", "error should mention allocation rejection")

	// Verify metric was incremented
	finalValue := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)
	require.Equal(t, initialValue+1, finalValue, "metric should increment by 1 after rejection")
}

func TestMemoryAllocationRejectedMetricHasNoLabels(t *testing.T) {
	metrics := monitoring.GetMetrics()
	value := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)
	require.GreaterOrEqual(t, value, float64(0), "metric should be readable without labels")
}

func TestAddBatchMemoryAllocationMultipleRejections(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	metrics := monitoring.GetMetrics()
	initialValue := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "metrics-test-multiple",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, fmt.Errorf("vector not found")
		},
		GetViewThunk:      GetViewThunk,
		AllocChecker:      rejectingAllocChecker{},
		PrometheusMetrics: metrics,
	}, ent.UserConfig{
		MaxConnections: 10,
		EFConstruction: 64,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	defer index.Shutdown(context.TODO())

	// Attempt multiple rejections
	for i := 0; i < 5; i++ {
		err = index.AddBatch(ctx, []uint64{uint64(i)}, [][]float32{vectors[0]})
		require.NotNil(t, err, "AddBatch should fail on attempt %d", i+1)
	}

	// Verify metric incremented by 5
	finalValue := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)
	require.Equal(t, initialValue+5, finalValue, "metric should increment by 5 after 5 rejections")
}

func TestAddBatchMemoryAllocationWithoutMetrics(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}
	ids := []uint64{0, 1}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	// Create index without PrometheusMetrics (nil)
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "metrics-test-nil",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, fmt.Errorf("vector not found")
		},
		GetViewThunk:      GetViewThunk,
		AllocChecker:      rejectingAllocChecker{},
		PrometheusMetrics: nil, // No metrics enabled
	}, ent.UserConfig{
		MaxConnections: 10,
		EFConstruction: 64,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	defer index.Shutdown(context.TODO())

	// Attempt to add batch - should fail due to allocation, but shouldn't panic
	// even though metrics are disabled
	err = index.AddBatch(ctx, ids, vectors)
	require.NotNil(t, err, "AddBatch should fail due to allocation rejection")
	require.Contains(t, err.Error(), "allocation rejected", "error should mention allocation rejection")

	// Test passes if no panic occurred
}

func TestAddBatchWithSuccessfulAllocation(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
		{0.7, 0.8, 0.9},
	}
	ids := []uint64{0, 1, 2}

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	metrics := monitoring.GetMetrics()

	// Get initial metric value
	initialValue := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "metrics-test-success",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) < len(vectors) {
				return vectors[id], nil
			}
			return nil, fmt.Errorf("vector not found")
		},
		GetViewThunk:      GetViewThunk,
		AllocChecker:      memwatch.NewDummyMonitor(), // This allows all allocations
		PrometheusMetrics: metrics,
	}, ent.UserConfig{
		MaxConnections: 10,
		EFConstruction: 64,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	defer index.Shutdown(context.TODO())

	// Add batch - this should succeed
	err = index.AddBatch(ctx, ids, vectors)
	require.Nil(t, err, "AddBatch should succeed with successful allocation")

	// Verify metric was NOT incremented
	finalValue := testutil.ToFloat64(metrics.VectorIndexMemoryAllocationRejected)
	require.Equal(t, initialValue, finalValue, "metric should not increment when allocation succeeds")
}

// TestRemovedTombstoneMetricsAreNotExported pins the removal of five per-shard
// tombstone series that were dropped to cut the number of series shipped to the
// metrics backend. A tombstone cleanup cycle is the only producer of these
// series, so running one with real Prometheus metrics must leave none of the
// removed names in the default registry.
func TestRemovedTombstoneMetricsAreNotExported(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForDeleteTest()

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "removed-tombstone-metrics",
		ClassName:             "RemovedTombstoneMetrics",
		ShardName:             "removed-tombstone-metrics-shard",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AllocChecker:                 memwatch.NewDummyMonitor(),
		PrometheusMetrics:            monitoring.GetMetrics(),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(context.TODO())

	for i, vec := range vectors {
		require.NoError(t, index.Add(ctx, uint64(i), vec))
	}
	for i := range vectors {
		if i%2 == 0 {
			require.NoError(t, index.Delete(uint64(i)))
		}
	}
	require.NoError(t, index.CleanUpTombstonedNodes(neverStop))

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	exported := make(map[string]struct{}, len(families))
	for _, f := range families {
		exported[f.GetName()] = struct{}{}
	}

	removed := []string{
		"tombstone_find_global_entrypoint",
		"tombstone_find_local_entrypoint",
		"vector_index_tombstone_cleaned",
		"vector_index_tombstone_cleanup_threads",
		"vector_index_tombstone_unexpected_total",
	}
	for _, name := range removed {
		_, ok := exported[name]
		require.False(t, ok, "removed metric %q is still exported", name)
	}

	// the cycle gauges stay and must still be produced by the same cycle
	for _, name := range []string{
		"vector_index_tombstones",
		"vector_index_tombstone_cycle_start_timestamp_seconds",
		"vector_index_tombstone_cycle_end_timestamp_seconds",
		"vector_index_tombstone_cycle_progress",
	} {
		_, ok := exported[name]
		require.True(t, ok, "kept metric %q is no longer exported", name)
	}
}
