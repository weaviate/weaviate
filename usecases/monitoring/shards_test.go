package monitoring

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShards(t *testing.T) {
	m := GetMetrics()
	// we share global `*prometheusMetrics` (GetMetrics()) in all the tests.
	// creating some weird `label` to avoid collition. Still the best effort.
	classNamePrefix := "test_shards_xyz"

	t.Run("start_loading_shard", func(t *testing.T) {
		className := fmt.Sprintf("%s-%s", classNamePrefix, t.Name())
		labels := prometheus.Labels{
			"class_name": className,
		}
		t.Cleanup(func() {
			m.ShardsLoading.DeletePartialMatch(labels)
			m.ShardsUnloaded.DeletePartialMatch(labels)
		})

		// invariant:
		// 1. `shards_loading` should be incremented
		// 2. `shards_unloaded` should be decremented

		// Setting base values
		mv, err := m.ShardsLoading.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		mv, err = m.ShardsUnloaded.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		err = m.StartLoadingShard(className)
		require.NoError(t, err)

		loadingCount := testutil.ToFloat64(m.ShardsLoading)
		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(2), loadingCount)
		assert.Equal(t, float64(0), unloadedCount)
	})

	t.Run("finish_loading_shard", func(t *testing.T) {
		className := fmt.Sprintf("%s-%s", classNamePrefix, t.Name())
		labels := prometheus.Labels{
			"class_name": className,
		}
		t.Cleanup(func() {
			m.ShardsLoading.DeletePartialMatch(labels)
			m.ShardsLoaded.DeletePartialMatch(labels)
		})

		// invariant:
		// 1. `shards_loading` should be decremented
		// 2. `shards_loaded` should be incremented

		// Setting base values
		mv, err := m.ShardsLoading.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		mv, err = m.ShardsLoaded.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		err = m.FinishLoadingShard(className)
		require.NoError(t, err)

		loadingCount := testutil.ToFloat64(m.ShardsLoading)
		loadedCount := testutil.ToFloat64(m.ShardsLoaded)

		assert.Equal(t, float64(0), loadingCount) // dec
		assert.Equal(t, float64(2), loadedCount)  // inc
	})

	t.Run("start_unloading_shard", func(t *testing.T) {
		className := fmt.Sprintf("%s-%s", classNamePrefix, t.Name())
		labels := prometheus.Labels{
			"class_name": className,
		}
		t.Cleanup(func() {
			m.ShardsLoaded.DeletePartialMatch(labels)
			m.ShardsUnloading.DeletePartialMatch(labels)
		})

		// invariant:
		// 1. `shards_loaded` should be decremented
		// 2. `shards_unloading` should be incremented

		// Setting base values
		mv, err := m.ShardsLoaded.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		mv, err = m.ShardsUnloading.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		err = m.StartUnloadingShard(className)
		require.NoError(t, err)

		loadedCount := testutil.ToFloat64(m.ShardsLoaded)
		unloadingCount := testutil.ToFloat64(m.ShardsUnloading)

		assert.Equal(t, float64(0), loadedCount)    // dec
		assert.Equal(t, float64(2), unloadingCount) // inc
	})

	t.Run("finish_unloading_shard", func(t *testing.T) {
		className := fmt.Sprintf("%s-%s", classNamePrefix, t.Name())
		labels := prometheus.Labels{
			"class_name": className,
		}
		t.Cleanup(func() {
			m.ShardsUnloading.DeletePartialMatch(labels)
			m.ShardsUnloaded.DeletePartialMatch(labels)
		})

		// invariant:
		// 1. `shards_unloading` should be decremented
		// 2. `shards_unloaded` should be incremented

		// Setting base values
		mv, err := m.ShardsUnloading.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		mv, err = m.ShardsUnloaded.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		err = m.FinishUnloadingShard(className)
		require.NoError(t, err)

		unloadingCount := testutil.ToFloat64(m.ShardsUnloading)
		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(0), unloadingCount) // dec
		assert.Equal(t, float64(2), unloadedCount)  // inc
	})

	t.Run("new_unloaded_shard", func(t *testing.T) {
		className := fmt.Sprintf("%s-%s", classNamePrefix, t.Name())
		labels := prometheus.Labels{
			"class_name": className,
		}
		t.Cleanup(func() {
			m.ShardsUnloaded.DeletePartialMatch(labels)
		})

		// invariant:
		// 1. `shards_unloaded` should be incremented

		// Setting base values
		mv, err := m.ShardsUnloaded.GetMetricWith(labels)
		require.NoError(t, err)
		mv.Set(1)

		err = m.NewUnloadedshard(className)
		require.NoError(t, err)

		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(2), unloadedCount) // inc
	})
}
