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

package monitoring

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestShards(t *testing.T) {
	m := GetMetrics()

	t.Run("start_loading_shard", func(t *testing.T) {
		// Setting base values
		mv := m.ShardsLoading
		mv.Set(1)

		mv = m.ShardsUnloaded
		mv.Set(1)

		m.StartLoadingShard()

		loadingCount := testutil.ToFloat64(m.ShardsLoading)
		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(2), loadingCount)
		assert.Equal(t, float64(0), unloadedCount)
	})

	t.Run("finish_loading_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_loading` should be decremented
		// 2. `shards_loaded` should be incremented

		// Setting base values
		mv := m.ShardsLoading
		mv.Set(1)

		mv = m.ShardsLoaded
		mv.Set(1)

		m.FinishLoadingShard()

		loadingCount := testutil.ToFloat64(m.ShardsLoading)
		loadedCount := testutil.ToFloat64(m.ShardsLoaded)

		assert.Equal(t, float64(0), loadingCount) // dec
		assert.Equal(t, float64(2), loadedCount)  // inc
	})

	t.Run("fail_loading_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_loading` should be decremented
		// 2. `shards_unloaded` should be incremented

		// Setting base values
		mv := m.ShardsLoading
		mv.Set(1)

		mv = m.ShardsUnloaded
		mv.Set(1)

		m.FailLoadingShard()

		loadingCount := testutil.ToFloat64(m.ShardsLoading)
		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(0), loadingCount)  // dec
		assert.Equal(t, float64(2), unloadedCount) // inc
	})

	t.Run("start_unloading_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_loaded` should be decremented
		// 2. `shards_unloading` should be incremented

		// Setting base values
		mv := m.ShardsLoaded
		mv.Set(1)

		mv = m.ShardsUnloading
		mv.Set(1)

		m.StartUnloadingShard()

		loadedCount := testutil.ToFloat64(m.ShardsLoaded)
		unloadingCount := testutil.ToFloat64(m.ShardsUnloading)

		assert.Equal(t, float64(0), loadedCount)    // dec
		assert.Equal(t, float64(2), unloadingCount) // inc
	})

	t.Run("finish_unloading_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_unloading` should be decremented
		// 2. `shards_unloaded` should be incremented

		// Setting base values
		mv := m.ShardsUnloading
		mv.Set(1)

		mv = m.ShardsUnloaded
		mv.Set(1)

		m.FinishUnloadingShard()

		unloadingCount := testutil.ToFloat64(m.ShardsUnloading)
		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(0), unloadingCount) // dec
		assert.Equal(t, float64(2), unloadedCount)  // inc
	})

	t.Run("new_unloaded_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_unloaded` should be incremented

		// Setting base values
		mv := m.ShardsUnloaded
		mv.Set(1)

		m.NewUnloadedshard()

		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(2), unloadedCount) // inc
	})

	t.Run("new_loaded_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_loaded` should be incremented

		// Setting base values
		mv := m.ShardsLoaded
		mv.Set(1)

		m.NewLoadedShard()

		loadedCount := testutil.ToFloat64(m.ShardsLoaded)

		assert.Equal(t, float64(2), loadedCount) // inc
	})

	t.Run("delete_loaded_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_loaded` should be decremented

		// Setting base values
		mv := m.ShardsLoaded
		mv.Set(2)

		m.DeleteLoadedShard()

		loadedCount := testutil.ToFloat64(m.ShardsLoaded)

		assert.Equal(t, float64(1), loadedCount) // dec
	})

	t.Run("delete_unloaded_shard", func(t *testing.T) {
		// invariant:
		// 1. `shards_unloaded` should be decremented

		// Setting base values
		mv := m.ShardsUnloaded
		mv.Set(2)

		m.DeleteUnloadedShard()

		unloadedCount := testutil.ToFloat64(m.ShardsUnloaded)

		assert.Equal(t, float64(1), unloadedCount) // dec
	})

	t.Run("record_warmup_outcome", func(t *testing.T) {
		// invariant: each outcome counts on its own series and on no other.
		outcomes := []WarmupOutcome{
			WarmupLoaded,
			WarmupFailed,
			WarmupSkippedNotCold,
			WarmupSkippedEmpty,
			WarmupSkippedBelowThreshold,
		}

		readAll := func() map[WarmupOutcome]float64 {
			counts := make(map[WarmupOutcome]float64, len(outcomes))
			for _, outcome := range outcomes {
				counts[outcome] = testutil.ToFloat64(
					m.LazyShardWarmupDecisions.WithLabelValues(string(outcome)))
			}
			return counts
		}

		for _, recorded := range outcomes {
			t.Run(string(recorded), func(t *testing.T) {
				before := readAll()

				m.RecordWarmupOutcome(recorded)

				after := readAll()
				for _, outcome := range outcomes {
					want := before[outcome]
					if outcome == recorded {
						want++
					}
					assert.Equal(t, want, after[outcome], "outcome %q", outcome)
				}
			})
		}
	})

	t.Run("nil_metrics_record_nothing", func(t *testing.T) {
		// An index built without monitoring passes nil here, so every helper has
		// to tolerate it.
		var nilMetrics *PrometheusMetrics

		assert.NotPanics(t, func() {
			nilMetrics.RecordWarmupOutcome(WarmupLoaded)
		})
	})
}
