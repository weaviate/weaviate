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
}
