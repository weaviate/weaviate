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

// Move the shard from unloaded to in progress
func (pm *PrometheusMetrics) StartLoadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsUnloaded.Dec()
	pm.ShardsLoading.Inc()
}

// Move the shard from in progress to loaded
func (pm *PrometheusMetrics) FinishLoadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoading.Dec()
	pm.ShardsLoaded.Inc()
}

// Revert shard from loading back to unloaded (when loading fails)
func (pm *PrometheusMetrics) FailLoadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoading.Dec()
	pm.ShardsUnloaded.Inc()
}

// Move the shard from loaded to in progress
func (pm *PrometheusMetrics) StartUnloadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoaded.Dec()
	pm.ShardsUnloading.Inc()
}

// Move the shard from in progress to unloaded
func (pm *PrometheusMetrics) FinishUnloadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsUnloading.Dec()
	pm.ShardsUnloaded.Inc()
}

// Register a new, unloaded shard
func (pm *PrometheusMetrics) NewUnloadedshard() {
	if pm == nil {
		return
	}

	pm.ShardsUnloaded.Inc()
}

// Register a new shard that is immediately loaded (for non-lazy loading path)
func (pm *PrometheusMetrics) NewLoadedShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoaded.Inc()
}

// Unregister a loaded shard (when it's deleted, not unloaded)
func (pm *PrometheusMetrics) DeleteLoadedShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoaded.Dec()
}

// Unregister an unloaded shard (when it's deleted without ever being loaded)
func (pm *PrometheusMetrics) DeleteUnloadedShard() {
	if pm == nil {
		return
	}

	pm.ShardsUnloaded.Dec()
}
