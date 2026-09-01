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

// ShardRegistration is the value of the closed `registration` label on
// weaviate_shards: eager for a shard opened when it was created, lazy for one
// registered unloaded and opened on first access.
type ShardRegistration string

const (
	ShardRegistrationEager ShardRegistration = "eager"
	ShardRegistrationLazy  ShardRegistration = "lazy"
)

// ShardState is the value of the closed `state` label on weaviate_shards. A
// shard the node holds is in exactly one of these, so summing the states gives
// the number of shards in the shard map.
type ShardState string

const (
	ShardStateLoaded    ShardState = "loaded"
	ShardStateUnloaded  ShardState = "unloaded"
	ShardStateLoading   ShardState = "loading"
	ShardStateUnloading ShardState = "unloading"
)

// AllShardLabels is every combination weaviate_shards can report. A GaugeVec
// exports no series until a label value is used, so without priming these a
// node whose collections are all eager omits registration="lazy" entirely
// instead of scraping zero.
func AllShardLabels() [][]string {
	var labels [][]string
	for _, reg := range []ShardRegistration{ShardRegistrationEager, ShardRegistrationLazy} {
		for _, state := range []ShardState{ShardStateLoaded, ShardStateUnloaded, ShardStateLoading, ShardStateUnloading} {
			labels = append(labels, []string{string(state), string(reg)})
		}
	}
	return labels
}

// enterShardState counts a shard the node did not hold before.
func (pm *PrometheusMetrics) enterShardState(reg ShardRegistration, state ShardState) {
	pm.Shards.WithLabelValues(string(state), string(reg)).Inc()
}

// leaveShardState stops counting a shard the node no longer holds.
func (pm *PrometheusMetrics) leaveShardState(reg ShardRegistration, state ShardState) {
	pm.Shards.WithLabelValues(string(state), string(reg)).Dec()
}

// moveShardState keeps the shard counted while changing which state holds it.
func (pm *PrometheusMetrics) moveShardState(reg ShardRegistration, from, to ShardState) {
	pm.leaveShardState(reg, from)
	pm.enterShardState(reg, to)
}

// WarmupOutcome is the value of the closed `outcome` label on
// weaviate_lazy_shard_warmup_decisions_total: what the startup sweep that loads
// lazy shards did with one of them.
type WarmupOutcome string

const (
	// WarmupLoaded: the sweep loaded the shard.
	WarmupLoaded WarmupOutcome = "loaded"
	// WarmupFailed: the sweep attempted the load and it failed.
	WarmupFailed WarmupOutcome = "failed"
	// WarmupSkippedShardGone: the index no longer holds the shard, its tenant
	// having been deactivated or deleted since the sweep listed it.
	WarmupSkippedShardGone WarmupOutcome = "skipped_shard_gone"
	// WarmupSkippedAlreadyLoaded: something reached the shard before the sweep
	// did, so there was nothing left to load.
	WarmupSkippedAlreadyLoaded WarmupOutcome = "skipped_already_loaded"
	// WarmupSkippedEmpty: the shard has never held an object.
	WarmupSkippedEmpty WarmupOutcome = "skipped_empty"
	// WarmupSkippedBelowThreshold: the shard holds too few objects for
	// LAZY_LOAD_SHARD_WARMUP_MIN_OBJECTS.
	WarmupSkippedBelowThreshold WarmupOutcome = "skipped_below_threshold"
)

// RecordWarmupOutcome records what the startup warmup sweep did with one shard.
func (pm *PrometheusMetrics) RecordWarmupOutcome(outcome WarmupOutcome) {
	if pm == nil {
		return
	}

	pm.LazyShardWarmupDecisions.WithLabelValues(string(outcome)).Inc()
}

// Move the shard from unloaded to in progress. Only a lazy shard loads this
// way; an eager one is born loaded.
func (pm *PrometheusMetrics) StartLoadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsUnloaded.Dec()
	pm.ShardsLoading.Inc()
	pm.moveShardState(ShardRegistrationLazy, ShardStateUnloaded, ShardStateLoading)
}

// Move the shard from in progress to loaded
func (pm *PrometheusMetrics) FinishLoadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoading.Dec()
	pm.ShardsLoaded.Inc()
	pm.moveShardState(ShardRegistrationLazy, ShardStateLoading, ShardStateLoaded)
}

// Revert shard from loading back to unloaded (when loading fails)
func (pm *PrometheusMetrics) FailLoadingShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoading.Dec()
	pm.ShardsUnloaded.Inc()
	pm.moveShardState(ShardRegistrationLazy, ShardStateLoading, ShardStateUnloaded)
}

// Move the shard from loaded to in progress
func (pm *PrometheusMetrics) StartUnloadingShard(reg ShardRegistration) {
	if pm == nil {
		return
	}

	pm.ShardsLoaded.Dec()
	pm.ShardsUnloading.Inc()
	pm.moveShardState(reg, ShardStateLoaded, ShardStateUnloading)
}

// Move the shard from in progress to unloaded
func (pm *PrometheusMetrics) FinishUnloadingShard(reg ShardRegistration) {
	if pm == nil {
		return
	}

	pm.ShardsUnloading.Dec()
	pm.ShardsUnloaded.Inc()
	pm.moveShardState(reg, ShardStateUnloading, ShardStateUnloaded)
}

// Register a new, unloaded shard. Only a lazy shard starts out unloaded.
func (pm *PrometheusMetrics) NewUnloadedshard() {
	if pm == nil {
		return
	}

	pm.ShardsUnloaded.Inc()
	pm.enterShardState(ShardRegistrationLazy, ShardStateUnloaded)
}

// Register a new shard that is immediately loaded (for non-lazy loading path)
func (pm *PrometheusMetrics) NewLoadedShard() {
	if pm == nil {
		return
	}

	pm.ShardsLoaded.Inc()
	pm.enterShardState(ShardRegistrationEager, ShardStateLoaded)
}

// Unregister a loaded shard (when it's deleted, not unloaded)
func (pm *PrometheusMetrics) DeleteLoadedShard(reg ShardRegistration) {
	if pm == nil {
		return
	}

	pm.ShardsLoaded.Dec()
	pm.leaveShardState(reg, ShardStateLoaded)
}

// Unregister an unloaded shard (when it's deleted without ever being loaded)
func (pm *PrometheusMetrics) DeleteUnloadedShard(reg ShardRegistration) {
	if pm == nil {
		return
	}

	pm.ShardsUnloaded.Dec()
	pm.leaveShardState(reg, ShardStateUnloaded)
}

// SetStartupShardProgress publishes the latest eager shard-loading progress
// (loaded so far / expected to load eagerly) computed during startup.
func (pm *PrometheusMetrics) SetStartupShardProgress(loaded, total int64) {
	if pm == nil {
		return
	}

	pm.StartupShardsLoaded.Set(float64(loaded))
	pm.StartupShardsToLoad.Set(float64(total))
}
