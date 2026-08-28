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
	"github.com/stretchr/testify/require"
)

type shardSeries struct {
	state ShardState
	reg   ShardRegistration
}

type shardCounts struct {
	vec    map[shardSeries]float64
	legacy map[ShardState]float64
}

func snapshotShards(m *PrometheusMetrics) shardCounts {
	c := shardCounts{
		vec:    map[shardSeries]float64{},
		legacy: map[ShardState]float64{},
	}
	for _, labels := range AllShardLabels() {
		s := shardSeries{state: ShardState(labels[0]), reg: ShardRegistration(labels[1])}
		c.vec[s] = testutil.ToFloat64(m.Shards.WithLabelValues(labels...))
	}
	c.legacy[ShardStateLoaded] = testutil.ToFloat64(m.ShardsLoaded)
	c.legacy[ShardStateUnloaded] = testutil.ToFloat64(m.ShardsUnloaded)
	c.legacy[ShardStateLoading] = testutil.ToFloat64(m.ShardsLoading)
	c.legacy[ShardStateUnloading] = testutil.ToFloat64(m.ShardsUnloading)
	return c
}

// TestShardTransitionsMoveBothMetrics pins every transition against the new
// weaviate_shards series it moves and the legacy gauge that must move with it,
// so the two stay reconcilable while consumers migrate.
func TestShardTransitionsMoveBothMetrics(t *testing.T) {
	tests := []struct {
		name       string
		apply      func(m *PrometheusMetrics)
		wantVec    map[shardSeries]float64
		wantLegacy map[ShardState]float64
	}{
		{
			name:  "registering an unloaded shard",
			apply: func(m *PrometheusMetrics) { m.NewUnloadedshard() },
			wantVec: map[shardSeries]float64{
				{ShardStateUnloaded, ShardRegistrationLazy}: 1,
			},
			wantLegacy: map[ShardState]float64{ShardStateUnloaded: 1},
		},
		{
			name:  "registering a shard that is born loaded",
			apply: func(m *PrometheusMetrics) { m.NewLoadedShard() },
			wantVec: map[shardSeries]float64{
				{ShardStateLoaded, ShardRegistrationEager}: 1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoaded: 1},
		},
		{
			name:  "an unloaded shard starts loading",
			apply: func(m *PrometheusMetrics) { m.StartLoadingShard() },
			wantVec: map[shardSeries]float64{
				{ShardStateUnloaded, ShardRegistrationLazy}: -1,
				{ShardStateLoading, ShardRegistrationLazy}:  1,
			},
			wantLegacy: map[ShardState]float64{ShardStateUnloaded: -1, ShardStateLoading: 1},
		},
		{
			name:  "a loading shard becomes loaded",
			apply: func(m *PrometheusMetrics) { m.FinishLoadingShard() },
			wantVec: map[shardSeries]float64{
				{ShardStateLoading, ShardRegistrationLazy}: -1,
				{ShardStateLoaded, ShardRegistrationLazy}:  1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoading: -1, ShardStateLoaded: 1},
		},
		{
			name:  "a load that fails returns the shard to unloaded",
			apply: func(m *PrometheusMetrics) { m.FailLoadingShard() },
			wantVec: map[shardSeries]float64{
				{ShardStateLoading, ShardRegistrationLazy}:  -1,
				{ShardStateUnloaded, ShardRegistrationLazy}: 1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoading: -1, ShardStateUnloaded: 1},
		},
		{
			name:  "a loaded lazy shard starts unloading",
			apply: func(m *PrometheusMetrics) { m.StartUnloadingShard(ShardRegistrationLazy) },
			wantVec: map[shardSeries]float64{
				{ShardStateLoaded, ShardRegistrationLazy}:    -1,
				{ShardStateUnloading, ShardRegistrationLazy}: 1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoaded: -1, ShardStateUnloading: 1},
		},
		{
			name:  "a loaded eager shard starts unloading",
			apply: func(m *PrometheusMetrics) { m.StartUnloadingShard(ShardRegistrationEager) },
			wantVec: map[shardSeries]float64{
				{ShardStateLoaded, ShardRegistrationEager}:    -1,
				{ShardStateUnloading, ShardRegistrationEager}: 1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoaded: -1, ShardStateUnloading: 1},
		},
		{
			name:  "an unloading lazy shard becomes unloaded",
			apply: func(m *PrometheusMetrics) { m.FinishUnloadingShard(ShardRegistrationLazy) },
			wantVec: map[shardSeries]float64{
				{ShardStateUnloading, ShardRegistrationLazy}: -1,
				{ShardStateUnloaded, ShardRegistrationLazy}:  1,
			},
			wantLegacy: map[ShardState]float64{ShardStateUnloading: -1, ShardStateUnloaded: 1},
		},
		{
			name:  "an unloading eager shard becomes unloaded",
			apply: func(m *PrometheusMetrics) { m.FinishUnloadingShard(ShardRegistrationEager) },
			wantVec: map[shardSeries]float64{
				{ShardStateUnloading, ShardRegistrationEager}: -1,
				{ShardStateUnloaded, ShardRegistrationEager}:  1,
			},
			wantLegacy: map[ShardState]float64{ShardStateUnloading: -1, ShardStateUnloaded: 1},
		},
		{
			name:  "dropping a loaded eager shard",
			apply: func(m *PrometheusMetrics) { m.DeleteLoadedShard(ShardRegistrationEager) },
			wantVec: map[shardSeries]float64{
				{ShardStateLoaded, ShardRegistrationEager}: -1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoaded: -1},
		},
		{
			name:  "dropping a loaded lazy shard",
			apply: func(m *PrometheusMetrics) { m.DeleteLoadedShard(ShardRegistrationLazy) },
			wantVec: map[shardSeries]float64{
				{ShardStateLoaded, ShardRegistrationLazy}: -1,
			},
			wantLegacy: map[ShardState]float64{ShardStateLoaded: -1},
		},
		{
			name:  "dropping an unloaded lazy shard",
			apply: func(m *PrometheusMetrics) { m.DeleteUnloadedShard(ShardRegistrationLazy) },
			wantVec: map[shardSeries]float64{
				{ShardStateUnloaded, ShardRegistrationLazy}: -1,
			},
			wantLegacy: map[ShardState]float64{ShardStateUnloaded: -1},
		},
		{
			name:  "dropping an unloaded eager shard",
			apply: func(m *PrometheusMetrics) { m.DeleteUnloadedShard(ShardRegistrationEager) },
			wantVec: map[shardSeries]float64{
				{ShardStateUnloaded, ShardRegistrationEager}: -1,
			},
			wantLegacy: map[ShardState]float64{ShardStateUnloaded: -1},
		},
	}

	m := GetMetrics()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := snapshotShards(m)
			tt.apply(m)
			after := snapshotShards(m)

			for s, got := range after.vec {
				assert.Equal(t, tt.wantVec[s], got-before.vec[s],
					"weaviate_shards{state=%q,registration=%q}", s.state, s.reg)
			}
			for state, got := range after.legacy {
				assert.Equal(t, tt.wantLegacy[state], got-before.legacy[state],
					"legacy gauge for state %q", state)
			}
		})
	}
}

// TestShardStatesSumToTheLegacyGauges is the migration contract: a consumer
// moving off shards_loaded and its siblings gets the same number from
// sum by (state) over weaviate_shards.
func TestShardStatesSumToTheLegacyGauges(t *testing.T) {
	m := GetMetrics()

	// A lazy collection waking two of three shards and putting one back to
	// sleep, alongside an eager collection that opens its shard at creation.
	m.NewUnloadedshard()
	m.NewUnloadedshard()
	m.NewUnloadedshard()
	m.NewLoadedShard()

	m.StartLoadingShard()
	m.FinishLoadingShard()
	m.StartLoadingShard()
	m.FinishLoadingShard()

	m.StartUnloadingShard(ShardRegistrationLazy)
	m.FinishUnloadingShard(ShardRegistrationLazy)

	counts := snapshotShards(m)
	for state, legacy := range counts.legacy {
		sum := counts.vec[shardSeries{state, ShardRegistrationEager}] +
			counts.vec[shardSeries{state, ShardRegistrationLazy}]
		assert.Equal(t, legacy, sum, "sum by (state) for %q", state)
	}
}

// TestShardsExportsEveryStateAtStartup keeps a node whose collections are all
// eager from omitting the lazy series instead of scraping zero.
func TestShardsExportsEveryStateAtStartup(t *testing.T) {
	m := GetMetrics()

	require.Equal(t, 8, testutil.CollectAndCount(m.Shards))

	for _, labels := range AllShardLabels() {
		assert.NotPanics(t, func() {
			testutil.ToFloat64(m.Shards.WithLabelValues(labels...))
		}, "series %v must exist", labels)
	}
}

// TestWarmupOutcomeCountsOnItsOwnSeries pins each outcome to one series, so a
// dashboard summing them reads the sweep's decisions without double counting.
func TestWarmupOutcomeCountsOnItsOwnSeries(t *testing.T) {
	m := GetMetrics()

	outcomes := []WarmupOutcome{
		WarmupLoaded,
		WarmupFailed,
		WarmupSkippedShardGone,
		WarmupSkippedAlreadyLoaded,
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
}

// TestRecordWarmupOutcomeToleratesNilMetrics covers an index built without
// monitoring, which passes nil here.
func TestRecordWarmupOutcomeToleratesNilMetrics(t *testing.T) {
	var nilMetrics *PrometheusMetrics

	assert.NotPanics(t, func() {
		nilMetrics.RecordWarmupOutcome(WarmupLoaded)
	})
}
