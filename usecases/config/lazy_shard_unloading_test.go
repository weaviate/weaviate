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

package config

import (
	"io"
	"math"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func lazyShardUnloadingValues(c LazyShardUnloading) map[string]any {
	return map[string]any{
		"idle_ttl": c.IdleTTL.Get(), "idle_check_interval": c.IdleCheckInterval.Get(), "max_loaded_shards": c.MaxLoadedShards.Get(),
		"memory_high_watermark": c.MemoryHighWatermark.Get(), "memory_low_watermark": c.MemoryLowWatermark.Get(),
		"min_residency": c.MinResidency.Get(), "unload_rate": c.UnloadRate.Get(), "interval": c.Interval.Get(),
		"make_room_candidates": c.MakeRoomCandidates.Get(), "unload_workers": c.UnloadWorkers.Get(),
		"namespace_max_share": c.NamespaceMaxShare.Get(), "paused": c.Paused.Get(),
	}
}

func defaultLazyShardUnloadingValues() map[string]any {
	return map[string]any{
		"idle_ttl": time.Duration(0), "idle_check_interval": 30 * time.Second, "max_loaded_shards": 0,
		"memory_high_watermark": 0.0, "memory_low_watermark": 0.0, "min_residency": 30 * time.Second,
		"unload_rate": 50, "interval": 5 * time.Second, "make_room_candidates": 16, "unload_workers": 4,
		"namespace_max_share": 0.0, "paused": false,
	}
}

// The knobs come only from the runtime overrides file: an env var, valid or
// not, neither seeds one nor fails startup on an unlicensed node.
func TestLazyShardUnloadingIgnoresEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "nothing set"},
		{name: "valid values", env: map[string]string{
			"LAZY_SHARD_UNLOADING_IDLE_TTL": "10m", "LAZY_SHARD_UNLOADING_MAX_LOADED_SHARDS": "500",
			"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "0.9", "LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK": "0.7",
			"LAZY_SHARD_UNLOADING_PAUSED": "true",
		}},
		{name: "invalid values", env: map[string]string{
			"LAZY_SHARD_UNLOADING_INTERVAL": "0s", "LAZY_SHARD_UNLOADING_UNLOAD_WORKERS": "0",
			"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "0.5", "LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK": "0.7",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			conf := Config{}
			require.NoError(t, FromEnv(&conf))
			require.Equal(t, defaultLazyShardUnloadingValues(), lazyShardUnloadingValues(conf.LazyShardUnloading))
		})
	}
}

func TestLazyShardUnloadingRuntimeOverrides(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)
	conf := Config{}
	require.NoError(t, FromEnv(&conf))
	registered := BuildRegisteredRuntimeConfig(&conf)

	parsed, err := ParseRuntimeConfig([]byte(`lazy_shard_unloading_idle_ttl: 1h
lazy_shard_unloading_idle_check_interval: 2m
lazy_shard_unloading_max_loaded_shards: 100
lazy_shard_unloading_memory_high_watermark: 0.8
lazy_shard_unloading_memory_low_watermark: 0.6
lazy_shard_unloading_min_residency: 1m
lazy_shard_unloading_unload_rate: 7
lazy_shard_unloading_interval: 3s
lazy_shard_unloading_make_room_candidates: 8
lazy_shard_unloading_unload_workers: 2
lazy_shard_unloading_namespace_max_share: 0.5
lazy_shard_unloading_paused: true
`))
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	require.Equal(t, map[string]any{
		"idle_ttl": time.Hour, "idle_check_interval": 2 * time.Minute, "max_loaded_shards": 100,
		"memory_high_watermark": 0.8, "memory_low_watermark": 0.6, "min_residency": time.Minute,
		"unload_rate": 7, "interval": 3 * time.Second, "make_room_candidates": 8, "unload_workers": 2,
		"namespace_max_share": 0.5, "paused": true,
	}, lazyShardUnloadingValues(conf.LazyShardUnloading))

	parsed, err = ParseRuntimeConfig([]byte(`lazy_shard_unloading_unload_workers: 0
lazy_shard_unloading_interval: 10ms
lazy_shard_unloading_memory_high_watermark: 2
lazy_shard_unloading_max_loaded_shards: 5
`))
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	got := lazyShardUnloadingValues(conf.LazyShardUnloading)
	require.Equal(t, 2, got["unload_workers"], "an invalid value keeps the previous one")
	require.Equal(t, 3*time.Second, got["interval"])
	require.Equal(t, 0.8, got["memory_high_watermark"])
	require.Equal(t, 5, got["max_loaded_shards"])
	require.Equal(t, time.Duration(0), got["idle_ttl"], "a removed key reverts to its default")

	parsed, err = ParseRuntimeConfig(nil)
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	require.Equal(t, defaultLazyShardUnloadingValues(), lazyShardUnloadingValues(conf.LazyShardUnloading))
}

func TestBuildRegisteredRuntimeConfigSharesLazyShardUnloading(t *testing.T) {
	conf := Config{}
	require.NoError(t, FromEnv(&conf))
	r := BuildRegisteredRuntimeConfig(&conf)
	c := conf.LazyShardUnloading
	require.Same(t, c.IdleTTL, r.LazyShardUnloadingIdleTTL)
	require.Same(t, c.IdleCheckInterval, r.LazyShardUnloadingIdleCheckInterval)
	require.Same(t, c.MaxLoadedShards, r.LazyShardUnloadingMaxLoadedShards)
	require.Same(t, c.MemoryHighWatermark, r.LazyShardUnloadingMemoryHighWatermark)
	require.Same(t, c.MemoryLowWatermark, r.LazyShardUnloadingMemoryLowWatermark)
	require.Same(t, c.MinResidency, r.LazyShardUnloadingMinResidency)
	require.Same(t, c.UnloadRate, r.LazyShardUnloadingUnloadRate)
	require.Same(t, c.Interval, r.LazyShardUnloadingInterval)
	require.Same(t, c.MakeRoomCandidates, r.LazyShardUnloadingMakeRoomCandidates)
	require.Same(t, c.UnloadWorkers, r.LazyShardUnloadingUnloadWorkers)
	require.Same(t, c.NamespaceMaxShare, r.LazyShardUnloadingNamespaceMaxShare)
	require.Same(t, c.Paused, r.LazyShardUnloadingPaused)
	require.True(t, HookKeyMatchesAnyField("LazyShardUnloading"))
}

func TestValidateLazyShardUnloadingFraction(t *testing.T) {
	for _, v := range []float64{0, 0.5, 1} {
		require.NoError(t, ValidateLazyShardUnloadingFraction(v))
	}
	for _, v := range []float64{-0.1, 1.01, math.NaN(), math.Inf(1)} {
		require.Error(t, ValidateLazyShardUnloadingFraction(v))
	}
}
