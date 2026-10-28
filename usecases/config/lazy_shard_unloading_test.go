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

var lazyShardUnloadingEnvs = []string{
	"LAZY_SHARD_UNLOADING_IDLE_TTL", "LAZY_SHARD_UNLOADING_IDLE_CHECK_INTERVAL", "LAZY_SHARD_UNLOADING_MAX_LOADED_SHARDS",
	"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK", "LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK", "LAZY_SHARD_UNLOADING_MIN_RESIDENCY",
	"LAZY_SHARD_UNLOADING_UNLOAD_RATE", "LAZY_SHARD_UNLOADING_INTERVAL", "LAZY_SHARD_UNLOADING_MAKE_ROOM_CANDIDATES",
	"LAZY_SHARD_UNLOADING_UNLOAD_WORKERS", "LAZY_SHARD_UNLOADING_NAMESPACE_MAX_SHARE", "LAZY_SHARD_UNLOADING_PAUSED",
}

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

func TestEnvironmentLazyShardUnloading(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    map[string]any
		wantErr string
	}{
		{name: "defaults", want: map[string]any{}},
		{
			name: "every knob set",
			env: map[string]string{
				"LAZY_SHARD_UNLOADING_IDLE_TTL": "10m", "LAZY_SHARD_UNLOADING_IDLE_CHECK_INTERVAL": "1m",
				"LAZY_SHARD_UNLOADING_MAX_LOADED_SHARDS": "500", "LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "0.9",
				"LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK": "0.7", "LAZY_SHARD_UNLOADING_MIN_RESIDENCY": "1m",
				"LAZY_SHARD_UNLOADING_UNLOAD_RATE": "0", "LAZY_SHARD_UNLOADING_INTERVAL": "1s",
				"LAZY_SHARD_UNLOADING_MAKE_ROOM_CANDIDATES": "1024", "LAZY_SHARD_UNLOADING_UNLOAD_WORKERS": "64",
				"LAZY_SHARD_UNLOADING_NAMESPACE_MAX_SHARE": "0.25", "LAZY_SHARD_UNLOADING_PAUSED": "true",
			},
			want: map[string]any{
				"idle_ttl": 10 * time.Minute, "idle_check_interval": time.Minute, "max_loaded_shards": 500,
				"memory_high_watermark": 0.9, "memory_low_watermark": 0.7, "min_residency": time.Minute,
				"unload_rate": 0, "interval": time.Second, "make_room_candidates": 1024, "unload_workers": 64,
				"namespace_max_share": 0.25, "paused": true,
			},
		},
		{name: "min_residency off", env: map[string]string{"LAZY_SHARD_UNLOADING_MIN_RESIDENCY": "0s"}, want: map[string]any{"min_residency": time.Duration(0)}},
		{name: "high of exactly one", env: map[string]string{"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "1", "LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK": "0.5"}, want: map[string]any{"memory_high_watermark": 1.0, "memory_low_watermark": 0.5}},
		{name: "sub-second idle_ttl", env: map[string]string{"LAZY_SHARD_UNLOADING_IDLE_TTL": "500ms"}, wantErr: "LAZY_SHARD_UNLOADING_IDLE_TTL"},
		{name: "zero interval", env: map[string]string{"LAZY_SHARD_UNLOADING_INTERVAL": "0s"}, wantErr: "LAZY_SHARD_UNLOADING_INTERVAL"},
		{name: "zero idle check interval", env: map[string]string{"LAZY_SHARD_UNLOADING_IDLE_CHECK_INTERVAL": "0s"}, wantErr: "LAZY_SHARD_UNLOADING_IDLE_CHECK_INTERVAL"},
		{name: "unparsable duration", env: map[string]string{"LAZY_SHARD_UNLOADING_MIN_RESIDENCY": "soon"}, wantErr: "LAZY_SHARD_UNLOADING_MIN_RESIDENCY"},
		{name: "negative limit", env: map[string]string{"LAZY_SHARD_UNLOADING_MAX_LOADED_SHARDS": "-1"}, wantErr: "LAZY_SHARD_UNLOADING_MAX_LOADED_SHARDS"},
		{name: "negative rate", env: map[string]string{"LAZY_SHARD_UNLOADING_UNLOAD_RATE": "-1"}, wantErr: "LAZY_SHARD_UNLOADING_UNLOAD_RATE"},
		{name: "too many candidates", env: map[string]string{"LAZY_SHARD_UNLOADING_MAKE_ROOM_CANDIDATES": "1025"}, wantErr: "LAZY_SHARD_UNLOADING_MAKE_ROOM_CANDIDATES"},
		{name: "no workers", env: map[string]string{"LAZY_SHARD_UNLOADING_UNLOAD_WORKERS": "0"}, wantErr: "LAZY_SHARD_UNLOADING_UNLOAD_WORKERS"},
		{name: "too many workers", env: map[string]string{"LAZY_SHARD_UNLOADING_UNLOAD_WORKERS": "65"}, wantErr: "LAZY_SHARD_UNLOADING_UNLOAD_WORKERS"},
		{name: "share above one", env: map[string]string{"LAZY_SHARD_UNLOADING_NAMESPACE_MAX_SHARE": "1.5"}, wantErr: "LAZY_SHARD_UNLOADING_NAMESPACE_MAX_SHARE"},
		{name: "NaN watermark", env: map[string]string{"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "NaN"}, wantErr: "LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK"},
		{name: "watermark above one", env: map[string]string{"LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK": "1.2"}, wantErr: "LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK"},
		{name: "low above high", env: map[string]string{"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "0.5", "LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK": "0.7"}, wantErr: "0 < low < high <= 1"},
		{name: "only high", env: map[string]string{"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK": "0.8"}, wantErr: "0 < low < high <= 1"},
		{name: "idle_ttl below min_residency", env: map[string]string{"LAZY_SHARD_UNLOADING_IDLE_TTL": "10s"}, wantErr: "at least LAZY_SHARD_UNLOADING_MIN_RESIDENCY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, env := range lazyShardUnloadingEnvs {
				t.Setenv(env, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			conf := Config{}
			err := FromEnv(&conf)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			want := defaultLazyShardUnloadingValues()
			for k, v := range tt.want {
				want[k] = v
			}
			require.Equal(t, want, lazyShardUnloadingValues(conf.LazyShardUnloading))
		})
	}
}

func TestLazyShardUnloadingRuntimeOverrides(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)
	for _, env := range lazyShardUnloadingEnvs {
		t.Setenv(env, "")
	}
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
	for _, env := range lazyShardUnloadingEnvs {
		t.Setenv(env, "")
	}
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
