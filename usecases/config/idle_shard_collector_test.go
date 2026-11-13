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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func idleShardCollectorValues(c IdleShardCollector) map[string]any {
	return map[string]any{
		"idle_ttl": c.IdleTTL.Get(), "check_interval": c.CheckInterval.Get(), "unload_rate": c.UnloadRate.Get(),
		"unload_workers": c.UnloadWorkers.Get(), "paused": c.Paused.Get(),
	}
}

func defaultIdleShardCollectorValues() map[string]any {
	return map[string]any{
		"idle_ttl": time.Duration(0), "check_interval": 30 * time.Second, "unload_rate": 300, "unload_workers": 4,
		"paused": false,
	}
}

// The knobs come only from the runtime overrides file: an env var, valid or
// not, neither seeds one nor fails startup on an unlicensed node.
func TestIdleShardCollectorIgnoresEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "nothing set"},
		{name: "valid values", env: map[string]string{
			"IDLE_SHARD_COLLECTOR_IDLE_TTL": "10m", "IDLE_SHARD_COLLECTOR_CHECK_INTERVAL": "1m",
			"IDLE_SHARD_COLLECTOR_PAUSED": "true",
		}},
		{name: "invalid values", env: map[string]string{
			"IDLE_SHARD_COLLECTOR_CHECK_INTERVAL": "0s", "IDLE_SHARD_COLLECTOR_UNLOAD_WORKERS": "0",
			"IDLE_SHARD_COLLECTOR_IDLE_TTL": "10ms",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			conf := Config{}
			require.NoError(t, FromEnv(&conf))
			require.Equal(t, defaultIdleShardCollectorValues(), idleShardCollectorValues(conf.IdleShardCollector))
		})
	}
}

func TestIdleShardCollectorRuntimeOverrides(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)
	conf := Config{}
	require.NoError(t, FromEnv(&conf))
	registered := BuildRegisteredRuntimeConfig(&conf)

	parsed, err := ParseRuntimeConfig([]byte(`idle_shard_collector_idle_ttl: 1h
idle_shard_collector_check_interval: 2m
idle_shard_collector_unload_rate: 7
idle_shard_collector_unload_workers: 2
idle_shard_collector_paused: true
`))
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	require.Equal(t, map[string]any{
		"idle_ttl": time.Hour, "check_interval": 2 * time.Minute, "unload_rate": 7, "unload_workers": 2,
		"paused": true,
	}, idleShardCollectorValues(conf.IdleShardCollector))

	parsed, err = ParseRuntimeConfig([]byte(`idle_shard_collector_unload_workers: 0
idle_shard_collector_check_interval: 10ms
idle_shard_collector_unload_rate: 5
idle_shard_collector_idle_ttl: 500ms
`))
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	got := idleShardCollectorValues(conf.IdleShardCollector)
	require.Equal(t, 2, got["unload_workers"], "an invalid value keeps the previous one")
	require.Equal(t, 2*time.Minute, got["check_interval"])
	require.Equal(t, 5, got["unload_rate"])
	require.Equal(t, time.Hour, got["idle_ttl"], "below 1s and not 0 is invalid")
	require.Equal(t, false, got["paused"], "a removed key reverts to its default")

	parsed, err = ParseRuntimeConfig([]byte("idle_shard_collector_idle_ttl: 0s\n"))
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	require.Equal(t, time.Duration(0), idleShardCollectorValues(conf.IdleShardCollector)["idle_ttl"], "0 turns idle unloading off")

	parsed, err = ParseRuntimeConfig(nil)
	require.NoError(t, err)
	require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
	require.Equal(t, defaultIdleShardCollectorValues(), idleShardCollectorValues(conf.IdleShardCollector))
}

func TestBuildRegisteredRuntimeConfigSharesIdleShardCollector(t *testing.T) {
	conf := Config{}
	require.NoError(t, FromEnv(&conf))
	r := BuildRegisteredRuntimeConfig(&conf)
	c := conf.IdleShardCollector
	require.Same(t, c.IdleTTL, r.IdleShardCollectorIdleTTL)
	require.Same(t, c.CheckInterval, r.IdleShardCollectorCheckInterval)
	require.Same(t, c.UnloadRate, r.IdleShardCollectorUnloadRate)
	require.Same(t, c.UnloadWorkers, r.IdleShardCollectorUnloadWorkers)
	require.Same(t, c.Paused, r.IdleShardCollectorPaused)
	require.True(t, HookKeyMatchesAnyField("IdleShardCollector"))
}
