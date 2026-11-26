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

// An env var, valid or not, neither seeds a knob nor fails startup on an unlicensed node.
func TestIdleShardCollectorIgnoresEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
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
	for _, step := range []struct {
		name string
		yaml string
		want map[string]any
	}{
		{
			name: "every knob set",
			yaml: "idle_shard_collector_idle_ttl: 1h\nidle_shard_collector_check_interval: 2m\nidle_shard_collector_unload_rate: 7\n" +
				"idle_shard_collector_unload_workers: 2\nidle_shard_collector_paused: true\n",
			want: map[string]any{
				"idle_ttl": time.Hour, "check_interval": 2 * time.Minute, "unload_rate": 7, "unload_workers": 2, "paused": true,
			},
		},
		{
			name: "invalid values keep the previous one and a removed key reverts to its default",
			yaml: "idle_shard_collector_unload_workers: 0\nidle_shard_collector_check_interval: 10ms\n" +
				"idle_shard_collector_unload_rate: 5\nidle_shard_collector_idle_ttl: 500ms\n",
			want: map[string]any{
				"idle_ttl": time.Hour, "check_interval": 2 * time.Minute, "unload_rate": 5, "unload_workers": 2, "paused": false,
			},
		},
		{name: "0 turns idle unloading off", yaml: "idle_shard_collector_idle_ttl: 0s\n", want: defaultIdleShardCollectorValues()},
		{name: "an empty file restores the defaults", want: defaultIdleShardCollectorValues()},
	} {
		parsed, err := ParseRuntimeConfig([]byte(step.yaml))
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, registered, parsed, nil, nil))
		require.Equal(t, step.want, idleShardCollectorValues(conf.IdleShardCollector), step.name)
	}
}
