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

package idleshardcollector

import (
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
)

func TestDefaultsMatchTheRuntimeConfig(t *testing.T) {
	require.Equal(t, config.DefaultIdleShardCollectorCheckInterval, DefaultCheckInterval)
	require.Equal(t, config.DefaultIdleShardCollectorUnloadRate, DefaultUnloadRate)
	require.Equal(t, config.DefaultIdleShardCollectorUnloadWorkers, DefaultUnloadWorkers)
	require.Equal(t, config.MaxIdleShardCollectorUnloadWorkers, MaxUnloadWorkers)
}

func TestRuntimeConfigUpdatesReachThePolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := logrus.New()
		log.SetOutput(io.Discard)
		conf := config.Config{}
		require.NoError(t, config.FromEnv(&conf))
		registered := config.BuildRegisteredRuntimeConfig(&conf)
		h := newHarnessWithKnobs(t, KnobsFrom(conf.IdleShardCollector))
		require.Equal(t, &snapshot{
			checkInterval: DefaultCheckInterval, rate: DefaultUnloadRate, workers: DefaultUnloadWorkers,
		}, h.p.snap.Load())

		parsed, err := config.ParseRuntimeConfig([]byte(`idle_shard_collector_idle_ttl: 2s
idle_shard_collector_check_interval: 1s
idle_shard_collector_unload_workers: 3
`))
		require.NoError(t, err)
		shards := h.loadN(5, "")
		require.NoError(t, config.UpdateRuntimeConfig(log, registered, parsed, nil, nil))
		h.p.refreshConfig()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Equal(t, 2*time.Second, h.p.snap.Load().idleTTL)
		require.Equal(t, time.Second, h.p.snap.Load().checkInterval)
		require.Zero(t, h.loadedCount(shards))
		require.EqualValues(t, 3, h.p.workersRunning.Load())

		parsed, err = config.ParseRuntimeConfig([]byte(`idle_shard_collector_paused: true
idle_shard_collector_check_interval: 1s
`))
		require.NoError(t, err)
		require.NoError(t, config.UpdateRuntimeConfig(log, registered, parsed, nil, nil))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.True(t, h.p.snap.Load().paused, "the loop picks a change up on its own tick")
		require.Zero(t, h.p.snap.Load().idleTTL)
	})
}
