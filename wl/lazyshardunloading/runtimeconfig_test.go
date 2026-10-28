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

package lazyshardunloading

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
	require.Equal(t, config.DefaultLazyShardUnloadingIdleCheckInterval, DefaultIdleCheckInterval)
	require.Equal(t, config.DefaultLazyShardUnloadingMinResidency, DefaultMinResidency)
	require.Equal(t, config.DefaultLazyShardUnloadingUnloadRate, DefaultUnloadRate)
	require.Equal(t, config.DefaultLazyShardUnloadingInterval, DefaultInterval)
	require.Equal(t, config.DefaultLazyShardUnloadingMakeRoomCandidates, DefaultMakeRoomCandidates)
	require.Equal(t, config.DefaultLazyShardUnloadingUnloadWorkers, DefaultUnloadWorkers)
	require.Equal(t, config.MaxLazyShardUnloadingMakeRoomCandidates, MaxMakeRoomCandidates)
	require.Equal(t, config.MaxLazyShardUnloadingUnloadWorkers, MaxUnloadWorkers)
}

func TestRuntimeConfigUpdatesReachThePolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := logrus.New()
		log.SetOutput(io.Discard)
		conf := config.Config{}
		require.NoError(t, config.FromEnv(&conf))
		registered := config.BuildRegisteredRuntimeConfig(&conf)
		h := newHarnessWithKnobs(t, KnobsFrom(conf.LazyShardUnloading))
		require.Equal(t, &snapshot{
			idleCheckInterval: DefaultIdleCheckInterval, minResidency: DefaultMinResidency, interval: DefaultInterval,
			rate: DefaultUnloadRate, candidates: DefaultMakeRoomCandidates, workers: DefaultUnloadWorkers,
		}, h.p.snap.Load())

		parsed, err := config.ParseRuntimeConfig([]byte(`lazy_shard_unloading_max_loaded_shards: 2
lazy_shard_unloading_min_residency: 0s
lazy_shard_unloading_unload_workers: 3
lazy_shard_unloading_interval: 1s
`))
		require.NoError(t, err)
		shards := h.loadN(5, "")
		require.NoError(t, config.UpdateRuntimeConfig(log, registered, parsed, nil, nil))
		h.p.refreshConfig()
		synctest.Wait()
		require.EqualValues(t, 2, h.p.snap.Load().maxLoaded)
		require.Equal(t, 2, h.loadedCount(shards))
		require.EqualValues(t, 3, h.p.workersRunning.Load())

		parsed, err = config.ParseRuntimeConfig([]byte(`lazy_shard_unloading_paused: true
lazy_shard_unloading_interval: 1s
`))
		require.NoError(t, err)
		require.NoError(t, config.UpdateRuntimeConfig(log, registered, parsed, nil, nil))
		time.Sleep(DefaultInterval)
		synctest.Wait()
		require.True(t, h.p.snap.Load().paused, "the loop picks a change up on its own tick")
		require.Zero(t, h.p.snap.Load().maxLoaded)
	})
}
