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

package rest

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/cron"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/wl/lazyshardunloading"
)

// A runtime-overrides push reaches a licensed node's unloader through the
// hooks map the server registers, without waiting for the unloader's tick.
func TestRuntimeOverridesReachTheLazyShardUnloaderThroughTheHooks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	cfg := config.Config{}
	require.NoError(t, config.FromEnv(&cfg))
	require.NoError(t, cfg.LazyShardUnloading.Interval.SetValue(time.Hour))
	reg := prometheus.NewPedanticRegistry()
	u := lazyshardunloading.New(lazyshardunloading.Config{
		Logger: logger, Registerer: reg, Knobs: lazyshardunloading.KnobsFrom(cfg.LazyShardUnloading),
	})

	nodes := cluster.NewMockNodeSelector(t)
	nodes.EXPECT().LocalName().Return("node1").Maybe()
	metrics := *monitoring.GetMetrics()
	metrics.Registerer = monitoring.NoopRegisterer
	repo, err := db.New(logger, "node1", db.Config{RootPath: t.TempDir(), MaxImportGoroutinesFactor: 1, ShardUnloader: u},
		&db.FakeRemoteClient{}, nodes, &db.FakeRemoteNodeClient{}, &db.FakeReplicationClient{}, &metrics,
		memwatch.NewDummyMonitor(), nodes, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	crons, err := cron.NewCrons(t.Context(), logger, func() config.Config { return config.Config{} })
	require.NoError(t, err)
	appState := &state.State{Logger: logger, Crons: crons, DB: repo, ServerConfig: &config.WeaviateConfig{Config: cfg}}
	hooks, err := runtimeConfigHooks(appState, t.Context())
	require.NoError(t, err)
	require.Contains(t, hooks, lazyShardUnloadingHookKey)

	limit := func() float64 {
		return testutil.ToFloat64(gaugeNamed(t, reg, "weaviate_lazy_shard_unloading_limit"))
	}
	parsed, err := config.ParseRuntimeConfig([]byte("lazy_shard_unloading_max_loaded_shards: 7\n"))
	require.NoError(t, err)
	require.NoError(t, config.UpdateRuntimeConfig(logger, config.BuildRegisteredRuntimeConfig(&cfg), parsed, nil, hooks))
	require.Eventually(t, func() bool { return limit() == 7 }, 5*time.Second, 10*time.Millisecond,
		"the hook applies the override without waiting the hour-long tick")
}

func gaugeNamed(t *testing.T, reg *prometheus.Registry, name string) prometheus.Collector {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name})
			g.Set(f.GetMetric()[0].GetGauge().GetValue())
			return g
		}
	}
	require.FailNow(t, "no such metric", name)
	return nil
}
