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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/cron"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/wl/idleshardcollector"
)

func TestIdleShardCollectorModeFor(t *testing.T) {
	on, off := true, false
	cases := []struct {
		name     string
		lazy     *bool
		licensed bool
		want     license.Mode
	}{
		{name: "lazy auto with a license", licensed: true, want: license.FeatureLicensed},
		{name: "lazy on without a license", lazy: &on, want: license.FeatureUnlicensed},
		{name: "lazy off with a license", lazy: &off, licensed: true, want: license.FeatureOff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.EnableLazyLoadShards = tc.lazy
			cfg.WeaviateLicense = tc.licensed
			require.Equal(t, tc.want, idleShardCollectorModeFor(cfg))
		})
	}
}

func TestIdleShardCollectorFor(t *testing.T) {
	cases := []struct {
		name       string
		mode       license.Mode
		wantBuilds int
	}{
		{name: "off", mode: license.FeatureOff},
		{name: "unlicensed", mode: license.FeatureUnlicensed},
		{name: "a mode outside the three", mode: license.Mode(99)},
		{name: "licensed", mode: license.FeatureLicensed, wantBuilds: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			builds := 0
			u := idleShardCollectorFor(tc.mode, func() db.ShardUnloader {
				builds++
				return idleshardcollector.New(idleshardcollector.Config{Logger: logger})
			})
			if u != nil {
				t.Cleanup(func() { require.NoError(t, u.Close(context.Background())) })
			}

			licensed := tc.wantBuilds == 1
			require.Equal(t, tc.wantBuilds, builds)
			require.Equal(t, licensed, u != nil)
			require.Equal(t, licensed, license.DeclaredInWL(u))
			require.Equal(t, licensed, idleShardCollectorHooks(u) != nil)
			require.Empty(t, hook.AllEntries(), "no mode logs about the license")
		})
	}
}

func TestRuntimeOverridesReachTheIdleShardCollectorThroughTheHooks(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	cfg := config.Config{}
	require.NoError(t, config.FromEnv(&cfg))
	require.NoError(t, cfg.IdleShardCollector.CheckInterval.SetValue(time.Hour))
	u := idleshardcollector.New(idleshardcollector.Config{Logger: logger, Knobs: idleshardcollector.KnobsFrom(cfg.IdleShardCollector)})

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
	require.Contains(t, hooks, idleShardCollectorHookKey)

	require.False(t, u.Collecting())
	parsed, err := config.ParseRuntimeConfig([]byte("idle_shard_collector_idle_ttl: 10s\n"))
	require.NoError(t, err)
	require.NoError(t, config.UpdateRuntimeConfig(logger, config.BuildRegisteredRuntimeConfig(&cfg), parsed, nil, hooks))
	require.True(t, u.Collecting(), "the hook applies the override without waiting the hour-long tick")
}
