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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/wl/lazyshardunloading"
)

func TestLazyShardUnloadingModeFor(t *testing.T) {
	on, off := true, false
	cases := []struct {
		name     string
		lazy     *bool
		licensed bool
		want     license.Mode
	}{
		{name: "lazy auto without a license", want: license.FeatureUnlicensed},
		{name: "lazy auto with a license", licensed: true, want: license.FeatureLicensed},
		{name: "lazy on without a license", lazy: &on, want: license.FeatureUnlicensed},
		{name: "lazy on with a license", lazy: &on, licensed: true, want: license.FeatureLicensed},
		{name: "lazy off without a license", lazy: &off, want: license.FeatureOff},
		{name: "lazy off with a license", lazy: &off, licensed: true, want: license.FeatureOff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.EnableLazyLoadShards = tc.lazy
			cfg.WeaviateLicense = tc.licensed

			require.Equal(t, tc.want, lazyShardUnloadingModeFor(cfg))
		})
	}
}

func TestLazyShardUnloaderFor(t *testing.T) {
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
			u := lazyShardUnloaderFor(tc.mode, func() db.ShardUnloader {
				builds++
				return lazyshardunloading.New(lazyshardunloading.Config{Logger: logger})
			})
			if u != nil {
				t.Cleanup(func() { require.NoError(t, u.Close(context.Background())) })
			}

			licensed := tc.wantBuilds == 1
			require.Equal(t, tc.wantBuilds, builds)
			require.Equal(t, licensed, u != nil)
			require.Equal(t, licensed, license.DeclaredInWL(u))
			require.Equal(t, licensed, lazyShardUnloadingHooks(u) != nil)
			require.Empty(t, hook.AllEntries(), "no mode logs about the license")
		})
	}
}

type countingRefresher struct {
	db.ShardUnloader
	refreshes int
}

func (c *countingRefresher) RefreshConfig() { c.refreshes++ }

func TestLazyShardUnloadingHooks(t *testing.T) {
	require.Nil(t, lazyShardUnloadingHooks(nil))
	r := &countingRefresher{}
	hooks := lazyShardUnloadingHooks(r)
	require.Len(t, hooks, 1)
	require.True(t, config.HookKeyMatchesAnyField(lazyShardUnloadingHookKey))
	require.NoError(t, mergeRuntimeHooks(map[string]func() error{}, hooks))
	require.NoError(t, hooks[lazyShardUnloadingHookKey]())
	require.Equal(t, 1, r.refreshes)
}

type fakeExister map[string]api.NamespaceState

func (f fakeExister) GetNamespace(name string) (api.Namespace, bool) {
	state, ok := f[name]
	return api.Namespace{Name: name, State: state}, ok
}

func TestNamespaceActiveFor(t *testing.T) {
	e := fakeExister{"live": api.NamespaceStateActive, "parked": api.NamespaceStateSuspended}
	require.Nil(t, namespaceActiveFor(false, e))
	require.Nil(t, namespaceActiveFor(true, nil))
	active := namespaceActiveFor(true, e)
	require.True(t, active("live"))
	require.False(t, active("parked"))
	require.False(t, active("gone"))
}
