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

package db

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

const injectedTeardownPanic = "injected vector index shutdown panic"

type panickingShutdownVectorIndex struct{ VectorIndex }

func (panickingShutdownVectorIndex) Shutdown(context.Context) error {
	panic(injectedTeardownPanic)
}

func requirePanicsWithInjected(t *testing.T, f func()) {
	t.Helper()
	require.PanicsWithValue(t, injectedTeardownPanic, f)
}

func TestLazyLoadShard_TeardownPanicLeavesTheShardTorn(t *testing.T) {
	tests := []struct {
		name     string
		teardown func(t *testing.T, f *unloadFixture)
	}{
		{
			name: "TryUnload answers torn",
			teardown: func(t *testing.T, f *unloadFixture) {
				require.Equal(t, UnloadOutcomeTorn, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
			},
		},
		{
			name: "Shutdown still panics",
			teardown: func(t *testing.T, f *unloadFixture) {
				requirePanicsWithInjected(t, func() { require.NoError(t, f.lazy.Shutdown(testCtx())) })
			},
		},
		{
			name: "the release completing a deferred shutdown still panics",
			teardown: func(t *testing.T, f *unloadFixture) {
				_, release, err := f.index.GetShard(testCtx(), f.name)
				require.NoError(t, err)
				cancelled, cancel := context.WithCancel(testCtx())
				cancel()
				require.Error(t, f.lazy.Shutdown(cancelled))
				requirePanicsWithInjected(t, release)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			s := f.lazy.currentShard()
			replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return panickingShutdownVectorIndex{v} })

			tt.teardown(t, f)

			require.Same(t, s, f.lazy.currentShard(), "the torn shard stays published as the last reference to its handles")
			require.ErrorContains(t, s.teardownError(), injectedTeardownPanic)
			require.Zero(t, f.unloader.registry.Loaded(), "a torn shard no longer counts as loaded")
			require.False(t, shardKnownShut(f.lazy), "a reactivation may not evict it and reopen the directory")

			require.Equal(t, UnloadOutcomeTorn, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
			require.ErrorIs(t, f.lazy.Shutdown(testCtx()), errTeardownFailed)
			_, _, err := f.index.getOrInitShard(testCtx(), f.name)
			require.ErrorContains(t, err, injectedTeardownPanic)
			require.Same(t, s, f.lazy.currentShard(), "no attempt built a second instance over the directory")
		})
	}
}

func TestShutdownOrRestoreShard_TeardownPanicKeepsTheEntry(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	s := f.lazy.currentShard()
	replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return panickingShutdownVectorIndex{v} })
	removed, ok := f.index.shards.LoadAndDelete(f.name)
	require.True(t, ok)

	requirePanicsWithInjected(t, func() {
		_, err := shutdownOrRestoreShard(testCtx(), f.index, f.name, removed)
		require.NoError(t, err)
	})

	require.Same(t, f.lazy, f.index.shards.Load(f.name), "the entry is the last reference to the torn shard's handles")
	_, _, err := f.index.getOrInitShard(testCtx(), f.name)
	require.ErrorContains(t, err, injectedTeardownPanic)
	require.Same(t, s, f.lazy.currentShard())
}

func TestShard_TeardownPanicStaysStickyAfterAnOuterRecover(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := &Shard{
		index:        &Index{logger: logger},
		shutdownLock: new(sync.RWMutex),
		shutCtxCancel: func(error) {
			panic(injectedTeardownPanic)
		},
	}

	requirePanicsWithInjected(t, func() { require.NoError(t, s.performShutdown(testCtx())) })

	err := s.performShutdown(testCtx())
	require.ErrorIs(t, err, errTeardownFailed)
	require.ErrorContains(t, err, injectedTeardownPanic)
	require.False(t, shardKnownShut(s))
	require.True(t, errors.Is(shardTeardownError(s), s.teardownErr))
}

func TestTeardownPanicDoesNotStrandTheUnloadingGauge(t *testing.T) {
	h := newShardMetricsHarness(t)
	name := h.addClass(t, "TeardownPanicGauge")
	index := h.repo.GetIndex(schema.ClassName("TeardownPanicGauge"))
	shard, release, err := index.GetShard(testCtx(), name)
	require.NoError(t, err)
	lazy, ok := shard.(*LazyLoadShard)
	require.True(t, ok)
	release()
	replaceLegacyVectorIndex(t, lazy.currentShard(), func(v VectorIndex) VectorIndex { return panickingShutdownVectorIndex{v} })
	before := h.gauges()

	requirePanicsWithInjected(t, func() { require.NoError(t, shard.Shutdown(testCtx())) })

	after := h.gauges()
	require.Zero(t, after.unloading)
	require.Equal(t, before.loaded-1, after.loaded)
	require.Equal(t, before.unloaded+1, after.unloaded)
}
