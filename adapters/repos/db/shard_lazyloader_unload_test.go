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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
)

type fakeShardUnloader struct {
	registry     *LoadedShardRegistry
	beforeLoads  atomic.Int64
	onBeforeLoad atomic.Pointer[func(*LazyLoadShard)]
	closes       atomic.Int64
}

func newFakeShardUnloader() *fakeShardUnloader {
	return &fakeShardUnloader{registry: NewLoadedShardRegistry()}
}

func (f *fakeShardUnloader) Registry() *LoadedShardRegistry { return f.registry }

func (f *fakeShardUnloader) BeforeLoad(l *LazyLoadShard) {
	f.beforeLoads.Add(1)
	if fn := f.onBeforeLoad.Load(); fn != nil {
		(*fn)(l)
	}
}

func (f *fakeShardUnloader) Close(context.Context) error {
	f.closes.Add(1)
	return nil
}

func withShardUnloader(u ShardUnloader) func(*Config) {
	return func(c *Config) { c.ShardUnloader = u }
}

type unloadFixture struct {
	*addPropertyLazyFixture
	unloader *fakeShardUnloader
	name     string
	lazy     *LazyLoadShard
}

func newUnloadFixture(t *testing.T, opts ...func(*Config)) *unloadFixture {
	t.Helper()
	u := newFakeShardUnloader()
	f := newAddPropertyLazyFixture(t, "Unload", singleShardState(), append([]func(*Config){withShardUnloader(u)}, opts...)...)
	uf := &unloadFixture{addPropertyLazyFixture: f, unloader: u}
	for name, lazy := range f.coldShards(t) {
		uf.name, uf.lazy = name, lazy
	}
	return uf
}

func (f *unloadFixture) put(t *testing.T) strfmt.UUID {
	t.Helper()
	obj := testObject(f.index.Config.ClassName.String())
	shard, release, err := f.index.GetShard(testCtx(), f.name)
	require.NoError(t, err)
	defer release()
	require.NoError(t, shard.PutObject(testCtx(), obj))
	return obj.ID()
}

func (f *unloadFixture) exists(t *testing.T, id strfmt.UUID) bool {
	t.Helper()
	shard, release, err := f.index.GetShard(testCtx(), f.name)
	require.NoError(t, err)
	defer release()
	ok, err := shard.Exists(testCtx(), id)
	require.NoError(t, err)
	return ok
}

func (f *unloadFixture) requireUnloaded(t *testing.T) {
	t.Helper()
	require.False(t, f.lazy.isLoaded())
	require.Same(t, f.lazy, f.index.shards.Load(f.name), "the wrapper stays in the shard map")
	require.Zero(t, f.unloader.registry.Loaded())
}

type failingShutdownVectorIndex struct{ VectorIndex }

func (f failingShutdownVectorIndex) Shutdown(ctx context.Context) error {
	return errors.Join(f.VectorIndex.Shutdown(ctx), errors.New("injected vector index shutdown failure"))
}

func replaceLegacyVectorIndex(t *testing.T, s *Shard, wrap func(VectorIndex) VectorIndex) func() {
	t.Helper()
	slot, ok := s.vectors.get("")
	require.True(t, ok)
	orig := slot.index
	require.True(t, s.vectors.Replace("", wrap(orig)))
	return func() { s.vectors.Replace("", orig) }
}

func TestLazyLoadShard_TryUnloadClauses(t *testing.T) {
	tests := []struct {
		name string
		req  UnloadRequest
		arm  func(t *testing.T, f *unloadFixture, s *Shard) func()
		want bool
	}{
		{name: "idle shard unloads", want: true},
		{
			name: "cold shard",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
				return nil
			},
		},
		{
			name: "pinned",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				_, release, err := f.index.GetShard(testCtx(), f.name)
				require.NoError(t, err)
				return release
			},
		},
		{
			name: "pending shutdown",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.shutdownRequested.Store(true)
				return func() { s.shutdownRequested.Store(false) }
			},
		},
		{
			name: "drop in progress",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.dropRequested.Store(true)
				return func() { s.dropRequested.Store(false) }
			},
		},
		{
			name: "index closing",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.signalCloseRequested(errIndexShutdown)
				return nil
			},
		},
		{
			name: "create lock held",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.shardCreateLocks.RLock(f.name)
				return func() { f.index.shardCreateLocks.RUnlock(f.name) }
			},
		},
		{
			name: "wrapper mutex held",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				return f.lazy.blockLoading()
			},
		},
		{
			name: "load waiter announced",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.lazy.loadWaiters.Add(1)
				return func() { f.lazy.loadWaiters.Add(-1) }
			},
		},
		{
			name: "load blocked",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.lazy.blockLoad(errors.New("blocked"))
				return f.lazy.clearLoadBlock
			},
		},
		{
			name: "wrapper replaced in the map",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				other := &LazyLoadShard{shardOpts: f.lazy.shardOpts}
				f.index.shards.Store(f.name, other)
				return func() { f.index.shards.Store(f.name, f.lazy) }
			},
		},
		{
			name: "wrapper out of the map",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.shards.LoadAndDelete(f.name)
				return func() { f.index.shards.Store(f.name, f.lazy) }
			},
		},
		{
			name: "recovering placeholder in the map",
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.shards.Store(f.name, &RecoveringShard{LazyLoadShard: f.lazy})
				return func() { f.index.shards.Store(f.name, f.lazy) }
			},
		},
		{name: "not idle", req: UnloadRequest{IdleTTL: time.Hour}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			s := f.lazy.currentShard()
			require.NotNil(t, s)
			require.EqualValues(t, 1, f.unloader.registry.Loaded())

			undo := func() {}
			if tt.arm != nil {
				if u := tt.arm(t, f, s); u != nil {
					undo = sync.OnceFunc(u)
				}
			}
			t.Cleanup(undo)
			cold := f.lazy.currentShard() == nil
			require.True(t, f.lazy.TryMarkUnloadPending())
			require.Equal(t, tt.want, tryUnloadPromptly(t, f.lazy, tt.req))
			require.False(t, f.lazy.UnloadPending(), "TryUnload clears the handoff claim")
			undo()

			switch {
			case tt.want || cold:
				f.requireUnloaded(t)
				require.True(t, s.shut.Load())
				require.False(t, s.shutdownRequested.Load(), "an unload never requests a shutdown")
				require.NoError(t, s.teardownError())
			case f.index.closeRequestedCause() != nil:
				require.Same(t, s, f.lazy.currentShard())
				return
			default:
				require.Same(t, s, f.lazy.currentShard(), "a refused unload leaves the shard loaded")
				require.False(t, s.shut.Load())
				require.EqualValues(t, 1, f.unloader.registry.Loaded())
				require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}), "the refusal lifts with its cause")
				f.requireUnloaded(t)
			}
			require.True(t, f.exists(t, id), "data survives the unload and the reload")
			require.NotSame(t, s, f.lazy.currentShard())
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
		})
	}
}

func tryUnloadPromptly(t *testing.T, l *LazyLoadShard, req UnloadRequest) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- l.TryUnload(testCtx(), req) }()
	select {
	case got := <-done:
		return got
	case <-time.After(10 * time.Second):
		require.FailNow(t, "TryUnload blocked instead of refusing")
		return false
	}
}

func TestLazyLoadShard_TryUnloadTornTeardown(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	s := f.lazy.currentShard()
	replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return failingShutdownVectorIndex{v} })

	require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
	require.Same(t, s, f.lazy.currentShard(), "the torn shard stays published as the last reference to its handles")
	require.Error(t, s.teardownError())
	require.Zero(t, f.unloader.registry.Loaded(), "a torn shard no longer counts as loaded")
	require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))

	_, _, err := f.index.getOrInitShard(testCtx(), f.name)
	require.ErrorContains(t, err, "injected vector index shutdown failure")
}
