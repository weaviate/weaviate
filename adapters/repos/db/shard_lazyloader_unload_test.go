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

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/storagestate"
)

type fakeShardUnloader struct {
	registry     *LoadedShardRegistry
	beforeLoads  atomic.Int64
	onBeforeLoad atomic.Pointer[func(*LazyLoadShard)]
	onRegistry   atomic.Pointer[func()]
	closes       atomic.Int64
	idle         atomic.Bool
}

func (f *fakeShardUnloader) Collecting() bool { return !f.idle.Load() }

func newFakeShardUnloader() *fakeShardUnloader {
	return &fakeShardUnloader{registry: NewLoadedShardRegistry()}
}

// Registry runs onRegistry once, inside TryUnload between its wrapper checks and tryShutdownIdle.
func (f *fakeShardUnloader) Registry() *LoadedShardRegistry {
	if fn := f.onRegistry.Swap(nil); fn != nil {
		(*fn)()
	}
	return f.registry
}

func (f *fakeShardUnloader) BeforeLoad(l *LazyLoadShard) {
	f.beforeLoads.Add(1)
	if fn := f.onBeforeLoad.Load(); fn != nil {
		(*fn)(l)
	}
}

func (f *fakeShardUnloader) Close(context.Context) error { f.closes.Add(1); return nil }

func withShardUnloader(u ShardUnloader) func(*Config) {
	return func(c *Config) { c.ShardUnloader = u }
}

type unloadFixture struct {
	*addPropertyLazyFixture
	unloader *fakeShardUnloader
	name     string
	lazy     *LazyLoadShard
}

func newUnloadFixture(t *testing.T) *unloadFixture {
	t.Helper()
	u := newFakeShardUnloader()
	f := newAddPropertyLazyFixture(t, "Unload", singleShardState(), withShardUnloader(u))
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

type movementFSM struct {
	replicationTypes.ReplicationFSMReader
	source, target bool
}

func (m movementFSM) HasActiveSourceReplicationForShard(string, string, string) bool { return m.source }

func (m movementFSM) HasActiveTargetReplicationForShard(string, string, string) bool { return m.target }

func (f *unloadFixture) setMovement(source, target bool) func() {
	prev := f.index.getReplicationFSMReader()
	f.index.SetReplicationFSMReader(movementFSM{ReplicationFSMReader: prev, source: source, target: target})
	return func() { f.index.SetReplicationFSMReader(prev) }
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
		{name: "cold shard", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
			return nil
		}},
		{name: "pinned", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			_, release, err := f.index.GetShard(testCtx(), f.name)
			require.NoError(t, err)
			return release
		}},
		{name: "pending shutdown", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			s.shutdownRequested.Store(true)
			return func() { s.shutdownRequested.Store(false) }
		}},
		{name: "drop in progress", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			s.dropRequested.Store(true)
			return func() { s.dropRequested.Store(false) }
		}},
		{name: "index closing", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.index.signalCloseRequested(errIndexShutdown)
			return nil
		}},
		{name: "close lock held for writing", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.index.closeLock.Lock()
			return f.index.closeLock.Unlock
		}},
		{name: "create lock held", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.index.shardCreateLocks.RLock(f.name)
			return func() { f.index.shardCreateLocks.RUnlock(f.name) }
		}},
		{name: "wrapper mutex held", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() { return f.lazy.blockLoading() }},
		{name: "load waiter announced", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.lazy.loadWaiters.Add(1)
			return func() { f.lazy.loadWaiters.Add(-1) }
		}},
		{name: "load blocked", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.lazy.blockLoad(errors.New("blocked"))
			return f.lazy.clearLoadBlock
		}},
		{name: "wrapper replaced in the map", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.index.shards.Store(f.name, &LazyLoadShard{shardOpts: f.lazy.shardOpts})
			return func() { f.index.shards.Store(f.name, f.lazy) }
		}},
		{name: "recovering placeholder in the map", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
			f.index.shards.Store(f.name, &RecoveringShard{LazyLoadShard: f.lazy})
			return func() { f.index.shards.Store(f.name, f.lazy) }
		}},
		{name: "not idle", req: UnloadRequest{IdleTTL: time.Hour}},
		{name: "movement source", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() { return f.setMovement(true, false) }},
		{name: "movement target", arm: func(t *testing.T, f *unloadFixture, s *Shard) func() { return f.setMovement(false, true) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			s := f.lazy.currentShard()
			before := time.Now()

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
			require.False(t, f.lazy.UnloadedAt().Before(before))
			require.True(t, f.exists(t, id), "data survives the unload and the reload")
			require.NotSame(t, s, f.lazy.currentShard())
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
			require.True(t, f.lazy.UnloadedAt().IsZero(), "a reload clears it")
		})
	}
}

func TestLazyLoadShard_TryUnloadSeesAStampMadeBeforeThePinReleased(t *testing.T) {
	tests := []struct {
		name         string
		ctx          context.Context
		wantUnloaded bool
	}{
		{name: "request stamps and releases", ctx: testCtx()},
		{name: "background work releases without a stamp", ctx: withBackgroundShardAccess(testCtx()), wantUnloaded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			s := f.lazy.currentShard()
			require.Eventually(t, func() bool { return time.Since(accessClockEpoch) >= 3*time.Second },
				5*time.Second, 10*time.Millisecond)
			advanceAccessClock()

			release, err := s.preventShutdown()
			require.NoError(t, err)
			f.lazy.lastAccess.Store(0)
			released := false
			hook := func() {
				f.index.recordShardAccess(tt.ctx, f.lazy)
				release()
				released = true
			}
			f.unloader.onRegistry.Store(&hook)

			require.Equal(t, tt.wantUnloaded, f.lazy.TryUnload(testCtx(), UnloadRequest{IdleTTL: time.Second}))
			require.True(t, released)
			require.Equal(t, !tt.wantUnloaded, f.lazy.isLoaded())
			require.True(t, f.exists(t, id))
		})
	}
}

func TestLazyLoadShard_TryUnloadRefusesWhileAReindexIsLive(t *testing.T) {
	tests := []struct {
		name   string
		lookup func(collection, shard string) ShardReindexActivityLookupBuilder
	}{
		{name: "live reindex on the shard", lookup: func(collection, shard string) ShardReindexActivityLookupBuilder {
			return makeActivityBuilder(map[[2]string]bool{{collection, shard}: true})
		}},
		{name: "task list unavailable", lookup: func(string, string) ShardReindexActivityLookupBuilder { return unreachableActivityBuilder }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			s := f.lazy.currentShard()
			f.index.db.SetShardReindexActivityLookup(tt.lookup(f.index.Config.ClassName.String(), f.name))

			require.False(t, tryUnloadPromptly(t, f.lazy, UnloadRequest{}))
			require.Same(t, s, f.lazy.currentShard())
			require.False(t, s.shut.Load())
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

func TestTryUnload_ReadOnlyStatusAcrossReload(t *testing.T) {
	manualReadOnly := setManualStatus(storagestate.StatusReadOnly)
	resourcePressure := func(t *testing.T, f *unloadFixture) { f.index.db.setShardsReadOnly("test") }
	resourcesRecover := func(t *testing.T, f *unloadFixture) { f.index.db.setShardsReady() }
	tests := []struct {
		name          string
		arm           func(t *testing.T, f *unloadFixture)
		whileUnloaded func(t *testing.T, f *unloadFixture)
		want          storagestate.Status
	}{
		{name: "manual READONLY survives repeated idle unloads", arm: manualReadOnly, want: storagestate.StatusReadOnly},
		{name: "manual READY on an unloaded READONLY shard lifts it", arm: manualReadOnly, whileUnloaded: setManualStatus(storagestate.StatusReady), want: storagestate.StatusReady},
		{name: "manual READONLY outlives a resource recovery while unloaded", arm: manualReadOnly, whileUnloaded: resourcesRecover, want: storagestate.StatusReadOnly},
		{name: "resource pressure READONLY is re-read at load", arm: resourcePressure, want: storagestate.StatusReadOnly},
		{name: "resource pressure READONLY lifted while unloaded loads READY", arm: resourcePressure, whileUnloaded: resourcesRecover, want: storagestate.StatusReady},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			tt.arm(t, f)
			for range 2 {
				require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
				f.requireUnloaded(t)
				if tt.whileUnloaded != nil {
					tt.whileUnloaded(t, f)
				}
				shard, release, err := f.index.GetShard(testCtx(), f.name)
				require.NoError(t, err)
				require.Equal(t, tt.want, shard.GetStatus())
				err = shard.PutObject(testCtx(), testObject(f.index.Config.ClassName.String()))
				release()
				if tt.want == storagestate.StatusReadOnly {
					require.ErrorContains(t, err, storagestate.ErrStatusReadOnly.Error())
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func setManualStatus(status storagestate.Status) func(t *testing.T, f *unloadFixture) {
	return func(t *testing.T, f *unloadFixture) {
		require.NoError(t, f.index.withShardForWrite(testCtx(), f.name, func(w shardWriter) error {
			return w.UpdateStatus(status.String(), statusReasonManualUpdate)
		}))
	}
}
