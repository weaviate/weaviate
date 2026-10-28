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

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type fakeShardUnloader struct {
	registry    *LoadedShardRegistry
	beforeLoads atomic.Int64
	denyWarmup  atomic.Bool
	warmupLimit atomic.Int64
	// warmupReserved mirrors the policy's atomic reservation.
	warmupReserved atomic.Int64
	onBeforeLoad   atomic.Pointer[func(*LazyLoadShard)]
	closes         atomic.Int64
}

// noReindex is a usable snapshot in which no reindex runs.
var noReindex = ReindexActivity{taken: true}

func (f *fakeShardUnloader) SetReindexActivitySource(func(context.Context) (ReindexActivity, error)) {
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

func (f *fakeShardUnloader) ReserveWarmupLoad() bool {
	if f.denyWarmup.Load() {
		return false
	}
	limit := f.warmupLimit.Load()
	for {
		reserved := f.warmupReserved.Load()
		if limit > 0 && f.registry.Loaded()+reserved >= limit {
			return false
		}
		if f.warmupReserved.CompareAndSwap(reserved, reserved+1) {
			return true
		}
	}
}

func (f *fakeShardUnloader) ReleaseWarmupLoad() { f.warmupReserved.Add(-1) }

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
	f.index.db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup { return nil })
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
	require.True(t, f.lazy.LoadedAt().IsZero())
}

type upgradingVectorIndex struct{ VectorIndex }

func (upgradingVectorIndex) Upgraded() bool             { return false }
func (upgradingVectorIndex) Upgrade(func()) error       { return nil }
func (upgradingVectorIndex) ShouldUpgrade() (bool, int) { return false, 0 }
func (upgradingVectorIndex) AlreadyIndexed() uint64     { return 0 }
func (upgradingVectorIndex) UpgradeInProgress() bool    { return true }

type lockedUpgradeVectorIndex struct{ VectorIndex }

func (lockedUpgradeVectorIndex) Upgraded() bool             { return false }
func (lockedUpgradeVectorIndex) Upgrade(func()) error       { return nil }
func (lockedUpgradeVectorIndex) ShouldUpgrade() (bool, int) { return false, 0 }
func (lockedUpgradeVectorIndex) AlreadyIndexed() uint64     { return 0 }
func (lockedUpgradeVectorIndex) UpgradeInProgress() bool {
	panic("the unloader must not wait on the index lock")
}
func (lockedUpgradeVectorIndex) TryUpgradeInProgress() (bool, bool) { return false, false }

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
	background := UnloadRequest{Reindex: noReindex, Mode: UnloadModeBackground}
	request := UnloadRequest{Reindex: noReindex, Mode: UnloadModeRequest}

	tests := []struct {
		name string
		req  UnloadRequest
		opts []func(*Config)
		// arm puts the shard in the clause's state and returns what undoes it; nil arm means no setup.
		arm  func(t *testing.T, f *unloadFixture, s *Shard) func()
		want UnloadOutcome
	}{
		{name: "idle shard unloads", req: background, want: UnloadOutcomeOK},
		{
			name: "cold shard",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), request))
				return nil
			},
			want: UnloadOutcomeAlreadyCold,
		},
		{
			name: "create lock held",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.shardCreateLocks.RLock(f.name)
				return func() { f.index.shardCreateLocks.RUnlock(f.name) }
			},
			want: UnloadOutcomeLockBusy,
		},
		{
			name: "loading blocked by backup or usage",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				return f.lazy.blockLoading()
			},
			want: UnloadOutcomeLockBusy,
		},
		{
			name: "wrapper replaced in the map",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				other := &LazyLoadShard{shardOpts: f.lazy.shardOpts}
				f.index.shards.Store(f.name, other)
				return func() { f.index.shards.Store(f.name, f.lazy) }
			},
			want: UnloadOutcomeNotInMap,
		},
		{
			name: "wrapper out of the map",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.shards.LoadAndDelete(f.name)
				return func() { f.index.shards.Store(f.name, f.lazy) }
			},
			want: UnloadOutcomeNotInMap,
		},
		{
			name: "recovering placeholder in the map",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.shards.Store(f.name, &RecoveringShard{LazyLoadShard: f.lazy})
				return func() { f.index.shards.Store(f.name, f.lazy) }
			},
			want: UnloadOutcomeRecovering,
		},
		{
			name: "pinned",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				_, release, err := f.index.GetShard(testCtx(), f.name)
				require.NoError(t, err)
				return release
			},
			want: UnloadOutcomeInUse,
		},
		{
			name: "halted for transfer",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				shard, release, err := f.index.GetShard(testCtx(), f.name)
				require.NoError(t, err)
				defer release()
				require.NoError(t, shard.HaltForTransfer(testCtx(), false, 0))
				return func() { require.NoError(t, s.resumeMaintenanceCycles(testCtx())) }
			},
			want: UnloadOutcomeHalted,
		},
		{
			name: "prepared 2PC write awaiting commit",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				resp := s.preparePutObject(testCtx(), "req-1", testObject(f.index.Config.ClassName.String()))
				require.Empty(t, resp.Errors)
				return func() { s.commitReplication(testCtx(), "req-1") }
			},
			want: UnloadOutcomePending2PC,
		},
		{
			name: "change log active",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				_, err := s.ActivateChangeLog(testCtx(), "op-1")
				require.NoError(t, err)
				return func() { require.NoError(t, s.StopChangeCapture(testCtx(), "op-1")) }
			},
			want: UnloadOutcomeChangeLog,
		},
		{
			name: "async replication target override",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.asyncReplicationRWMux.Lock()
				s.targetNodeOverrides = append(s.targetNodeOverrides, additional.AsyncReplicationTargetNodeOverride{TargetNode: "node2"})
				s.asyncReplicationRWMux.Unlock()
				return func() {
					s.asyncReplicationRWMux.Lock()
					s.targetNodeOverrides = nil
					s.asyncReplicationRWMux.Unlock()
				}
			},
			want: UnloadOutcomeAsyncOverride,
		},
		{
			name: "live async checkpoint",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				ht, err := hashtree.NewHashTree(2)
				require.NoError(t, err)
				s.asyncReplicationRWMux.Lock()
				s.asyncCheckpointHashtree = ht
				s.asyncReplicationRWMux.Unlock()
				return func() {
					s.asyncReplicationRWMux.Lock()
					s.asyncCheckpointHashtree = nil
					s.asyncReplicationRWMux.Unlock()
				}
			},
			want: UnloadOutcomeAsyncCheckpoint,
		},
		{
			name: "structural vector op in flight",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				return replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return upgradingVectorIndex{v} })
			},
			want: UnloadOutcomeVectorOp,
		},
		{
			name: "vector index lock held",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				return replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return lockedUpgradeVectorIndex{v} })
			},
			want: UnloadOutcomeLockBusy,
		},
		{
			name: "vector slots write-locked",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.vectors.mu.Lock()
				return s.vectors.mu.Unlock
			},
			want: UnloadOutcomeLockBusy,
		},
		{
			name: "property indices write-locked",
			req:  background,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.propertyIndicesLock.Lock()
				return s.propertyIndicesLock.Unlock
			},
			want: UnloadOutcomeLockBusy,
		},
		{
			name: "live reindex",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
					return func(string, string) bool { return true }
				})
				return func() {
					f.index.db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
						return func(string, string) bool { return false }
					})
				}
			},
			want: UnloadOutcomeReindex,
		},
		{
			name: "manually set read-only",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				require.NoError(t, s.UpdateStatus(storagestate.StatusReadOnly.String(), statusReasonManualUpdate))
				return func() {
					require.NoError(t, s.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate))
				}
			},
			want: UnloadOutcomeManualReadOnly,
		},
		{
			name: "read-only from resource pressure still unloads",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				require.NoError(t, s.SetStatusReadonly(statusReasonResourcePressure))
				return nil
			},
			want: UnloadOutcomeOK,
		},
		{
			name: "background respects min residency",
			req:  UnloadRequest{Reindex: noReindex, Mode: UnloadModeBackground, MinResidency: time.Hour},
			want: UnloadOutcomeMinResidency,
		},
		{
			name: "request ignores min residency",
			req:  UnloadRequest{Reindex: noReindex, Mode: UnloadModeRequest, MinResidency: time.Hour},
			want: UnloadOutcomeOK,
		},
		{
			name: "background respects idle ttl",
			req:  UnloadRequest{Reindex: noReindex, Mode: UnloadModeBackground, IdleTTL: time.Hour},
			want: UnloadOutcomeNotIdle,
		},
		{
			name: "request ignores idle ttl",
			req:  UnloadRequest{Reindex: noReindex, Mode: UnloadModeRequest, IdleTTL: time.Hour},
			want: UnloadOutcomeOK,
		},
		{
			name: "background waits for the vector queue",
			req:  background,
			opts: []func(*Config){func(c *Config) { c.AsyncIndexingEnabled = true }},
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				slot, ok := s.vectors.get("")
				require.True(t, ok)
				q := slot.queue
				require.NoError(t, q.Pause(testCtx()))
				obj := testObject(f.index.Config.ClassName.String())
				obj.Vector = []float32{1, 2, 3}
				require.NoError(t, s.PutObject(testCtx(), obj))
				require.Positive(t, q.Size())
				return func() {
					q.Resume()
					require.Eventually(t, func() bool { return q.Size() == 0 }, 10*time.Second, 10*time.Millisecond)
				}
			},
			want: UnloadOutcomeQueueNotEmpty,
		},
		{
			name: "background waits for hashtree init",
			req:  background,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				ht, err := hashtree.NewHashTree(2)
				require.NoError(t, err)
				s.asyncReplicationRWMux.Lock()
				s.hashtree, s.hashtreeFullyInitialized = ht, false
				s.asyncReplicationCancelFunc = func() {}
				s.asyncReplicationRWMux.Unlock()
				return func() {
					s.asyncReplicationRWMux.Lock()
					s.hashtree = nil
					s.asyncReplicationRWMux.Unlock()
				}
			},
			want: UnloadOutcomeHashtreeInit,
		},
		{
			name: "request ignores hashtree init",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				ht, err := hashtree.NewHashTree(2)
				require.NoError(t, err)
				s.asyncReplicationRWMux.Lock()
				s.hashtree, s.hashtreeFullyInitialized = ht, false
				s.asyncReplicationCancelFunc = func() {}
				s.asyncReplicationRWMux.Unlock()
				return nil
			},
			want: UnloadOutcomeOK,
		},
		{
			name: "pending shutdown counts as in use",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.shutdownRequested.Store(true)
				return func() { s.shutdownRequested.Store(false) }
			},
			want: UnloadOutcomeInUse,
		},
		{
			name: "drop in progress counts as in use",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				s.dropRequested.Store(true)
				return func() { s.dropRequested.Store(false) }
			},
			want: UnloadOutcomeInUse,
		},
		{
			name: "index closing",
			req:  request,
			arm: func(t *testing.T, f *unloadFixture, s *Shard) func() {
				f.index.signalCloseRequested(errIndexShutdown)
				return nil
			},
			want: UnloadOutcomeIndexClosing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t, tt.opts...)
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
			require.True(t, f.lazy.TryMarkUnloadPending())
			req := tt.req
			snap, err := f.index.db.ReindexActivitySnapshot(testCtx())
			require.NoError(t, err)
			req.Reindex = snap
			got := tryUnloadPromptly(t, f.lazy, req)
			require.Equal(t, tt.want, got)
			require.False(t, f.lazy.UnloadPending(), "TryUnload clears the handoff claim")
			undo()

			switch tt.want {
			case UnloadOutcomeOK, UnloadOutcomeAlreadyCold:
				f.requireUnloaded(t)
				require.True(t, s.shut.Load())
				require.False(t, s.shutdownRequested.Load(), "an unload never requests a shutdown")
				require.NoError(t, s.teardownError())
			case UnloadOutcomeIndexClosing:
				require.Same(t, s, f.lazy.currentShard())
				return
			default:
				require.Same(t, s, f.lazy.currentShard(), "a refused unload leaves the shard loaded")
				require.False(t, s.shut.Load())
				require.EqualValues(t, 1, f.unloader.registry.Loaded())
				require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), request), "the refusal lifts with its cause")
				f.requireUnloaded(t)
			}
			require.True(t, f.exists(t, id), "data survives the unload and the reload")
			require.NotSame(t, s, f.lazy.currentShard())
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
		})
	}
}

// tryUnloadPromptly fails the test instead of hanging when TryUnload waits on a lock.
func tryUnloadPromptly(t *testing.T, l *LazyLoadShard, req UnloadRequest) UnloadOutcome {
	t.Helper()
	done := make(chan UnloadOutcome, 1)
	go func() { done <- l.TryUnload(testCtx(), req) }()
	select {
	case got := <-done:
		return got
	case <-time.After(10 * time.Second):
		require.FailNow(t, "TryUnload blocked instead of refusing")
		return ""
	}
}

func TestLazyLoadShard_TryUnloadTornTeardown(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	s := f.lazy.currentShard()
	replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return failingShutdownVectorIndex{v} })

	require.Equal(t, UnloadOutcomeTorn, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
	require.Same(t, s, f.lazy.currentShard(), "the torn shard stays published as the last reference to its handles")
	require.Error(t, s.teardownError())
	require.Zero(t, f.unloader.registry.Loaded(), "a torn shard no longer counts as loaded")
	require.Equal(t, UnloadOutcomeTorn, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))

	_, _, err := f.index.getOrInitShard(testCtx(), f.name)
	require.ErrorContains(t, err, "injected vector index shutdown failure")
}
