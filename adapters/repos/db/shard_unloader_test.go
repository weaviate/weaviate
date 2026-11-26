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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func registryWrapper() *LazyLoadShard {
	return &LazyLoadShard{shardOpts: &deferredShardOpts{index: &Index{}}}
}

func requireRegistryConsistent(t *testing.T, r *LoadedShardRegistry) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.EqualValues(t, len(r.shards), r.loaded.Load())
	for i, l := range r.shards {
		require.Equal(t, i+1, l.regSlot)
	}
}

func TestLoadedShardRegistry(t *testing.T) {
	tests := []struct {
		name       string
		n          int
		remove     []int
		wantLoaded int64
	}{
		{name: "empty"},
		{name: "single", n: 1, wantLoaded: 1},
		{name: "remove the only one", n: 1, remove: []int{0}},
		{name: "remove first swaps last in", n: 3, remove: []int{0}, wantLoaded: 2},
		{name: "remove last", n: 3, remove: []int{2}, wantLoaded: 2},
		{name: "remove the same twice", n: 4, remove: []int{1, 1}, wantLoaded: 3},
		{name: "remove two in the middle", n: 4, remove: []int{1, 2}, wantLoaded: 2},
		{name: "remove all", n: 2, remove: []int{0, 1, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewLoadedShardRegistry()
			wrappers := make([]*LazyLoadShard, tt.n)
			for i := range wrappers {
				wrappers[i] = registryWrapper()
				r.add(wrappers[i], nil)
				r.add(wrappers[i], nil)
			}
			requireRegistryConsistent(t, r)
			for _, idx := range tt.remove {
				r.remove(wrappers[idx])
				requireRegistryConsistent(t, r)
			}
			require.Equal(t, tt.wantLoaded, r.Loaded())
		})
	}
}

func TestLoadedShardRegistryAppendChunk(t *testing.T) {
	tests := []struct {
		name          string
		loaded, limit int
		wantChunks    int
	}{
		{name: "empty", loaded: 0, limit: 4, wantChunks: 1},
		{name: "one chunk", loaded: 3, limit: 4, wantChunks: 1},
		{name: "exact chunks", loaded: 8, limit: 4, wantChunks: 2},
		{name: "partial last chunk", loaded: 9, limit: 4, wantChunks: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewLoadedShardRegistry()
			for range tt.loaded {
				r.add(registryWrapper(), nil)
			}
			var got []*LazyLoadShard
			chunks := 0
			for from := 0; from >= 0; chunks++ {
				before := len(got)
				got, from = r.AppendChunk(got, from, tt.limit)
				require.LessOrEqual(t, len(got)-before, tt.limit)
			}
			require.Equal(t, tt.wantChunks, chunks)
			require.Equal(t, r.shards, got[:len(r.shards):len(r.shards)])
			require.Len(t, got, tt.loaded)
		})
	}
}

func TestLazyLoadShard_NilUnloaderLeavesWrapperUntouched(t *testing.T) {
	f := newAddPropertyLazyFixture(t, "NoUnloader", singleShardState())
	require.Nil(t, f.index.Config.ShardUnloader)
	prev := accessClock.Load()
	t.Cleanup(func() { accessClock.Store(prev) })
	for name, lazy := range f.coldShards(t) {
		accessClock.Store(42)
		shard, release, err := f.index.GetShard(testCtx(), name)
		require.NoError(t, err)
		require.NoError(t, shard.PutObject(testCtx(), testObject(f.index.Config.ClassName.String())))
		release()
		require.True(t, lazy.isLoaded())
		require.Zero(t, lazy.LastAccess())
		require.Zero(t, lazy.regSlot)
	}
}

func TestGetShard_UnloaderAddsNoAllocations(t *testing.T) {
	plain := newAddPropertyLazyFixture(t, "AllocsPlain", singleShardState())
	withUnloader := newUnloadFixture(t)
	allocs := func(index *Index, name string) float64 {
		run := func() {
			_, release, err := index.GetShard(context.Background(), name)
			if err != nil {
				panic(err)
			}
			release()
		}
		run()
		quietest := math.Inf(1)
		for range 5 {
			quietest = min(quietest, testing.AllocsPerRun(100, run))
		}
		return quietest
	}
	advanceAccessClock()
	for name := range plain.coldShards(t) {
		require.Equal(t, allocs(plain.index, name), allocs(withUnloader.index, withUnloader.name))
	}
}

func TestLazyLoadShard_DeferredShutdownLeavesTheRegistry(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	_, release, err := f.index.GetShard(testCtx(), f.name)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(testCtx(), 300*time.Millisecond)
	defer cancel()
	outcome, err := f.index.UnloadLocalShard(ctx, f.name)
	require.Error(t, err)
	require.Equal(t, ShardUnloadOutcomeRefusedInUse, outcome)
	require.EqualValues(t, 1, f.unloader.registry.Loaded())

	release()
	require.Eventually(t, func() bool { return shardKnownShut(f.lazy) }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, f.unloader.registry.Loaded(), "a shard the last release shut no longer counts as loaded")

	shard, release, err := f.index.getOrInitShard(testCtx(), f.name)
	require.NoError(t, err)
	require.NoError(t, shard.PutObject(testCtx(), testObject(f.index.Config.ClassName.String())))
	release()
	require.NotSame(t, f.lazy, f.index.shards.Load(f.name), "the cleanly shut wrapper was evicted and replaced")
	require.EqualValues(t, 1, f.unloader.registry.Loaded(), "the evicted wrapper must leave the registry")
}

func TestDBShutdownClosesTheShardUnloaderOnce(t *testing.T) {
	u := newFakeShardUnloader()
	repo, _, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, -1, withShardUnloader(u))
	require.Same(t, u, repo.ShardUnloader())
	require.NoError(t, repo.Shutdown(context.Background()))
	require.EqualValues(t, 1, u.closes.Load())
}

func TestLazyLoadShard_LateDeferredShutdownHookKeepsAReloadRegistered(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	old := f.lazy.currentShard()
	require.NotNil(t, old.deferredShutdownDone)
	require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
	f.put(t)
	reloaded := f.lazy.currentShard()
	require.NotSame(t, old, reloaded)
	require.EqualValues(t, 1, f.unloader.registry.Loaded())

	old.deferredShutdownDone()
	require.EqualValues(t, 1, f.unloader.registry.Loaded(), "a late hook of the old shard leaves the reload registered")
	requireRegistryConsistent(t, f.unloader.registry)

	reloaded.deferredShutdownDone()
	require.Zero(t, f.unloader.registry.Loaded(), "the current shard's hook deregisters")
}

func TestAccessClockIdle(t *testing.T) {
	for _, tc := range []struct {
		now, last uint32
		ttl       time.Duration
		want      bool
	}{
		{now: 10, last: 10, ttl: 0, want: false},
		{now: 10, last: 11, ttl: 0, want: false},
		{now: 11, last: 10, ttl: 0, want: true},
		{now: 11, last: 10, ttl: time.Second, want: false},
		{now: 12, last: 10, ttl: time.Second, want: true},
		{now: 12, last: 10, ttl: 1500 * time.Millisecond, want: false},
		{now: 13, last: 10, ttl: 1500 * time.Millisecond, want: true},
		{now: 12, last: 10, ttl: 2 * time.Second, want: false},
		{now: 13, last: 10, ttl: 2 * time.Second, want: true},
		{now: 0, last: 1<<32 - 1, ttl: time.Second, want: false},
		{now: 1<<32 - 1, last: 0, ttl: time.Hour, want: true},
	} {
		require.Equal(t, tc.want, AccessClockIdle(tc.now, tc.last, tc.ttl), "now=%d last=%d ttl=%v", tc.now, tc.last, tc.ttl)
	}
}

func TestRecordAccessNeverMovesLastAccessBack(t *testing.T) {
	for _, uptime := range []time.Duration{0, 2000 * time.Second} {
		t.Run(uptime.String(), func(t *testing.T) {
			epoch, prev := accessClockEpoch, accessClock.Load()
			t.Cleanup(func() { accessClockEpoch = epoch; accessClock.Store(prev) })
			accessClockEpoch = epoch.Add(-uptime)
			f := newUnloadFixture(t)
			f.put(t)

			newer := AccessClockNow() + 1000
			f.lazy.stampAccess(newer)
			f.lazy.stampAccess(newer - 10)
			require.Equal(t, newer, f.lazy.LastAccess(), "an older stamp must not overwrite a newer one")
			require.Equal(t, newer, AccessClockNow(), "an older stamp must not move the clock back")
			advanceAccessClock()
			require.Equal(t, newer, AccessClockNow(), "an older reading never moves the clock back")

			accessClock.Store(newer + 3)
			require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{IdleTTL: 5 * time.Second}))
			require.True(t, f.lazy.isLoaded())
		})
	}
}

func TestRequestDuringAStalledAccessClockKeepsTheShard(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	prev := accessClock.Load()
	t.Cleanup(func() { accessClock.Store(prev) })
	require.Eventually(t, func() bool { return time.Since(accessClockEpoch) >= 3*time.Second },
		5*time.Second, 10*time.Millisecond)

	accessClock.Store(1)
	_, release, err := f.index.GetShard(testCtx(), f.name)
	require.NoError(t, err)
	release()
	advanceAccessClock()

	require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{IdleTTL: time.Second}),
		"a request during a stalled clock must still count as recent")
	require.True(t, f.lazy.isLoaded())
}

type panickingCompressedIndex struct{ VectorIndex }

func (panickingCompressedIndex) Compressed() bool { panic("injected status read panic") }

func TestNodesStatusReleasesItsPinOnPanic(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	s := f.lazy.currentShard()
	restore := replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return panickingCompressedIndex{v} })
	defer restore()

	func() {
		defer func() { require.NotNil(t, recover()) }()
		var status []*models.NodeShardStatus
		f.index.getShardsNodeStatus(testCtx(), &status, "")
	}()
	require.Zero(t, s.inUseCounter.Load(), "a panicking status read must not leak the shard's pin")
}
