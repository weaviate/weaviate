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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func registryWrapper(namespace string) *LazyLoadShard {
	return &LazyLoadShard{shardOpts: &deferredShardOpts{index: &Index{namespace: namespace}}}
}

func requireRegistryConsistent(t *testing.T, r *LoadedShardRegistry) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.EqualValues(t, len(r.shards), r.loaded.Load())
	perNamespace := map[string]int{}
	for i, l := range r.shards {
		require.Equal(t, i+1, l.regSlot)
		perNamespace[l.namespace()]++
	}
	require.Equal(t, perNamespace, r.byNamespace)
}

func TestLoadedShardRegistry(t *testing.T) {
	tests := []struct {
		name       string
		namespaces []string
		remove     []int
		wantNS     map[string]int
	}{
		{name: "empty", wantNS: map[string]int{}},
		{name: "single", namespaces: []string{""}, wantNS: map[string]int{"": 1}},
		{name: "remove the only one", namespaces: []string{""}, remove: []int{0}, wantNS: map[string]int{}},
		{name: "remove first swaps last in", namespaces: []string{"a", "b", "c"}, remove: []int{0}, wantNS: map[string]int{"b": 1, "c": 1}},
		{name: "remove last", namespaces: []string{"a", "a", "b"}, remove: []int{2}, wantNS: map[string]int{"a": 2}},
		{name: "remove the same twice", namespaces: []string{"a", "b", "a", "b"}, remove: []int{1, 1}, wantNS: map[string]int{"a": 2, "b": 1}},
		{name: "remove two in the middle", namespaces: []string{"a", "b", "a", "b"}, remove: []int{1, 2}, wantNS: map[string]int{"a": 1, "b": 1}},
		{name: "remove all", namespaces: []string{"a", "b"}, remove: []int{0, 1, 0}, wantNS: map[string]int{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewLoadedShardRegistry()
			wrappers := make([]*LazyLoadShard, len(tt.namespaces))
			for i, ns := range tt.namespaces {
				wrappers[i] = registryWrapper(ns)
				r.add(wrappers[i])
				r.add(wrappers[i])
			}
			requireRegistryConsistent(t, r)
			for _, idx := range tt.remove {
				r.remove(wrappers[idx])
				requireRegistryConsistent(t, r)
			}
			for ns, n := range tt.wantNS {
				require.Equal(t, n, r.NamespaceLoaded(ns))
			}
			require.Len(t, r.byNamespace, len(tt.wantNS))
		})
	}
}

func TestLoadedShardRegistrySample(t *testing.T) {
	tests := []struct {
		name   string
		loaded int
		k      int
		want   int
	}{
		{name: "nothing loaded", loaded: 0, k: 16, want: 0},
		{name: "fewer than k", loaded: 5, k: 16, want: 5},
		{name: "exactly k", loaded: 16, k: 16, want: 16},
		{name: "more than k", loaded: 1000, k: 16, want: 16},
		{name: "zero k", loaded: 10, k: 0, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewLoadedShardRegistry()
			for range tt.loaded {
				r.add(registryWrapper(""))
			}
			dst := make([]*LazyLoadShard, 0, 16)
			got := r.Sample(dst, tt.k)
			require.LessOrEqual(t, len(got), tt.want)
			if tt.loaded <= tt.k || tt.loaded >= 4*tt.k {
				require.Len(t, got, tt.want)
			}
			seen := map[*LazyLoadShard]struct{}{}
			for _, l := range got {
				require.NotZero(t, l.regSlot)
				_, dup := seen[l]
				require.False(t, dup, "sampled twice")
				seen[l] = struct{}{}
			}
			if tt.k > 0 && tt.k <= cap(dst) {
				allocs := testing.AllocsPerRun(50, func() { dst = r.Sample(dst[:0], tt.k) })
				require.Zero(t, allocs)
			}
		})
	}
}

func TestAccessClockIsMonotonic(t *testing.T) {
	before := AccessClockNow()
	advanceAccessClock()
	after := AccessClockNow()
	require.GreaterOrEqual(t, after, before)
	require.Positive(t, after)
	accessClock.Store(after + 1000)
	advanceAccessClock()
	require.Equal(t, after+1000, AccessClockNow(), "an older reading never moves the clock back")
	accessClock.Store(after)
}

func TestLazyLoadShard_UnloaderHooks(t *testing.T) {
	f := newUnloadFixture(t)
	require.Zero(t, f.unloader.beforeLoads.Load())
	require.Zero(t, f.unloader.registry.Loaded())

	f.put(t)
	require.EqualValues(t, 1, f.unloader.beforeLoads.Load())
	require.EqualValues(t, 1, f.unloader.registry.Loaded())
	require.Equal(t, 1, f.unloader.registry.NamespaceLoaded(""))
	require.False(t, f.lazy.LoadedAt().IsZero())

	f.put(t)
	require.EqualValues(t, 1, f.unloader.beforeLoads.Load(), "a warm shard does not call BeforeLoad")

	f.lazy.lastAccess.Store(0)
	advanceAccessClock()
	_, release, err := f.index.GetShard(testCtx(), f.name)
	require.NoError(t, err)
	release()
	require.Equal(t, AccessClockNow(), f.lazy.LastAccess(), "a pin records the access")

	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
	f.put(t)
	require.EqualValues(t, 2, f.unloader.beforeLoads.Load())
	require.EqualValues(t, 1, f.unloader.registry.Loaded())

	outcome, err := f.index.UnloadLocalShard(testCtx(), f.name)
	require.NoError(t, err)
	require.Equal(t, ShardUnloadOutcomeUnloaded, outcome)
	require.Zero(t, f.unloader.registry.Loaded(), "a COLD unload deregisters")
}

func TestLazyLoadShard_UnloaderDeregisteredOnDropAndShutdown(t *testing.T) {
	tests := []struct {
		name  string
		stop  func(t *testing.T, f *unloadFixture)
		after UnloadOutcome
	}{
		{name: "drop", stop: func(t *testing.T, f *unloadFixture) { require.NoError(t, f.index.dropShards([]string{f.name})) }, after: UnloadOutcomeNotInMap},
		{name: "index shutdown", stop: func(t *testing.T, f *unloadFixture) { require.NoError(t, f.index.Shutdown(testCtx())) }, after: UnloadOutcomeIndexClosing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
			tt.stop(t, f)
			require.Zero(t, f.unloader.registry.Loaded())
			require.Equal(t, tt.after, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
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
		require.True(t, lazy.LoadedAt().IsZero())
		require.Zero(t, lazy.regSlot)
	}
}

func getShardAllocs(t testing.TB, f *addPropertyLazyFixture, name string) float64 {
	run := func() {
		_, release, err := f.index.GetShard(context.Background(), name)
		if err != nil {
			panic(err)
		}
		release()
	}
	run()
	return testing.AllocsPerRun(200, run)
}

func TestGetShard_UnloaderAddsNoAllocations(t *testing.T) {
	plain := newAddPropertyLazyFixture(t, "AllocsPlain", singleShardState())
	withUnloader := newUnloadFixture(t)
	var plainName string
	for name := range plain.coldShards(t) {
		plainName = name
	}
	advanceAccessClock()
	base := getShardAllocs(t, plain, plainName)
	got := getShardAllocs(t, withUnloader.addPropertyLazyFixture, withUnloader.name)
	require.Equal(t, base, got, "GetShard with an unloader allocated %v times, without %v", got, base)
}

func BenchmarkGetShardLazy(b *testing.B) {
	tests := []struct {
		name     string
		unloader bool
	}{{name: "nil unloader"}, {name: "with unloader", unloader: true}}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			var opts []func(*Config)
			if tt.unloader {
				opts = append(opts, withShardUnloader(newFakeShardUnloader()))
			}
			f := newAddPropertyLazyFixture(b, "BenchGetShard", singleShardState(), opts...)
			var name string
			for n := range f.coldShards(b) {
				name = n
			}
			advanceAccessClock()
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, release, err := f.index.GetShard(ctx, name)
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
	}
}

func TestLazyLoadShard_RequestDuringTeardownWaitsThenReloads(t *testing.T) {
	f := newUnloadFixture(t)
	id := f.put(t)
	victim := f.lazy.currentShard()

	marked := make(chan struct{})
	proceed := make(chan struct{})
	f.lazy.unloadMarkedHook = func() {
		close(marked)
		<-proceed
	}
	unloaded := make(chan UnloadOutcome, 1)
	enterrors.GoWrapper(func() { unloaded <- f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}) }, f.index.logger)
	<-marked

	require.True(t, f.index.shardCreateLocks.TryLock(f.name), "the teardown runs without the create lock")
	f.index.shardCreateLocks.Unlock(f.name)
	require.Equal(t, UnloadOutcomeLockBusy, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))

	type readResult struct {
		shard  *Shard
		exists bool
		err    error
	}
	read := make(chan readResult, 1)
	enterrors.GoWrapper(func() {
		shard, release, err := f.index.GetShard(testCtx(), f.name)
		defer release()
		if err != nil {
			read <- readResult{err: err}
			return
		}
		ok, err := shard.Exists(testCtx(), id)
		read <- readResult{shard: f.lazy.currentShard(), exists: ok, err: err}
	}, f.index.logger)

	select {
	case r := <-read:
		t.Fatalf("a request finished during the teardown: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}

	close(proceed)
	require.Equal(t, UnloadOutcomeOK, <-unloaded)
	r := <-read
	require.NoError(t, r.err)
	require.True(t, r.exists)
	require.NotSame(t, victim, r.shard, "the request reloads a fresh instance")
	require.True(t, victim.shut.Load())
	require.EqualValues(t, 1, f.unloader.registry.Loaded())
}

func TestStartupWarmupStopsWhenTheUnloaderSaysSo(t *testing.T) {
	tests := []struct {
		name       string
		denyWarmup bool
	}{
		{name: "warmup allowed loads the shard"},
		{name: "warmup denied leaves the shard cold", denyWarmup: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := newFakeShardUnloader()
			u.denyWarmup.Store(tt.denyWarmup)
			repo, migrator, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, 0, withShardUnloader(u))
			t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
			require.NoError(t, migrator.AddClass(testCtx(), newClassWithWarmProp("Warmup")))
			index := repo.GetIndex("Warmup")
			require.NotNil(t, index)
			require.Eventually(t, index.allShardsReady.Load, 10*time.Second, 10*time.Millisecond)
			loaded := 0
			require.NoError(t, index.ForEachLoadedShard(func(string, ShardLike) error { loaded++; return nil }))
			if tt.denyWarmup {
				require.Zero(t, loaded)
				require.Zero(t, u.registry.Loaded())
				return
			}
			require.Equal(t, 1, loaded)
			require.EqualValues(t, 1, u.registry.Loaded())
		})
	}
}

func TestLazyLoadShard_EvictedAfterDeferredShutdownLeavesTheRegistry(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	_, release, err := f.index.GetShard(testCtx(), f.name)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(testCtx(), 300*time.Millisecond)
	defer cancel()
	outcome, err := f.index.UnloadLocalShard(ctx, f.name)
	require.Error(t, err)
	require.Equal(t, ShardUnloadOutcomeRefusedInUse, outcome)
	release()
	require.Eventually(t, func() bool { return shardKnownShut(f.lazy) }, 5*time.Second, 10*time.Millisecond)

	shard, release, err := f.index.getOrInitShard(testCtx(), f.name)
	require.NoError(t, err)
	require.NoError(t, shard.PutObject(testCtx(), testObject(f.index.Config.ClassName.String())))
	release()
	require.NotSame(t, f.lazy, f.index.shards.Load(f.name), "the cleanly shut wrapper was evicted and replaced")
	require.EqualValues(t, 1, f.unloader.registry.Loaded(), "the evicted wrapper must leave the registry")
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
	require.True(t, f.lazy.LoadedAt().IsZero())
}

func TestLoadedShardRegistryNamespaceCounts(t *testing.T) {
	tests := []struct {
		name       string
		namespaces []string
		dst        map[string]int
		want       map[string]int
	}{
		{name: "empty into nil", want: map[string]int{}},
		{name: "stale entries cleared", dst: map[string]int{"gone": 3}, want: map[string]int{}},
		{name: "per namespace", namespaces: []string{"a", "b", "a", ""}, dst: map[string]int{"gone": 1}, want: map[string]int{"a": 2, "b": 1, "": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewLoadedShardRegistry()
			for _, ns := range tt.namespaces {
				r.add(registryWrapper(ns))
			}
			require.Equal(t, tt.want, r.NamespaceCounts(tt.dst))
		})
	}
}

func TestLazyLoadShard_UnloadedAtTracksTheLastUnload(t *testing.T) {
	f := newUnloadFixture(t)
	require.True(t, f.lazy.UnloadedAt().IsZero())
	f.put(t)
	require.True(t, f.lazy.UnloadedAt().IsZero())

	before := time.Now()
	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
	require.False(t, f.lazy.UnloadedAt().Before(before))

	f.put(t)
	require.True(t, f.lazy.UnloadedAt().IsZero(), "a reload clears it")
}

func TestDBShutdownClosesTheShardUnloaderOnce(t *testing.T) {
	u := newFakeShardUnloader()
	repo, _, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, -1, withShardUnloader(u))
	require.Same(t, u, repo.ShardUnloader())
	require.NoError(t, repo.Shutdown(context.Background()))
	require.EqualValues(t, 1, u.closes.Load())
}

func TestDBShardUnloaderIsNilWithoutOne(t *testing.T) {
	repo, _, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, -1)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	require.Nil(t, repo.ShardUnloader())
}
