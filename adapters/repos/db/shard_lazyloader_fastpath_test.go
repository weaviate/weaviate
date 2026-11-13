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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storagestate"
)

type fastPathOp struct {
	name string
	run  func(f *unloadFixture, id strfmt.UUID) (*Shard, error)
}

func pinnedShard(f *unloadFixture, release func(), err error) (*Shard, error) {
	if err != nil {
		return nil, err
	}
	defer release()
	return f.lazy.currentShard(), nil
}

func fastPathOps() []fastPathOp {
	return []fastPathOp{
		{name: "GetShard pin", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			_, release, err := f.index.GetShard(testCtx(), f.name)
			return pinnedShard(f, release, err)
		}},
		{name: "getOrInitShard pin", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			_, release, err := f.index.getOrInitShard(testCtx(), f.name)
			return pinnedShard(f, release, err)
		}},
		{name: "wrapper pin", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			release, err := f.lazy.preventShutdown()
			return pinnedShard(f, release, err)
		}},
		{name: "mustLoad read", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			if n, err := f.lazy.ObjectCount(testCtx()); err != nil || n == 0 {
				return nil, fmt.Errorf("object count %d: %w", n, err)
			}
			return f.lazy.currentShard(), nil
		}},
		{name: "loadIfCold read", run: func(f *unloadFixture, id strfmt.UUID) (*Shard, error) {
			if ok, err := f.lazy.Exists(testCtx(), id); err != nil || !ok {
				return nil, fmt.Errorf("exists %v: %w", ok, err)
			}
			return f.lazy.currentShard(), nil
		}},
		{name: "write", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			if err := f.lazy.PutObject(testCtx(), testObject(f.index.Config.ClassName.String())); err != nil {
				return nil, err
			}
			return f.lazy.currentShard(), nil
		}},
	}
}

func readOnlyFastPathOps() []fastPathOp {
	return []fastPathOp{
		{name: "status", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			if got := f.lazy.GetStatus(); got != storagestate.StatusReady {
				return nil, fmt.Errorf("status %s", got)
			}
			if got := f.lazy.GetStatusReason(); got == storagestate.StatusLazyLoading.String() {
				return nil, fmt.Errorf("status reason %s", got)
			}
			return f.lazy.currentShard(), nil
		}},
		{name: "async object count", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			if _, err := f.lazy.ObjectCountAsync(testCtx()); err != nil {
				return nil, err
			}
			return f.lazy.currentShard(), nil
		}},
		{name: "loaded shard", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			if !f.lazy.isLoaded() {
				return nil, errors.New("reads as unloaded")
			}
			return f.lazy.loadedShard(), nil
		}},
		{name: "teardown checks", run: func(f *unloadFixture, _ strfmt.UUID) (*Shard, error) {
			if err := shardTeardownError(f.lazy); err != nil || shardKnownShut(f.lazy) {
				return nil, fmt.Errorf("live shard reads as shut: %w", err)
			}
			return f.lazy.currentShard(), nil
		}},
	}
}

func newPlainUnloadFixture(t *testing.T) *unloadFixture {
	f := newAddPropertyLazyFixture(t, "FastPathPlain", singleShardState())
	uf := &unloadFixture{addPropertyLazyFixture: f}
	for name, lazy := range f.coldShards(t) {
		uf.name, uf.lazy = name, lazy
	}
	return uf
}

func runFastPathOp(f *unloadFixture, op fastPathOp, id strfmt.UUID) <-chan fastPathResult {
	done := make(chan fastPathResult, 1)
	go func() {
		s, err := op.run(f, id)
		done <- fastPathResult{shard: s, err: err}
	}()
	return done
}

type fastPathResult struct {
	shard *Shard
	err   error
}

func TestLazyLoadShard_LoadedWrapperServesWhileItsMutexIsHeld(t *testing.T) {
	fixtures := []struct {
		name string
		new  func(t *testing.T) *unloadFixture
	}{
		{name: "with unloader", new: func(t *testing.T) *unloadFixture { return newUnloadFixture(t) }},
		{name: "nil unloader", new: newPlainUnloadFixture},
	}
	for _, fx := range fixtures {
		for _, op := range append(fastPathOps(), readOnlyFastPathOps()...) {
			t.Run(fx.name+"/"+op.name, func(t *testing.T) {
				f := fx.new(t)
				id := f.put(t)
				loaded := f.lazy.currentShard()
				require.NotNil(t, loaded)

				release := sync.OnceFunc(f.lazy.blockLoading())
				t.Cleanup(release)
				select {
				case r := <-runFastPathOp(f, op, id):
					require.NoError(t, r.err)
					require.Same(t, loaded, r.shard)
					require.False(t, r.shard.shut.Load())
				case <-time.After(5 * time.Second):
					require.FailNow(t, "a request on a loaded wrapper waited on the loading mutex")
				}
			})
		}
	}
}

func TestLazyLoadShard_FastPathWaitsOnAnUnloadTeardown(t *testing.T) {
	for _, op := range fastPathOps() {
		t.Run(op.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			victim := f.lazy.currentShard()
			entered, proceed := blockTeardown(t, f.lazy)
			unloaded := make(chan bool, 1)
			go func() { unloaded <- f.lazy.TryUnload(testCtx(), UnloadRequest{}) }()
			<-entered

			done := runFastPathOp(f, op, id)
			select {
			case r := <-done:
				t.Fatalf("finished during the teardown: %+v", r)
			case <-time.After(100 * time.Millisecond):
			}
			proceed()
			require.True(t, <-unloaded)
			r := <-done
			require.NoError(t, r.err)
			require.NotSame(t, victim, r.shard, "the request reloads a fresh instance")
			require.False(t, r.shard.shut.Load())
			require.True(t, victim.shut.Load())
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
		})
	}
}

func TestLazyLoadShard_FastPathWaitsOnAnUnloadDecision(t *testing.T) {
	for _, op := range append(fastPathOps(), readOnlyFastPathOps()...) {
		t.Run(op.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			loaded := f.lazy.currentShard()

			f.lazy.mutex.Lock()
			f.lazy.unloadPhase.Store(unloadPhaseDeciding)
			undo := sync.OnceFunc(func() {
				f.lazy.unloadPhase.Store(unloadPhaseNone)
				f.lazy.mutex.Unlock()
			})
			t.Cleanup(undo)

			done := runFastPathOp(f, op, id)
			select {
			case r := <-done:
				t.Fatalf("finished while an unload was deciding: %+v", r)
			case <-time.After(100 * time.Millisecond):
			}
			undo()
			r := <-done
			require.NoError(t, r.err)
			require.Same(t, loaded, r.shard)
		})
	}
}

func TestLazyLoadShard_TornWrapperIsNotServedOrReloaded(t *testing.T) {
	f := newUnloadFixture(t)
	id := f.put(t)
	s := f.lazy.currentShard()
	replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return failingShutdownVectorIndex{v} })
	require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
	require.Error(t, s.teardownError())
	beforeLoads := f.unloader.beforeLoads.Load()

	for _, op := range fastPathOps()[:3] {
		t.Run(op.name, func(t *testing.T) {
			_, err := op.run(f, id)
			require.Error(t, err)
			require.Same(t, s, f.lazy.currentShard(), "the torn shard stays published")
			require.Equal(t, beforeLoads, f.unloader.beforeLoads.Load(), "a torn wrapper never reloads")
		})
	}
}

func TestLazyLoadShard_ColdWrapperLoadsOnceUnderConcurrentPins(t *testing.T) {
	for _, op := range fastPathOps() {
		t.Run(op.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
			beforeLoads := f.unloader.beforeLoads.Load()

			const callers = 16
			start := make(chan struct{})
			results := make(chan fastPathResult, callers)
			for range callers {
				go func() {
					<-start
					s, err := op.run(f, id)
					results <- fastPathResult{shard: s, err: err}
				}()
			}
			close(start)
			loaded := map[*Shard]struct{}{}
			for range callers {
				r := <-results
				require.NoError(t, r.err)
				require.False(t, r.shard.shut.Load())
				loaded[r.shard] = struct{}{}
			}
			require.Len(t, loaded, 1, "concurrent callers built more than one instance")
			require.Equal(t, beforeLoads+1, f.unloader.beforeLoads.Load())
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
		})
	}
}

func BenchmarkPinBareShardVsLoadedWrapper(b *testing.B) {
	tests := []struct {
		name     string
		unloader bool
		bare     bool
	}{
		{name: "bare shard", bare: true},
		{name: "loaded wrapper"},
		{name: "loaded wrapper with unloader", unloader: true},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			var opts []func(*Config)
			if tt.unloader {
				opts = append(opts, withShardUnloader(newFakeShardUnloader()))
			}
			f := newAddPropertyLazyFixture(b, "BenchPin", singleShardState(), opts...)
			var lazy *LazyLoadShard
			for _, l := range f.coldShards(b) {
				lazy = l
			}
			if _, _, err := lazy.loadIfCold(context.Background()); err != nil {
				b.Fatal(err)
			}
			var target ShardLike = lazy
			if tt.bare {
				target = lazy.currentShard()
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					release, err := target.preventShutdown()
					if err != nil {
						b.Error(err)
						return
					}
					release()
				}
			})
		})
	}
}
