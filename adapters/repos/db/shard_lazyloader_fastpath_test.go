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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

type fastPathOp struct {
	name string
	run  func(f *unloadFixture, id strfmt.UUID) error
}

func fastPathOps() []fastPathOp {
	return []fastPathOp{
		{name: "GetShard pin", run: func(f *unloadFixture, id strfmt.UUID) error {
			_, release, err := f.index.GetShard(testCtx(), f.name)
			release()
			return err
		}},
		{name: "getOrInitShard pin", run: func(f *unloadFixture, id strfmt.UUID) error {
			_, release, err := f.index.getOrInitShard(testCtx(), f.name)
			release()
			return err
		}},
		{name: "loadIfCold read", run: func(f *unloadFixture, id strfmt.UUID) error {
			if ok, err := f.lazy.Exists(testCtx(), id); err != nil || !ok {
				return fmt.Errorf("exists %v: %w", ok, err)
			}
			return nil
		}},
		{name: "write", run: func(f *unloadFixture, id strfmt.UUID) error {
			return f.lazy.PutObject(testCtx(), testObject(f.index.Config.ClassName.String()))
		}},
	}
}

func readOnlyFastPathOps() []fastPathOp {
	return []fastPathOp{
		{name: "status", run: func(f *unloadFixture, id strfmt.UUID) error {
			if got := f.lazy.GetStatus(); got != storagestate.StatusReady {
				return fmt.Errorf("status %s", got)
			}
			if got := f.lazy.GetStatusReason(); got == storagestate.StatusLazyLoading.String() {
				return fmt.Errorf("status reason %s", got)
			}
			return nil
		}},
		{name: "async object count", run: func(f *unloadFixture, id strfmt.UUID) error {
			_, err := f.lazy.ObjectCountAsync(testCtx())
			return err
		}},
		{name: "loaded shard", run: func(f *unloadFixture, id strfmt.UUID) error {
			if !f.lazy.isLoaded() || f.lazy.loadedShard() == nil {
				return errors.New("reads as unloaded")
			}
			return nil
		}},
		{name: "teardown checks", run: func(f *unloadFixture, id strfmt.UUID) error {
			if err := shardTeardownError(f.lazy); err != nil || shardKnownShut(f.lazy) {
				return fmt.Errorf("live shard reads as shut: %w", err)
			}
			return nil
		}},
	}
}

type fastPathResult struct {
	shard *Shard
	err   error
}

// runFastPathOp runs op and reports the shard the wrapper holds once it returned.
func runFastPathOp(f *unloadFixture, op fastPathOp, id strfmt.UUID) <-chan fastPathResult {
	done := make(chan fastPathResult, 1)
	enterrors.GoWrapper(func() {
		err := op.run(f, id)
		done <- fastPathResult{shard: f.lazy.currentShard(), err: err}
	}, f.index.logger)
	return done
}

func TestLazyLoadShard_LoadedWrapperServesWhileItsMutexIsHeld(t *testing.T) {
	for _, op := range append(fastPathOps(), readOnlyFastPathOps()...) {
		t.Run(op.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			loaded := f.lazy.currentShard()

			t.Cleanup(f.lazy.blockLoading())
			select {
			case r := <-runFastPathOp(f, op, id):
				require.NoError(t, r.err)
				require.Same(t, loaded, r.shard)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "a request on a loaded wrapper waited on the loading mutex")
			}
		})
	}
}

func TestLazyLoadShard_FastPathWaitsOnAnUnloadTeardown(t *testing.T) {
	for _, op := range fastPathOps() {
		t.Run(op.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			victim := f.lazy.currentShard()
			proceed, unloaded := unloadParkedInTeardown(t, f.lazy)

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
	require.Zero(t, f.unloader.registry.Loaded(), "a torn shard no longer counts as loaded")
	require.False(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
	_, _, err := f.index.getOrInitShard(testCtx(), f.name)
	require.ErrorContains(t, err, "injected vector index shutdown failure")
	for _, op := range fastPathOps()[:3] {
		t.Run(op.name, func(t *testing.T) {
			require.Error(t, op.run(f, id))
			require.Same(t, s, f.lazy.currentShard(), "the torn shard stays published")
		})
	}
}

func TestLazyLoadShard_ColdWrapperLoadsOnceUnderConcurrentPins(t *testing.T) {
	for _, op := range fastPathOps() {
		t.Run(op.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			id := f.put(t)
			require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))

			const callers = 16
			start := make(chan struct{})
			results := make([]<-chan fastPathResult, callers)
			for i := range results {
				results[i] = runFastPathOp(f, fastPathOp{run: func(f *unloadFixture, id strfmt.UUID) error {
					<-start
					return op.run(f, id)
				}}, id)
			}
			close(start)
			for _, done := range results {
				r := <-done
				require.NoError(t, r.err)
				require.False(t, r.shard.shut.Load())
				require.Same(t, f.lazy.currentShard(), r.shard, "concurrent callers loaded more than once")
			}
			require.EqualValues(t, 1, f.unloader.registry.Loaded())
		})
	}
}

func TestLazyLoadShard_StatusFastPathNeverReadsATeardownsShutdown(t *testing.T) {
	tests := []struct {
		name string
		read func(l *LazyLoadShard) string
	}{
		{name: "GetStatus", read: func(l *LazyLoadShard) string { return l.GetStatus().String() }},
		{name: "GetStatusReason", read: func(l *LazyLoadShard) string { return l.GetStatusReason() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			s := f.lazy.currentShard()
			s.statusLock.RLock()
			got := make(chan string, 1)
			enterrors.GoWrapper(func() { got <- tt.read(f.lazy) }, f.index.logger)
			require.Eventually(t, func() bool {
				if s.statusLock.TryRLock() {
					s.statusLock.RUnlock()
					return false
				}
				return true
			}, 5*time.Second, time.Millisecond)
			prev := s.status
			s.shut.Store(true)
			s.status = ShardStatus{Status: storagestate.StatusShutdown, Reason: statusReasonShutdown}
			f.lazy.shard.Store(nil)
			s.statusLock.RUnlock()

			require.Equal(t, storagestate.StatusLazyLoading.String(), <-got)
			s.statusLock.Lock()
			s.status = prev
			s.statusLock.Unlock()
			s.shut.Store(false)
			f.lazy.shard.Store(s)
		})
	}
}
