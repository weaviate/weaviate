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

package lsmkv

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// newTestStoreWithObjectsBucket spins up a noop-cycled store with a real
// objects bucket — needed because Pause/Resume dereferences
// b.disk.compactionCallbackCtrl.
func newTestStoreWithObjectsBucket(t *testing.T) (*Store, *Bucket) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		WithStrategy(StrategyReplace)))
	b := store.bucketsByName[helpers.ObjectsBucketLSM]
	require.NotNil(t, b)
	return store, b
}

// TestPauseResumeObjectBucketCompaction_NoRecursiveRLock pins
// weaviate/0-weaviate-issues#251: a bucketAccessLock writer queued between
// recursive RLocks deadlocked the store. With the bug, this test hangs.
func TestPauseResumeObjectBucketCompaction_NoRecursiveRLock(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreWithObjectsBucket(t)

	const (
		readers = 100
		writers = 20
		iters   = 200
	)

	var wg sync.WaitGroup
	wg.Add(readers + writers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				require.NoError(t, store.PauseObjectBucketCompaction(ctx))
				require.NoError(t, store.ResumeObjectBucketCompaction(ctx))
			}
		}()
	}

	// Queued writers — the contention the recursive RLock would deadlock against.
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				store.bucketAccessLock.Lock()
				runtime.Gosched()
				store.bucketAccessLock.Unlock()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("deadlock: Pause/Resume + write-lock contention did not drain in 60s "+
			"(recursive bucketAccessLock.RLock regression, see weaviate/0-weaviate-issues#251)\n%s", buf)
	}
}

// TestDoStartStopPauseTimer_RefCount pins ref-counted timer behavior:
// overlapping pause sources (backup + reindex) must not overwrite a live
// timer (weaviate/weaviate#11486 Copilot review).
func TestDoStartStopPauseTimer_RefCount(t *testing.T) {
	_, b := newTestStoreWithObjectsBucket(t)

	b.doStartPauseTimer()
	outer := b.pauseTimer
	require.NotNil(t, outer, "outer Start must allocate a timer")

	b.doStartPauseTimer()
	require.Same(t, outer, b.pauseTimer, "inner Start must not overwrite the outer timer")

	b.doStopPauseTimer()
	require.NotNil(t, b.pauseTimer, "inner Stop must not clear timer while outer holds")

	b.doStopPauseTimer()
	require.Nil(t, b.pauseTimer, "outer Stop must clear the timer")

	// Extra Stop must be a no-op (ref-count guard).
	b.doStopPauseTimer()
	require.Nil(t, b.pauseTimer)
}

// TestDoStartStopPauseTimer_RaceFree pins the race surfaced on
// weaviate/weaviate#11486: b.pauseTimer is touched by both the backup path
// (Store.Pause/ResumeCompaction) and the reindex path
// (Pause/ResumeObjectBucketCompaction), both via doStartPauseTimer /
// doStopPauseTimer. Without a shared mutex these race on b.pauseTimer
// (assignment vs ObserveDuration); -race fails the test under the bug.
func TestDoStartStopPauseTimer_RaceFree(t *testing.T) {
	_, b := newTestStoreWithObjectsBucket(t)

	const (
		goroutines = 50
		iters      = 200
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				b.doStartPauseTimer()
				b.doStopPauseTimer()
			}
		}()
	}
	wg.Wait()
}
