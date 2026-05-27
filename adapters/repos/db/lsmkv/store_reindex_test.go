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

// TestPauseResumeObjectBucketCompaction_NoRecursiveRLock guards against
// reintroducing the recursive bucketAccessLock.RLock in Pause/Resume
// ObjectBucketCompaction (weaviate/0-weaviate-issues#251).
//
// Both methods take bucketAccessLock.RLock() and then look up the objects
// bucket. The fix has them use the non-locking bucketNoLock; the bug used the
// public Bucket(), which RLocks again. RWMutex is not reentrant: if a writer
// takes bucketAccessLock.Lock() while queued between the two RLocks, the second
// RLock blocks, the first is never released, and the store deadlocks.
//
// Here many goroutines run Pause/Resume while others hold the write lock; with
// the recursive RLock this hangs (caught by the timeout), with the fix it
// drains promptly.
func TestPauseResumeObjectBucketCompaction_NoRecursiveRLock(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(ctx)

	// Real objects bucket — Pause/Resume dereference b.disk.compactionCallbackCtrl.
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		WithStrategy(StrategyReplace)))

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

	// Writers take bucketAccessLock.Lock() directly: this is the queued writer
	// that the recursive RLock deadlocks against.
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				store.bucketAccessLock.Lock()
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
