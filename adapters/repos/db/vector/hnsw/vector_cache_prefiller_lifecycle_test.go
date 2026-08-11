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

package hnsw

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// blockingPreloadCache holds a scan worker inside the cache write so a prefill can be
// observed mid-flight, without depending on scan timing.
type blockingPreloadCache struct {
	cache.Cache[float32]
	inner       cache.IfAbsentPreloader[float32]
	entered     chan struct{}
	enterOnce   sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

// newBlockingPreloadCache releases on cleanup as well: a blocked scan worker holds a
// cursor, so a test that fails before releasing would deadlock the store shutdown
// registered by newTestObjectsStore instead of reporting the failure.
func newBlockingPreloadCache(t *testing.T, inner cache.Cache[float32]) *blockingPreloadCache {
	b := &blockingPreloadCache{
		Cache:   inner,
		inner:   mustIfAbsentPreloader(t, inner),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(b.Release)
	return b
}

func (b *blockingPreloadCache) Release() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func (b *blockingPreloadCache) PreloadIfAbsent(id uint64, vec []float32) bool {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.release
	return b.inner.PreloadIfAbsent(id, vec)
}

func newLifecycleTestIndex(t *testing.T, c cache.Cache[float32], nodesLen int) *hnsw {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return newPrefillTestIndex("main", nil, c, nodesLen, distancer.NewDotProductProvider(), logger)
}

func waitFor(t *testing.T, done <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

func stopPrefillAsync(h *hnsw) <-chan struct{} {
	returned := make(chan struct{})
	enterrors.GoWrapper(func() {
		h.stopPrefill()
		close(returned)
	}, h.logger)
	return returned
}

// TestStopPrefillWaitsForInFlightScan is the shutdown-safety contract: stopPrefill must
// not return while a scan worker is still running, because Drop/Shutdown close the lsmkv
// segments right after and a live cursor would then read unmapped memory. Cancellation
// alone does not provide this — a worker between context checks keeps reading.
func TestStopPrefillWaitsForInFlightScan(t *testing.T) {
	const n = 200
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i), float32(i) + 1}, nil)
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	inner := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	inner.Grow(n)
	blocking := newBlockingPreloadCache(t, inner)

	h := newLifecycleTestIndex(t, blocking, n)
	h.store = store
	h.waitForCachePrefill = false

	require.True(t, h.useParallelPrefill(), "test must exercise the scan path")
	h.prefillCache(context.Background())

	waitFor(t, blocking.entered, 10*time.Second, "no scan worker reached the cache")

	returned := stopPrefillAsync(h)
	select {
	case <-returned:
		t.Error("stopPrefill returned while a scan worker was still inside the cache write")
	case <-time.After(100 * time.Millisecond):
	}

	blocking.Release()
	waitFor(t, returned, 10*time.Second, "stopPrefill did not return after the scan was released")
	require.True(t, h.cachePrefilled.Load(), "a stopped prefill must still mark the cache prefilled")
}

// TestStopPrefillWaitsForPrefillRegisteredBeforeReturn pins the registration ordering:
// prefillCache must have joined the WaitGroup by the time it returns, so a stopPrefill
// that arrives before the prefill goroutine is scheduled still waits for it. Moving the
// WaitGroup join into the goroutine passes every other test here but lets stopPrefill
// return while the prefill is about to touch lsmkv segments.
//
// prefillCacheFunc defers cachePrefilled ahead of the deferred Done, so LIFO runs it
// first and "cachePrefilled is set once stopPrefill returns" is exactly the guarantee.
func TestStopPrefillWaitsForPrefillRegisteredBeforeReturn(t *testing.T) {
	const n = 200
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i), float32(i) + 1}, nil)
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	h := newLifecycleTestIndex(t, c, n)
	h.store = store
	h.waitForCachePrefill = false

	h.prefillCache(context.Background())
	h.stopPrefill()

	require.True(t, h.cachePrefilled.Load(),
		"stopPrefill returned before the prefill goroutine finished: it was not registered by the time prefillCache returned")
}

// TestStopPrefillCancelsBlockedPrefill: the wait alone would deadlock a prefill that is
// blocked on its own work, so stopPrefill must also cancel. Nothing here releases the
// prefiller except context cancellation.
func TestStopPrefillCancelsBlockedPrefill(t *testing.T) {
	const n = 50
	started := make(chan struct{})
	var startOnce sync.Once
	blockUntilCanceled := func(ctx context.Context, id uint64) ([]float32, error) {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(blockUntilCanceled, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	// no store: routing falls through to the serial by-id prefiller
	h := newLifecycleTestIndex(t, c, n)
	h.waitForCachePrefill = false
	require.False(t, h.useParallelPrefill())

	h.prefillCache(context.Background())
	waitFor(t, started, 10*time.Second, "prefill never started")

	waitFor(t, stopPrefillAsync(h), 10*time.Second,
		"stopPrefill did not cancel a prefill blocked on its context")
}

// TestStopPrefillWithoutPrefill: Drop/Shutdown call stopPrefill unconditionally, including
// on indexes whose prefill was skipped or never started.
func TestStopPrefillWithoutPrefill(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("never registered", func(t *testing.T) {
		h := newLifecycleTestIndex(t, nil, 0)
		waitFor(t, stopPrefillAsync(h), 10*time.Second, "stopPrefill hung with no prefill registered")
	})

	t.Run("prefill skipped as already done", func(t *testing.T) {
		c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
		h := newLifecycleTestIndex(t, c, 10)
		h.cachePrefilled.Store(true)

		h.prefillCache(context.Background())
		waitFor(t, stopPrefillAsync(h), 10*time.Second, "stopPrefill hung after a skipped prefill")
		require.Equal(t, int64(0), c.CountVectors())
	})
}

// TestPrefillCancelPropagatesFromCallerContext: the prefill runs under the caller's
// context (the shard's shutCtx in production), so canceling that must stop it too.
func TestPrefillCancelPropagatesFromCallerContext(t *testing.T) {
	const n = 50
	started := make(chan struct{})
	var startOnce sync.Once
	blockUntilCanceled := func(ctx context.Context, id uint64) ([]float32, error) {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(blockUntilCanceled, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	h := newLifecycleTestIndex(t, c, n)
	h.waitForCachePrefill = false

	ctx, cancel := context.WithCancel(context.Background())
	h.prefillCache(ctx)
	waitFor(t, started, 10*time.Second, "prefill never started")

	cancel()
	waitFor(t, stopPrefillAsync(h), 10*time.Second,
		"canceling the caller context did not stop the prefill")
}
