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

func stopPostStartupAsync(h *hnsw) <-chan struct{} {
	returned := make(chan struct{})
	enterrors.GoWrapper(func() {
		h.stopPostStartup()
		close(returned)
	}, h.logger)
	return returned
}

// TestStopPrefillWaitsForInFlightScan is the shutdown-safety contract: stopPostStartup must
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

	returned := stopPostStartupAsync(h)
	select {
	case <-returned:
		t.Error("stopPostStartup returned while a scan worker was still inside the cache write")
	case <-time.After(100 * time.Millisecond):
	}

	blocking.Release()
	waitFor(t, returned, 10*time.Second, "stopPostStartup did not return after the scan was released")
	require.True(t, h.cachePrefilled.Load(), "a stopped prefill must still mark the cache prefilled")
}

// TestStopPrefillWaitsForPrefillRegisteredBeforeReturn: prefillCache must have joined
// the WaitGroup before it returns, so a stopPostStartup arriving before the goroutine is
// scheduled still waits. Moving the join into the goroutine passes every other test
// here. cachePrefilled is deferred ahead of Done, so it is set once the wait returns.
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
	h.stopPostStartup()

	require.True(t, h.cachePrefilled.Load(),
		"stopPostStartup returned before the prefill goroutine finished: it was not registered by the time prefillCache returned")
}

// TestStopPrefillCancelsBlockedPrefill: the wait alone would deadlock a prefill that is
// blocked on its own work, so stopPostStartup must also cancel. Nothing here releases the
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

	waitFor(t, stopPostStartupAsync(h), 10*time.Second,
		"stopPostStartup did not cancel a prefill blocked on its context")
}

// TestStopPrefillWithoutPrefill: Drop/Shutdown call stopPostStartup unconditionally, including
// on indexes whose prefill was skipped or never started.
func TestStopPrefillWithoutPrefill(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("never registered", func(t *testing.T) {
		h := newLifecycleTestIndex(t, nil, 0)
		waitFor(t, stopPostStartupAsync(h), 10*time.Second, "stopPostStartup hung with no prefill registered")
	})

	t.Run("prefill skipped as already done", func(t *testing.T) {
		c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
		h := newLifecycleTestIndex(t, c, 10)
		h.cachePrefilled.Store(true)

		h.prefillCache(context.Background())
		waitFor(t, stopPostStartupAsync(h), 10*time.Second, "stopPostStartup hung after a skipped prefill")
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
	waitFor(t, stopPostStartupAsync(h), 10*time.Second,
		"canceling the caller context did not stop the prefill")
}

// TestPrefillRefusedAfterStop: once stopPostStartup has run, no later prefill may start.
// Drop reaches this directly — it never cancels shutdownCtx, so a PostStartup arriving
// after it has nothing in its own context saying the index is gone.
func TestPrefillRefusedAfterStop(t *testing.T) {
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
	h.waitForCachePrefill = true // a started prefill would run to completion inline
	require.True(t, h.useParallelPrefill(), "test must exercise the scan path")

	h.stopPostStartup() // stands in for Drop, which does not cancel shutdownCtx
	h.prefillCache(context.Background())

	require.Equal(t, int64(0), c.CountVectors(),
		"a prefill started after stopPostStartup would read a store the caller is closing")
	require.False(t, h.cachePrefilled.Load(), "a refused prefill must not claim to have run")
}

// TestPrefillRegistrationSerializedWithStop: the check for "already stopped" and the
// registration that follows must be one atomic step. A stopPostStartup landing between
// them observes an empty WaitGroup and returns while the prefill goes on to start, so
// this drives the two concurrently and requires that they cannot both win.
func TestPrefillRegistrationSerializedWithStop(t *testing.T) {
	const n = 100
	logger, _ := test.NewNullLogger()

	for i := 0; i < 200; i++ {
		c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
		c.Grow(n)
		// no store: a prefill that does start takes the serial by-id path and errors
		// on the first cache miss, so it cannot mask a refusal by filling the cache
		h := newLifecycleTestIndex(t, c, n)
		h.waitForCachePrefill = false

		var wg sync.WaitGroup
		wg.Add(2)
		enterrors.GoWrapper(func() { defer wg.Done(); h.prefillCache(context.Background()) }, logger)
		enterrors.GoWrapper(func() { defer wg.Done(); h.stopPostStartup() }, logger)
		wg.Wait()

		// Either the prefill registered first — stopPostStartup then cancelled and waited
		// for it — or it was refused and never ran. What must never happen is
		// stopPostStartup returning while a prefill is still touching the cache.
		h.lifecycleMu.Lock()
		require.True(t, h.tornDown, "stopPostStartup must leave registration closed")
		h.lifecycleMu.Unlock()

		h.stopPostStartup() // idempotent
		require.Equal(t, int64(0), c.CountVectors())
	}
}

// TestPostStartupRefusedAfterStop: the refusal has to cover the whole entry point,
// not just the prefill. Drop unregisters the commit log's maintenance callbacks and
// then removes its directory, so a late InitMaintenance re-registers switch_logs and
// maintain_logs against files that are gone — and Drop has already run, so nothing
// unregisters them again.
func TestPostStartupRefusedAfterStop(t *testing.T) {
	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)

	h := newLifecycleTestIndex(t, c, 10)
	cl := &countingCommitLogger{}
	h.commitLog = cl

	h.PostStartup(context.Background())
	require.Equal(t, int32(1), cl.initMaintenanceCall.Load(), "a live index must register maintenance")

	h.stopPostStartup() // stands in for Drop, which does not cancel shutdownCtx
	h.PostStartup(context.Background())

	require.Equal(t, int32(1), cl.initMaintenanceCall.Load(),
		"maintenance was re-registered after teardown; Drop has already removed the commit log directory")
}
