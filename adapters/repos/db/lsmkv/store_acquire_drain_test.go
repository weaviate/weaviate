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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func newTestStoreForDrain(t *testing.T) *Store {
	t.Helper()
	dirName := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(dirName) })
	logger, _ := test.NewNullLogger()

	store, err := New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroup("classCompactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("classCompactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	return store
}

// TestAcquireBucketForRead_PinAndRelease verifies the basic contract:
// AcquireBucketForRead returns the registered bucket and a working release,
// and returns (nil, no-op) for an unknown name.
func TestAcquireBucketForRead_PinAndRelease(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, "b1", WithStrategy(StrategyReplace)))

	b, release := store.AcquireBucketForRead("b1")
	require.NotNil(t, b)
	require.Same(t, store.Bucket("b1"), b)
	release() // must not panic

	miss, releaseMiss := store.AcquireBucketForRead("does-not-exist")
	require.Nil(t, miss)
	require.NotNil(t, releaseMiss)
	releaseMiss() // no-op must not panic
}

// TestShutdown_DrainsInFlightPin is the lsmkv-level proof that
// Bucket.Shutdown drains in-flight read pins AND that Store.Shutdown does so
// WITHOUT holding bucketAccessLock across the drain (so a concurrent query
// that still needs Store.Bucket cannot deadlock against it).
//
// It pins a bucket via AcquireBucketForRead, then runs Store.Shutdown in a
// goroutine. The Shutdown must BLOCK on the pin (proven: it does not complete
// while the pin is held), and a concurrent Store.Bucket lookup must NOT block
// — it returns nil because Store.Shutdown clears the registry before draining
// (the de-inversion fix). Once the pin releases, Store.Shutdown completes.
func TestShutdown_DrainsInFlightPin(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "pinned", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "other", WithStrategy(StrategyReplace)))

	// Pin "pinned" for the duration of a simulated query.
	b, release := store.AcquireBucketForRead("pinned")
	require.NotNil(t, b)

	var shutdownDone atomic.Bool
	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- store.Shutdown(ctx)
		shutdownDone.Store(true)
	}()

	// Give Shutdown time to reach the drain and confirm it is BLOCKED on the
	// pin (has not completed while we still hold it). The drain is what makes
	// the use-after-free impossible.
	time.Sleep(100 * time.Millisecond)
	require.False(t, shutdownDone.Load(),
		"DRAIN VIOLATED: Store.Shutdown completed while a read pin was still held")

	// While Shutdown is parked in the drain, a concurrent query's Store.Bucket
	// lookup must NOT block. This is the de-inversion guarantee: Store.Shutdown
	// clears the registry BEFORE draining and releases bucketAccessLock, so the
	// lookup completes (returning nil for the now-unregistered name) instead of
	// blocking on bucketAccessLock held across the drain — the deadlock the fix
	// prevents. A hard timeout turns a hang into a visible failure.
	bucketLookupCh := make(chan *Bucket, 1)
	go func() { bucketLookupCh <- store.Bucket("other") }()
	select {
	case got := <-bucketLookupCh:
		require.Nil(t, got,
			"after Store.Shutdown cleared the registry, Store.Bucket must return nil")
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("DEADLOCK: Store.Bucket blocked while Store.Shutdown drained a pinned bucket under bucketAccessLock")
	}

	// Release the pin; Shutdown must now complete promptly.
	release()

	select {
	case err := <-shutdownErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Store.Shutdown did not complete after the pin was released")
	}
}
