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

// TestShutdown_DrainsInFlightPin proves Store.Shutdown blocks on an
// in-flight read pin AND does so without holding bucketAccessLock across
// the drain: a concurrent Store.Bucket lookup returns nil (registry cleared
// first) instead of deadlocking. Once the pin releases, Shutdown completes.
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

	// Give Shutdown time to reach the drain; it must be BLOCKED on the pin.
	time.Sleep(100 * time.Millisecond)
	require.False(t, shutdownDone.Load(),
		"DRAIN VIOLATED: Store.Shutdown completed while a read pin was still held")

	// De-inversion guarantee: while Shutdown is parked in the drain, a
	// concurrent Store.Bucket must NOT block (registry cleared, lock
	// released). The hard timeout turns a hang into a visible failure.
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

// TestShutdownBucket_DoesNotWedgeStoreOnPinnedBucket pins the QA-found F1
// deadlock: ShutdownBucket used to hold bucketAccessLock across the
// drain-Shutdown, wedging the whole store against a pinned query that still
// needed Store.Bucket (runtime-reachable via property-index DELETE). With the
// remove-from-map-first protocol, a concurrent lookup completes while the
// drain is parked, and ShutdownBucket finishes once the pin releases.
func TestShutdownBucket_DoesNotWedgeStoreOnPinnedBucket(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, "pinned", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "other", WithStrategy(StrategyReplace)))

	pinned, release := store.AcquireBucketForRead("pinned")
	require.NotNil(t, pinned)

	shutdownErrCh := make(chan error, 1)
	go func() { shutdownErrCh <- store.ShutdownBucket(ctx, "pinned") }()

	// While ShutdownBucket is parked in the drain, an unrelated lookup must
	// complete promptly — pre-fix it blocked on bucketAccessLock forever.
	lookupCh := make(chan *Bucket, 1)
	go func() { lookupCh <- store.Bucket("other") }()
	select {
	case got := <-lookupCh:
		require.NotNil(t, got, "unrelated bucket must remain fetchable during the drain")
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("DEADLOCK: Store.Bucket blocked while ShutdownBucket drained a pinned bucket under bucketAccessLock")
	}

	// The pinned name must already be deregistered (remove-first protocol).
	require.Nil(t, store.Bucket("pinned"))

	select {
	case err := <-shutdownErrCh:
		t.Fatalf("ShutdownBucket returned before the pin was released: %v", err)
	default:
	}

	release()
	select {
	case err := <-shutdownErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("ShutdownBucket did not complete after the pin was released")
	}
}
