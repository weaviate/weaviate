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
	"sync"
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

// TestShutdown_DrainsInFlightPin: Store.Shutdown must block on an in-flight
// read pin WITHOUT holding bucketAccessLock across the drain — a concurrent
// Store.Bucket lookup returns nil instead of deadlocking.
func TestShutdown_DrainsInFlightPin(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "pinned", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "other", WithStrategy(StrategyReplace)))

	// Once-wrapped + deferred so a FailNow still releases the pin exactly
	// once (the raw release is a bare RUnlock).
	b, rawRelease := store.AcquireBucketForRead("pinned")
	require.NotNil(t, b)
	var relOnce sync.Once
	release := func() { relOnce.Do(rawRelease) }
	defer release()

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

	// While Shutdown is parked in the drain, a concurrent Store.Bucket must
	// NOT block; the hard timeout turns a hang into a visible failure.
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

	release()

	select {
	case err := <-shutdownErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Store.Shutdown did not complete after the pin was released")
	}
}

// TestOnlineTeardown_DoesNotWedgeStoreOnPinnedBucket: ShutdownBucket and
// ReplaceBuckets used to hold bucketAccessLock across the drain, wedging
// the whole store against a pinned query that still needed Store.Bucket.
func TestOnlineTeardown_DoesNotWedgeStoreOnPinnedBucket(t *testing.T) {
	cases := []struct {
		name     string
		teardown func(ctx context.Context, store *Store) error
		// registryReflects reports whether the teardown's registry phase
		// completed: deregistered for ShutdownBucket, swapped to the
		// replacement for ReplaceBuckets.
		registryReflects func(got, pinned, replacement *Bucket) bool
	}{
		{
			name: "ShutdownBucket",
			teardown: func(ctx context.Context, store *Store) error {
				return store.ShutdownBucket(ctx, "pinned")
			},
			registryReflects: func(got, _, _ *Bucket) bool { return got == nil },
		},
		{
			name: "ReplaceBuckets",
			teardown: func(ctx context.Context, store *Store) error {
				return store.ReplaceBuckets(ctx, "pinned", "replacement")
			},
			registryReflects: func(got, _, replacement *Bucket) bool { return got == replacement },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStoreForDrain(t)
			t.Cleanup(func() { _ = store.Shutdown(ctx) })

			require.NoError(t, store.CreateOrLoadBucket(ctx, "pinned", WithStrategy(StrategyReplace)))
			require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))
			require.NoError(t, store.CreateOrLoadBucket(ctx, "other", WithStrategy(StrategyReplace)))
			replacement := store.Bucket("replacement")

			pinned, release := store.AcquireBucketForRead("pinned")
			require.NotNil(t, pinned)
			// Must release on every exit path (incl. FailNow) or the parked
			// drain wedges the Cleanup Store.Shutdown; Once because the raw
			// release is a bare RUnlock.
			var relOnce sync.Once
			releaseOnce := func() { relOnce.Do(release) }
			defer releaseOnce()

			teardownErrCh := make(chan error, 1)
			go func() { teardownErrCh <- tc.teardown(ctx, store) }()

			// While the teardown is parked in the drain, an unrelated lookup
			// must complete promptly — pre-fix it blocked on bucketAccessLock.
			lookupCh := make(chan *Bucket, 1)
			go func() { lookupCh <- store.Bucket("other") }()
			select {
			case got := <-lookupCh:
				require.NotNil(t, got, "unrelated bucket must remain fetchable during the drain")
			case <-time.After(5 * time.Second):
				t.Fatal("DEADLOCK: Store.Bucket blocked while the teardown drained a pinned bucket under bucketAccessLock")
			}

			// The registry phase must complete while the pin is held;
			// Eventually because it races this check.
			require.Eventually(t, func() bool {
				return tc.registryReflects(store.Bucket("pinned"), pinned, replacement)
			}, 5*time.Second, time.Millisecond,
				"registry must reflect the teardown while the drain is still parked")

			select {
			case err := <-teardownErrCh:
				t.Fatalf("teardown returned before the pin was released: %v", err)
			default:
			}

			releaseOnce()
			select {
			case err := <-teardownErrCh:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("teardown did not complete after the pin was released")
			}
		})
	}
}

// TestReplaceBuckets_ConcurrentWriteSurvives pins the lost-write window: a
// by-name write landing after the map swap but before ReplaceBuckets' tail
// completed went to a memtable the tail discarded unflushed. The freeze
// (flushLock held across the whole move) makes the writer block instead.
func TestReplaceBuckets_ConcurrentWriteSurvives(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))

	key, val := []byte("mid-swap-key"), []byte("mid-swap-val")
	putDone := make(chan error, 1)

	// Reproduce ReplaceBuckets' body test-side (white-box, package lsmkv) so the
	// mid-swap window — after the registry swap, before the tail completes — is
	// reachable through the REAL entry points, no production hook. The freeze
	// holds replacementBucket.flushLock across the whole move; that is the fix
	// under test, so a by-name Put landing mid-swap must block on it instead of
	// writing into the memtable the tail discards.
	bucket, replacementBucket, err := store.freezeAndSwapForReplace("target", "replacement")
	require.NoError(t, err)
	// Release on every exit path (Once so the explicit unlock below is a no-op
	// for the defer); otherwise a mid-tail FailNow leaves flushLock held and
	// wedges the Cleanup Shutdown.
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(func() { replacementBucket.flushLock.Unlock() }) }
	defer unlock()

	// Mid-swap window: race a by-name write against the in-progress move.
	go func() {
		b := store.Bucket("target") // resolves to the replacement post-swap
		putDone <- b.Put(key, val)
	}()
	// Pre-fix the Put completes into the doomed memtable (immediate return);
	// post-fix it blocks on the held flushLock and the timeout lets the tail
	// finish first.
	select {
	case err := <-putDone:
		putDone <- err
	case <-time.After(2 * time.Second):
	}

	// Tail of ReplaceBuckets, driven test-side through the same unexported
	// methods the production path uses.
	currBucketDir, newBucketDir, currReplacementBucketDir, newReplacementBucketDir, err := store.replaceBucket(ctx, replacementBucket, "replacement", bucket, "target")
	require.NoError(t, err)
	replacementBucket.dir = newReplacementBucketDir
	mt, err := replacementBucket.createNewActiveMemtable()
	require.NoError(t, err)
	replacementBucket.active = mt
	store.updateBucketDir(bucket, currBucketDir, newBucketDir)
	store.updateBucketDir(replacementBucket, currReplacementBucketDir, newReplacementBucketDir)
	require.NoError(t, os.RemoveAll(newBucketDir))
	unlock() // matches ReplaceBuckets' deferred flushLock release at tail end

	select {
	case err := <-putDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Put did not complete after ReplaceBuckets returned")
	}

	got, err := store.Bucket("target").Get(key)
	require.NoError(t, err)
	require.Equal(t, val, got, "LOST WRITE: value acknowledged during ReplaceBuckets is gone")
}
