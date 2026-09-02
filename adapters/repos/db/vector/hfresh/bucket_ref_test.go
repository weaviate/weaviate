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

package hfresh

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// Resolving a bucket by name is not enough: teardown deregisters the bucket
// and then frees its mmap'd segments, waiting only for pins taken via
// [lsmkv.Store.AcquireBucketForRead]. An operation holding an unpinned pointer
// reads through freed memory. acquire must pin, so a concurrent teardown
// blocks until the operation releases.
func TestBucketRefAcquirePinsAgainstShutdown(t *testing.T) {
	ctx := context.Background()

	store := testinghelpers.NewDummyStore(t)
	cfg := StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions}
	shared, err := NewSharedBucket(store, "pin", cfg)
	require.NoError(t, err)

	bucket, rawRelease, err := shared.acquire()
	require.NoError(t, err)
	require.NotNil(t, bucket)

	// Release exactly once on every exit path, or a parked drain wedges the test.
	var once sync.Once
	release := func() { once.Do(rawRelease) }
	defer release()

	var done atomic.Bool
	errCh := make(chan error, 1)
	go func() {
		errCh <- store.Shutdown(ctx)
		done.Store(true)
	}()

	time.Sleep(100 * time.Millisecond)
	require.False(t, done.Load(),
		"store shutdown completed while an acquired bucket was still in use: the bucket was not pinned")

	release()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("store shutdown did not complete after the pin was released")
	}
}

// The pin must span the whole operation, cursor iteration included: a cursor
// walks the segment list, so a teardown that frees segments mid-iteration is
// exactly the use-after-free the pin exists to prevent.
func TestHFreshIterationPinsBucketForItsWholeDuration(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		seed func(t *testing.T, shared bucketRef)
		iter func(shared bucketRef, onEntry func()) error
	}{
		{
			name: "posting map iterate",
			seed: func(t *testing.T, shared bucketRef) {
				store := NewPostingMapStore(shared, postingMapBucketPrefixV2)
				for id := range uint64(3) {
					require.NoError(t, store.Set(ctx, id, NewPackedPostingMetadata([]uint64{id})))
				}
			},
			iter: func(shared bucketRef, onEntry func()) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).
					Iter(ctx, func(uint64, PackedPostingMetadata) error {
						onEntry()
						return nil
					})
			},
		},
		{
			name: "posting size iterate",
			seed: func(t *testing.T, shared bucketRef) {
				store := NewPostingSizesStore(shared, postingSizesBucketPrefix)
				for id := range uint64(3) {
					require.NoError(t, store.Set(ctx, id, 1))
				}
			},
			iter: func(shared bucketRef, onEntry func()) error {
				return NewPostingSizesStore(shared, postingSizesBucketPrefix).
					Iter(ctx, func(uint64, uint32) error {
						onEntry()
						return nil
					})
			},
		},
		{
			name: "vector version iterate",
			seed: func(t *testing.T, shared bucketRef) {
				store := NewVersionStore(shared)
				for id := range uint64(3) {
					require.NoError(t, store.Set(ctx, id, 1))
				}
			},
			iter: func(shared bucketRef, onEntry func()) error {
				return NewVersionStore(shared).IterateAll(func(uint64, VectorVersion) bool {
					onEntry()
					return true
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			cfg := StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions}
			shared, err := NewSharedBucket(store, "pin", cfg)
			require.NoError(t, err)
			test.seed(t, shared)

			// Park the iteration inside its callback, mid-cursor.
			entered := make(chan struct{})
			unblock := make(chan struct{})
			var enteredOnce sync.Once
			iterErrCh := make(chan error, 1)
			go func() {
				iterErrCh <- test.iter(shared, func() {
					enteredOnce.Do(func() {
						close(entered)
						<-unblock
					})
				})
			}()

			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("iteration never reached its callback")
			}

			var done atomic.Bool
			shutdownErrCh := make(chan error, 1)
			go func() {
				shutdownErrCh <- store.Shutdown(ctx)
				done.Store(true)
			}()

			time.Sleep(100 * time.Millisecond)
			shutdownRaced := done.Load()
			close(unblock)

			require.NoError(t, <-iterErrCh)
			select {
			case err := <-shutdownErrCh:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("store shutdown did not complete after iteration finished")
			}

			require.False(t, shutdownRaced,
				"store shutdown freed segments while a cursor was still iterating them")
		})
	}
}
