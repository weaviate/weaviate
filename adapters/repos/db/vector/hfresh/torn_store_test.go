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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// The tests in this file cover what HFresh does when the buckets under it are
// torn down: the resolve step for a bucket that is already gone, and the pins
// that keep a teardown from landing in the middle of an operation.

// Every operation must report a bucket that is already gone rather than
// working on a stale pointer. This covers the resolve step only — teardown
// completes before the operation starts. The concurrent case, which the
// resolve alone cannot catch, is covered by the pin tests below.
func TestHFreshStoresOnTornDownStore(t *testing.T) {
	ctx := context.Background()

	newStores := func(t *testing.T) (*lsmkv.Store, bucketRef, *PostingStore) {
		t.Helper()

		store := testinghelpers.NewDummyStore(t)
		cfg := StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions}
		shared, err := NewSharedBucket(store, "torn", cfg)
		require.NoError(t, err)
		postings, err := NewPostingStore(store, shared, NewMetrics(nil, "n/a", "n/a"), "torn", cfg)
		require.NoError(t, err)

		// One real write before the teardown. It leaves data a stale read
		// could return, and it caches posting 1's version, so the operations
		// below reach the postings bucket instead of stopping at the version
		// lookup that precedes it.
		var posting Posting
		posting = posting.AddVector(NewVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
		require.NoError(t, postings.Put(ctx, 1, posting))

		return store, shared, postings
	}

	tests := []struct {
		name string
		op   func(shared bucketRef, postings *PostingStore) error
	}{
		{
			name: "posting get",
			op: func(_ bucketRef, p *PostingStore) error {
				_, err := p.Get(ctx, 1)
				return err
			},
		},
		{
			name: "posting put",
			op: func(_ bucketRef, p *PostingStore) error {
				var posting Posting
				posting = posting.AddVector(NewVector(1, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
				return p.Put(ctx, 1, posting)
			},
		},
		{
			name: "posting append",
			op: func(_ bucketRef, p *PostingStore) error {
				return p.Append(ctx, 1, NewVector(2, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
			},
		},
		{
			name: "posting version get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewPostingVersionsStore(shared).Get(ctx, 7)
				return err
			},
		},
		{
			name: "posting version set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingVersionsStore(shared).Set(ctx, 7, 3)
			},
		},
		{
			name: "vector version get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewVersionStore(shared).Get(ctx, 7)
				return err
			},
		},
		{
			name: "vector version set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewVersionStore(shared).Set(ctx, 7, 2)
			},
		},
		{
			name: "vector version iterate",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewVersionStore(shared).IterateAll(func(uint64, VectorVersion) bool { return true })
			},
		},
		{
			name: "posting map get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewPostingMapStore(shared, postingMapBucketPrefixV2).Get(ctx, 7)
				return err
			},
		},
		{
			name: "posting map set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).Set(ctx, 7, PackedPostingMetadata{})
			},
		},
		{
			name: "posting map delete",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).Delete(ctx, 7)
			},
		},
		{
			name: "posting map iterate",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingMapStore(shared, postingMapBucketPrefixV2).
					Iter(ctx, func(uint64, PackedPostingMetadata) error { return nil })
			},
		},
		{
			name: "posting size get",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewPostingSizesStore(shared, postingSizesBucketPrefix).Get(ctx, 7)
				return err
			},
		},
		{
			name: "posting size set",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingSizesStore(shared, postingSizesBucketPrefix).Set(ctx, 7, 3)
			},
		},
		{
			name: "posting size iterate",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewPostingSizesStore(shared, postingSizesBucketPrefix).
					Iter(ctx, func(uint64, uint32) error { return nil })
			},
		},
		{
			name: "index metadata get dimensions",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewIndexMetadataStore(shared).GetDimensions()
				return err
			},
		},
		{
			name: "index metadata set dimensions",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewIndexMetadataStore(shared).SetDimensions(64)
			},
		},
		{
			name: "id sequence load",
			op: func(shared bucketRef, _ *PostingStore) error {
				_, err := NewBucketStore(shared).Load()
				return err
			},
		},
		{
			name: "id sequence store",
			op: func(shared bucketRef, _ *PostingStore) error {
				return NewBucketStore(shared).Store(42)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, shared, postings := newStores(t)
			require.NoError(t, store.Shutdown(ctx))

			require.ErrorIs(t, test.op(shared, postings), lsmkv.ErrBucketNotFound)
		})
	}
}

// A warm cache must not paper over a torn-down bucket. The version read backs
// a subsequent postings read, so answering from cache while the store no
// longer holds the bucket makes the same call succeed or fail depending only
// on what happens to be cached.
func TestPostingVersionsGetOnTornDownStoreWithWarmCache(t *testing.T) {
	ctx := context.Background()

	store := testinghelpers.NewDummyStore(t)
	cfg := StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions}
	shared, err := NewSharedBucket(store, "torn", cfg)
	require.NoError(t, err)

	versions := NewPostingVersionsStore(shared)
	// Set populates the cache, so the Get below never reaches its loader.
	require.NoError(t, versions.Set(ctx, 7, 3))

	require.NoError(t, store.Shutdown(ctx))

	_, err = versions.Get(ctx, 7)
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}

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

// A teardown deregisters the objects bucket while queries are still in flight,
// and Store.Bucket then returns nil by design. Taking a view straight off that
// nil pointer dereferences it and takes the node down, so every path that
// needs the objects bucket has to report it missing instead.
func TestObjectsBucketViewReportsMissingBucket(t *testing.T) {
	ctx := context.Background()

	vectors, _ := testinghelpers.RandomVecs(64, 1, 32)
	index := newSearchTestIndex(t, vectors, nil)

	require.NoError(t, index.store.ShutdownBucket(ctx, helpers.ObjectsBucketLSM))

	_, _, err := index.objectsBucketView()
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}

func TestSearchReportsMissingObjectsBucket(t *testing.T) {
	ctx := context.Background()

	vectors, queries := testinghelpers.RandomVecs(64, 1, 32)
	index := newSearchTestIndex(t, vectors, nil)

	require.NoError(t, index.store.ShutdownBucket(ctx, helpers.ObjectsBucketLSM))

	_, _, err := index.SearchByVector(ctx, queries[0], 10, nil)
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}
