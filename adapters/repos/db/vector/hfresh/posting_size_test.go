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
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func makePostingSizes(t *testing.T) *PostingSizes {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a"))
}

func makePostingSizesWithBucket(t *testing.T) (*PostingSizes, *lsmkv.Bucket) {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a")), bucket
}

func TestPostingSizes(t *testing.T) {
	ctx := t.Context()

	t.Run("Get on empty", func(t *testing.T) {
		ps := makePostingSizes(t)

		size, err := ps.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.EqualValues(t, 0, size)

		require.EqualValues(t, 0, ps.Count())
	})

	t.Run("Get on a high posting ID returns ErrPostingNotFound", func(t *testing.T) {
		ps := makePostingSizes(t)

		size, err := ps.Get(ctx, 1_000_000)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.EqualValues(t, 0, size)
	})

	t.Run("Increment creates a new entry", func(t *testing.T) {
		ps := makePostingSizes(t)

		size, err := ps.Increment(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 1, size)

		got, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 1, got)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Increment increases an existing entry", func(t *testing.T) {
		ps := makePostingSizes(t)

		for i := 1; i <= 10; i++ {
			size, err := ps.Increment(ctx, 42)
			require.NoError(t, err)
			require.EqualValues(t, i, size)
		}

		got, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, got)

		// only one posting created
		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Increment on multiple postings", func(t *testing.T) {
		ps := makePostingSizes(t)

		_, err := ps.Increment(ctx, 1)
		require.NoError(t, err)
		_, err = ps.Increment(ctx, 2)
		require.NoError(t, err)
		_, err = ps.Increment(ctx, 100)
		require.NoError(t, err)
		_, err = ps.Increment(ctx, 100)
		require.NoError(t, err)

		s, err := ps.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 1, s)

		s, err = ps.Get(ctx, 2)
		require.NoError(t, err)
		require.EqualValues(t, 1, s)

		s, err = ps.Get(ctx, 100)
		require.NoError(t, err)
		require.EqualValues(t, 2, s)

		require.EqualValues(t, 3, ps.Count())
	})

	t.Run("Set new posting to non-zero", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 5)
		require.NoError(t, err)

		got, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 5, got)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Set existing posting overrides size without changing count", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 5)
		require.NoError(t, err)
		require.EqualValues(t, 1, ps.Count())

		err = ps.Set(ctx, 42, 100)
		require.NoError(t, err)

		got, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 100, got)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Set existing posting to zero decrements count", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 5)
		require.NoError(t, err)
		err = ps.Set(ctx, 43, 7)
		require.NoError(t, err)
		require.EqualValues(t, 2, ps.Count())

		err = ps.Set(ctx, 42, 0)
		require.NoError(t, err)

		// after setting to 0, Get should report ErrPostingNotFound
		_, err = ps.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Set new posting to zero does not change count", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 0)
		require.NoError(t, err)

		_, err = ps.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)

		require.EqualValues(t, 0, ps.Count())
	})

	t.Run("Set zero on already-zero posting does not change count", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 5)
		require.NoError(t, err)
		err = ps.Set(ctx, 42, 0)
		require.NoError(t, err)
		require.EqualValues(t, 0, ps.Count())

		err = ps.Set(ctx, 42, 0)
		require.NoError(t, err)
		require.EqualValues(t, 0, ps.Count())
	})

	t.Run("Set then increment", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 5)
		require.NoError(t, err)

		size, err := ps.Increment(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 6, size)

		got, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 6, got)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Set negative size returns an error", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative")

		require.EqualValues(t, 0, ps.Count())

		_, err = ps.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("Set negative size on existing posting does not change state", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Set(ctx, 42, 5)
		require.NoError(t, err)

		err = ps.Set(ctx, 42, -10)
		require.Error(t, err)

		got, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 5, got)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("Set persists size to the store", func(t *testing.T) {
		ps, bucket := makePostingSizesWithBucket(t)

		err := ps.Set(ctx, 42, 1234)
		require.NoError(t, err)

		// read it directly from the store
		store := NewPostingSizesStore(bucket, postingSizesBucketPrefix)
		size, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 1234, size)
	})

	t.Run("Increment persists size to the store", func(t *testing.T) {
		ps, bucket := makePostingSizesWithBucket(t)

		_, err := ps.Increment(ctx, 42)
		require.NoError(t, err)
		_, err = ps.Increment(ctx, 42)
		require.NoError(t, err)
		_, err = ps.Increment(ctx, 42)
		require.NoError(t, err)

		store := NewPostingSizesStore(bucket, postingSizesBucketPrefix)
		size, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 3, size)
	})

	t.Run("Restore loads sizes from the store", func(t *testing.T) {
		ps, bucket := makePostingSizesWithBucket(t)

		// populate via Set (which writes to disk)
		err := ps.Set(ctx, 1, 10)
		require.NoError(t, err)
		err = ps.Set(ctx, 42, 100)
		require.NoError(t, err)
		err = ps.Set(ctx, 1000, 200)
		require.NoError(t, err)

		// create a fresh PostingSizes instance pointing at the same bucket
		fresh := NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a"))
		require.EqualValues(t, 0, fresh.Count())

		err = fresh.Restore(ctx)
		require.NoError(t, err)

		require.EqualValues(t, 3, fresh.Count())

		s, err := fresh.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 10, s)

		s, err = fresh.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 100, s)

		s, err = fresh.Get(ctx, 1000)
		require.NoError(t, err)
		require.EqualValues(t, 200, s)
	})

	t.Run("Restore on empty store yields zero entries", func(t *testing.T) {
		ps := makePostingSizes(t)

		err := ps.Restore(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 0, ps.Count())
	})

	t.Run("Restore skips zero-sized entries when counting", func(t *testing.T) {
		ps, bucket := makePostingSizesWithBucket(t)

		err := ps.Set(ctx, 1, 5)
		require.NoError(t, err)
		err = ps.Set(ctx, 2, 0)
		require.NoError(t, err)
		err = ps.Set(ctx, 3, 7)
		require.NoError(t, err)

		fresh := NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a"))
		err = fresh.Restore(ctx)
		require.NoError(t, err)

		// only postings with size > 0 should be counted
		require.EqualValues(t, 2, fresh.Count())

		s, err := fresh.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 5, s)

		_, err = fresh.Get(ctx, 2)
		require.ErrorIs(t, err, ErrPostingNotFound)

		s, err = fresh.Get(ctx, 3)
		require.NoError(t, err)
		require.EqualValues(t, 7, s)
	})

	t.Run("Restore with cancelled context returns error", func(t *testing.T) {
		ps, bucket := makePostingSizesWithBucket(t)

		err := ps.Set(ctx, 1, 5)
		require.NoError(t, err)

		fresh := NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a"))
		cctx, cancel := context.WithCancel(ctx)
		cancel()

		err = fresh.Restore(cctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Posting ID 0 is supported", func(t *testing.T) {
		ps := makePostingSizes(t)

		size, err := ps.Increment(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, size)

		got, err := ps.Get(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, got)

		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("large posting ID within capacity", func(t *testing.T) {
		ps := makePostingSizes(t)

		// 16k pages * 64k entries per page = ~1B addressable entries.
		// Use a value that's safely within capacity but on a high page.
		id := uint64(500_000_000)

		err := ps.Set(ctx, id, 42)
		require.NoError(t, err)

		s, err := ps.Get(ctx, id)
		require.NoError(t, err)
		require.EqualValues(t, 42, s)
	})

	t.Run("Count reflects mixed Set/Increment operations", func(t *testing.T) {
		ps := makePostingSizes(t)

		_, err := ps.Increment(ctx, 1)
		require.NoError(t, err)
		_, err = ps.Increment(ctx, 2)
		require.NoError(t, err)
		err = ps.Set(ctx, 3, 5)
		require.NoError(t, err)
		err = ps.Set(ctx, 4, 0) // doesn't count
		require.NoError(t, err)
		require.EqualValues(t, 3, ps.Count())

		err = ps.Set(ctx, 1, 0) // decrement
		require.NoError(t, err)
		require.EqualValues(t, 2, ps.Count())

		_, err = ps.Increment(ctx, 1) // re-create
		require.NoError(t, err)
		require.EqualValues(t, 3, ps.Count())
	})
}

func TestPostingSizesConcurrency(t *testing.T) {
	ctx := t.Context()

	t.Run("concurrent increments on the same posting", func(t *testing.T) {
		ps := makePostingSizes(t)

		const goroutines = 50
		const incrementsPerGoroutine = 100
		var wg sync.WaitGroup

		for range goroutines {
			wg.Go(func() {
				for range incrementsPerGoroutine {
					_, err := ps.Increment(ctx, 42)
					require.NoError(t, err)
				}
			})
		}

		wg.Wait()

		size, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, goroutines*incrementsPerGoroutine, size)
		require.EqualValues(t, 1, ps.Count())
	})

	t.Run("concurrent increments on different postings", func(t *testing.T) {
		ps := makePostingSizes(t)

		const goroutines = 50
		var wg sync.WaitGroup

		for i := range uint64(goroutines) {
			wg.Go(func() {
				_, err := ps.Increment(ctx, i)
				require.NoError(t, err)
			})
		}

		wg.Wait()

		for i := range uint64(goroutines) {
			size, err := ps.Get(ctx, i)
			require.NoError(t, err)
			require.EqualValues(t, 1, size)
		}
		require.EqualValues(t, goroutines, ps.Count())
	})

	t.Run("concurrent Set on different postings", func(t *testing.T) {
		ps := makePostingSizes(t)

		const goroutines = 50
		var wg sync.WaitGroup

		for i := range uint64(goroutines) {
			wg.Go(func() {
				err := ps.Set(ctx, i, int(i+1))
				require.NoError(t, err)
			})
		}

		wg.Wait()

		require.EqualValues(t, goroutines, ps.Count())
		for i := range uint64(goroutines) {
			size, err := ps.Get(ctx, i)
			require.NoError(t, err)
			require.EqualValues(t, i+1, size)
		}
	})

	t.Run("concurrent Increment and Get", func(t *testing.T) {
		ps := makePostingSizes(t)

		const writers = 10
		const readers = 10
		const ops = 200

		var writersWg sync.WaitGroup
		var readersWg sync.WaitGroup
		var stop atomic.Bool

		for range writers {
			writersWg.Go(func() {
				for range ops {
					_, err := ps.Increment(ctx, 42)
					require.NoError(t, err)
				}
			})
		}

		for range readers {
			readersWg.Go(func() {
				for !stop.Load() {
					// the posting may not exist yet, both outcomes are valid
					_, err := ps.Get(ctx, 42)
					if err != nil {
						require.ErrorIs(t, err, ErrPostingNotFound)
					}
				}
			})
		}

		writersWg.Wait()
		stop.Store(true)
		readersWg.Wait()

		size, err := ps.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, writers*ops, size)
	})
}

func TestPostingSizesStore(t *testing.T) {
	ctx := t.Context()

	makeStore := func(t *testing.T) *PostingSizesStore {
		t.Helper()
		s := testinghelpers.NewDummyStore(t)
		bucket, err := NewSharedBucket(s, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
		require.NoError(t, err)
		return NewPostingSizesStore(bucket, postingSizesBucketPrefix)
	}

	t.Run("Get on empty store returns ErrPostingNotFound", func(t *testing.T) {
		store := makeStore(t)

		size, err := store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.EqualValues(t, 0, size)
	})

	t.Run("Set and Get", func(t *testing.T) {
		store := makeStore(t)

		err := store.Set(ctx, 42, 100)
		require.NoError(t, err)

		size, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 100, size)
	})

	t.Run("Set overwrites existing value", func(t *testing.T) {
		store := makeStore(t)

		err := store.Set(ctx, 42, 100)
		require.NoError(t, err)
		err = store.Set(ctx, 42, 200)
		require.NoError(t, err)

		size, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 200, size)
	})

	t.Run("Set zero is retrievable", func(t *testing.T) {
		store := makeStore(t)

		err := store.Set(ctx, 42, 0)
		require.NoError(t, err)

		size, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 0, size)
	})

	t.Run("Set max uint32", func(t *testing.T) {
		store := makeStore(t)

		err := store.Set(ctx, 42, ^uint32(0))
		require.NoError(t, err)

		size, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, ^uint32(0), size)
	})

	t.Run("multiple posting IDs do not collide", func(t *testing.T) {
		store := makeStore(t)

		err := store.Set(ctx, 0, 1)
		require.NoError(t, err)
		err = store.Set(ctx, 1, 2)
		require.NoError(t, err)
		err = store.Set(ctx, 256, 3)
		require.NoError(t, err)
		err = store.Set(ctx, 65536, 4)
		require.NoError(t, err)
		err = store.Set(ctx, ^uint64(0), 5)
		require.NoError(t, err)

		s, err := store.Get(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, s)

		s, err = store.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 2, s)

		s, err = store.Get(ctx, 256)
		require.NoError(t, err)
		require.EqualValues(t, 3, s)

		s, err = store.Get(ctx, 65536)
		require.NoError(t, err)
		require.EqualValues(t, 4, s)

		s, err = store.Get(ctx, ^uint64(0))
		require.NoError(t, err)
		require.EqualValues(t, 5, s)
	})

	t.Run("Iter on empty store does not invoke fn", func(t *testing.T) {
		store := makeStore(t)

		var calls int
		err := store.Iter(ctx, func(uint64, uint32) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, calls)
	})

	t.Run("Iter visits every entry exactly once", func(t *testing.T) {
		store := makeStore(t)

		expected := map[uint64]uint32{
			1:        10,
			42:       100,
			1000:     200,
			65536:    300,
			16777216: 400,
		}
		for id, size := range expected {
			err := store.Set(ctx, id, size)
			require.NoError(t, err)
		}

		seen := make(map[uint64]uint32)
		err := store.Iter(ctx, func(id uint64, size uint32) error {
			seen[id] = size
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, expected, seen)
	})

	t.Run("Iter respects fn errors", func(t *testing.T) {
		store := makeStore(t)

		for i := uint64(1); i <= 10; i++ {
			err := store.Set(ctx, i, uint32(i))
			require.NoError(t, err)
		}

		errSentinel := errors.New("stop")
		var calls int
		err := store.Iter(ctx, func(uint64, uint32) error {
			calls++
			if calls == 3 {
				return errSentinel
			}
			return nil
		})
		require.ErrorIs(t, err, errSentinel)
		require.Equal(t, 3, calls)
	})

	t.Run("Iter respects context cancellation", func(t *testing.T) {
		store := makeStore(t)

		// Insert >1000 entries so the per-1000 cancellation check fires.
		for i := uint64(1); i <= 1500; i++ {
			err := store.Set(ctx, i, uint32(i))
			require.NoError(t, err)
		}

		cctx, cancel := context.WithCancel(ctx)
		cancel()

		err := store.Iter(cctx, func(uint64, uint32) error {
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Iter only returns keys with the configured prefix", func(t *testing.T) {
		s := testinghelpers.NewDummyStore(t)
		bucket, err := NewSharedBucket(s, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
		require.NoError(t, err)

		// the posting sizes store
		sizesStore := NewPostingSizesStore(bucket, postingSizesBucketPrefix)

		// another store using the same shared bucket but a different prefix
		other := NewPostingSizesStore(bucket, indexMetadataBucketPrefix)

		err = sizesStore.Set(ctx, 1, 10)
		require.NoError(t, err)
		err = sizesStore.Set(ctx, 2, 20)
		require.NoError(t, err)

		// inject sibling data under a different prefix
		err = other.Set(ctx, 99, 9999)
		require.NoError(t, err)

		seen := make(map[uint64]uint32)
		err = sizesStore.Iter(ctx, func(id uint64, size uint32) error {
			seen[id] = size
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, map[uint64]uint32{1: 10, 2: 20}, seen)
	})

	t.Run("different prefixes do not collide on the same posting ID", func(t *testing.T) {
		s := testinghelpers.NewDummyStore(t)
		bucket, err := NewSharedBucket(s, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
		require.NoError(t, err)

		a := NewPostingSizesStore(bucket, postingSizesBucketPrefix)
		b := NewPostingSizesStore(bucket, indexMetadataBucketPrefix)

		err = a.Set(ctx, 42, 1)
		require.NoError(t, err)
		err = b.Set(ctx, 42, 2)
		require.NoError(t, err)

		s1, err := a.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 1, s1)

		s2, err := b.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 2, s2)
	})
}
