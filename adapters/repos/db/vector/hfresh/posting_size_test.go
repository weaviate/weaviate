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

func TestPostingSizes(t *testing.T) {
	ctx := t.Context()

	t.Run("Set creates and updates a posting size", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		size, err := sizes.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, size)
		require.EqualValues(t, 1, sizes.Count())
		require.EqualValues(t, 10, sizes.totalSize.Load())

		persisted, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, persisted)

		err = sizes.Set(ctx, 42, 20)
		require.NoError(t, err)

		size, err = sizes.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 20, size)
		require.EqualValues(t, 1, sizes.Count())
		require.EqualValues(t, 20, sizes.totalSize.Load())

		persisted, err = sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 20, persisted)
	})

	t.Run("Set zero removes an existing posting size", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		err = sizes.Set(ctx, 42, 0)
		require.NoError(t, err)

		size, err := sizes.Get(ctx, 42)
		require.NoError(t, err)
		require.Zero(t, size)
		require.Zero(t, sizes.Count())
		require.Zero(t, sizes.totalSize.Load())

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("Set persists after FastIncrement changed memory only", func(t *testing.T) {
		sizes := makePostingSizes(t)

		size := sizes.FastIncrement(42)
		require.EqualValues(t, 1, size)

		_, err := sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)

		err = sizes.Set(ctx, 42, 1)
		require.NoError(t, err)

		persisted, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 1, persisted)
		require.EqualValues(t, 1, sizes.Count())
		require.EqualValues(t, 1, sizes.totalSize.Load())
	})

	t.Run("Set unchanged persisted size keeps value", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		err = sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		persisted, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, persisted)
		require.EqualValues(t, 1, sizes.Count())
		require.EqualValues(t, 10, sizes.totalSize.Load())
	})

	t.Run("Set zero on missing posting is a no-op", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 0)
		require.NoError(t, err)

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.Zero(t, sizes.Count())
		require.Zero(t, sizes.totalSize.Load())
	})

	t.Run("Set zero deletes stale persisted size when memory is already empty", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.store.Set(ctx, 42, 10)
		require.NoError(t, err)

		err = sizes.Set(ctx, 42, 0)
		require.NoError(t, err)

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.Zero(t, sizes.Count())
		require.Zero(t, sizes.totalSize.Load())
	})

	t.Run("Set rejects negative sizes without mutating memory or disk", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, -1)
		require.ErrorContains(t, err, "size cannot be negative")

		size, err := sizes.Get(ctx, 42)
		require.NoError(t, err)
		require.Zero(t, size)
		require.Zero(t, sizes.Count())
		require.Zero(t, sizes.totalSize.Load())

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("FastIncrement tracks memory without persisting", func(t *testing.T) {
		sizes := makePostingSizes(t)

		size := sizes.FastIncrement(42)
		require.EqualValues(t, 1, size)
		size = sizes.FastIncrement(42)
		require.EqualValues(t, 2, size)

		size, err := sizes.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 2, size)
		require.EqualValues(t, 1, sizes.Count())
		require.EqualValues(t, 2, sizes.totalSize.Load())

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("FastIncrement continues from a persisted size in memory", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		size := sizes.FastIncrement(42)
		require.EqualValues(t, 11, size)

		persisted, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, persisted)

		err = sizes.Set(ctx, 42, 11)
		require.NoError(t, err)

		persisted, err = sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 11, persisted)
		require.EqualValues(t, 1, sizes.Count())
		require.EqualValues(t, 11, sizes.totalSize.Load())
	})

	t.Run("Set handles large posting IDs across pages", func(t *testing.T) {
		sizes := makePostingSizes(t)
		postingID := uint64(64*1024*3 + 17)

		err := sizes.Set(ctx, postingID, 33)
		require.NoError(t, err)

		size, err := sizes.Get(ctx, postingID)
		require.NoError(t, err)
		require.EqualValues(t, 33, size)

		persisted, err := sizes.store.Get(ctx, postingID)
		require.NoError(t, err)
		require.EqualValues(t, 33, persisted)
	})

	t.Run("Restore loads persisted non-zero sizes", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.store.Set(ctx, 1, 10)
		require.NoError(t, err)
		err = sizes.store.Set(ctx, 2, 0)
		require.NoError(t, err)
		err = sizes.store.Set(ctx, 3, 20)
		require.NoError(t, err)

		restored := NewPostingSizes(sizes.store.bucket, NewMetrics(nil, "n/a", "n/a"))
		err = restored.Restore(ctx)
		require.NoError(t, err)

		size, err := restored.Get(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 10, size)

		size, err = restored.Get(ctx, 2)
		require.NoError(t, err)
		require.Zero(t, size)

		size, err = restored.Get(ctx, 3)
		require.NoError(t, err)
		require.EqualValues(t, 20, size)

		require.EqualValues(t, 2, restored.Count())
		require.EqualValues(t, 30, restored.totalSize.Load())
	})
}

func TestPostingSizesStore(t *testing.T) {
	ctx := t.Context()

	t.Run("Get Set Delete round trip", func(t *testing.T) {
		sizes := makePostingSizes(t)

		_, err := sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)

		err = sizes.store.Set(ctx, 42, 10)
		require.NoError(t, err)

		size, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, size)

		err = sizes.store.Delete(ctx, 42)
		require.NoError(t, err)

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("Iter returns only entries for its prefix", func(t *testing.T) {
		sizes := makePostingSizes(t)
		otherStore := NewPostingSizesStore(sizes.store.bucket, []byte{sharedBucketVersionV1, 99})

		err := sizes.store.Set(ctx, 1, 10)
		require.NoError(t, err)
		err = sizes.store.Set(ctx, 2, 20)
		require.NoError(t, err)
		err = otherStore.Set(ctx, 3, 30)
		require.NoError(t, err)

		seen := map[uint64]uint32{}
		err = sizes.store.Iter(ctx, func(postingID uint64, size uint32) error {
			seen[postingID] = size
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, map[uint64]uint32{
			1: 10,
			2: 20,
		}, seen)
	})
}
