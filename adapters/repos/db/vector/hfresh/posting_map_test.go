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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func makePostingMetadataStore(t *testing.T) *PostingMap {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return NewPostingMap(bucket, makeTestMetrics())
}

func makeVectors(n, dims int) []Vector {
	vectors, _ := testinghelpers.RandomVecsFixedSeed(n, 0, dims)
	result := make([]Vector, n)

	quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dims, 42, distancer.NewL2SquaredProvider())

	for i := 0; i < n; i++ {
		compressed := quantizer.CompressedBytes(quantizer.Encode(vectors[i]))
		result[i] = NewVector(uint64(i+1), 1, compressed)
	}
	return result
}

func TestPostingMetadataStore(t *testing.T) {
	ctx := t.Context()

	t.Run("Get on empty store", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		m, release, err := store.Get(ctx, 42)
		require.Equal(t, ErrPostingNotFound, err)
		require.Nil(t, m)
		release()
	})

	t.Run("SetVectorIDs and Get", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		posting := Posting(makeVectors(10, 16))
		err := store.SetVectorIDs(ctx, 42, posting)
		require.NoError(t, err)

		m, release, err := store.Get(ctx, 42)
		require.NoError(t, err)
		for i, v := range posting {
			require.Equal(t, v.ID(), m.Vectors[i])
		}
		require.EqualValues(t, 0, m.Version)
		release()

		count, err := store.CountVectorIDs(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)
	})

	t.Run("SetVersion and GetVersion", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		err := store.SetVersion(ctx, 42, 5)
		require.NoError(t, err)

		m, release, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 5, m.Version)
		release()

		err = store.SetVersion(ctx, 42, 10)
		require.NoError(t, err)

		v, err := store.GetVersion(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, v)
	})

	t.Run("SetVectorIDs doesn't overwrite version", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		posting := Posting(makeVectors(10, 16))

		err := store.SetVersion(ctx, 42, 5)
		require.NoError(t, err)
		err = store.SetVectorIDs(ctx, 42, posting)
		require.NoError(t, err)

		m, release, err := store.Get(ctx, 42)
		require.NoError(t, err)
		for i, v := range posting {
			require.Equal(t, v.ID(), m.Vectors[i])
		}
		require.EqualValues(t, 5, m.Version)
		release()
	})

	t.Run("SetVersion doesn't overwrite vector IDs", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		posting := Posting(makeVectors(10, 16))

		err := store.SetVectorIDs(ctx, 42, posting)
		require.NoError(t, err)
		err = store.SetVersion(ctx, 42, 5)
		require.NoError(t, err)

		m, release, err := store.Get(ctx, 42)
		require.NoError(t, err)
		for i, v := range posting {
			require.Equal(t, v.ID(), m.Vectors[i])
		}
		require.EqualValues(t, 5, m.Version)
		release()
	})

	t.Run("CountVectorIDs on non-existing posting", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		count, err := store.CountVectorIDs(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 0, count)
	})

	t.Run("GetVersion on non-existing posting", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		v, err := store.GetVersion(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 0, v)
	})

	t.Run("AddVectorID", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		count, err := store.AddVectorID(ctx, 42, 100)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)

		count, err = store.AddVectorID(ctx, 42, 200)
		require.NoError(t, err)
		require.EqualValues(t, 2, count)

		m, release, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, []uint64{100, 200}, m.Vectors)
		release()
	})
}
