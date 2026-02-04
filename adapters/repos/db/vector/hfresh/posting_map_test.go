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
		m, err := store.Get(ctx, 42)
		require.Equal(t, ErrPostingNotFound, err)
		require.Nil(t, m)
	})

	t.Run("SetVectorIDs and Get", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		posting := Posting(makeVectors(10, 16))
		err := store.SetVectorIDs(ctx, 42, posting)
		require.NoError(t, err)

		m, err := store.Get(ctx, 42)
		require.NoError(t, err)
		for i, v := range posting {
			require.Equal(t, v.ID(), m.vectors[i])
			require.Equal(t, v.Version(), m.version[i])
		}

		count, err := store.CountVectorIDs(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)

		store.cache.Invalidate(42)

		m, err = store.Get(ctx, 42)
		require.NoError(t, err)
		for i, v := range posting {
			require.Equal(t, v.ID(), m.vectors[i])
			require.Equal(t, v.Version(), m.version[i])
		}
	})

	t.Run("CountVectorIDs on non-existing posting", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		count, err := store.CountVectorIDs(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 0, count)
	})

	t.Run("FastAddVectorID", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		count, err := store.FastAddVectorID(ctx, 42, 100, 1, 10)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)

		count, err = store.FastAddVectorID(ctx, 42, 200, 1, 10)
		require.NoError(t, err)
		require.EqualValues(t, 2, count)

		m, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, uint64(100), m.vectors[0])
		require.Equal(t, VectorVersion(1), m.version[0])
		require.Equal(t, uint64(200), m.vectors[1])
		require.Equal(t, VectorVersion(1), m.version[1])
	})
}
