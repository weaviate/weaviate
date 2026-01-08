package hfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func makePostingMetadataStore(t *testing.T) *PostingMetadataStore {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return NewPostingMetadataStore(bucket, makeTestMetrics())
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
	})
}
