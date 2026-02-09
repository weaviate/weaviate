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

func decodePacked(encoded PackedPostingMetadata) ([]uint64, []VectorVersion) {
	var ids []uint64
	var versions []VectorVersion
	for id, version := range encoded.Iter() {
		ids = append(ids, id)
		versions = append(versions, version)
	}
	return ids, versions
}

func TestPostingMapEncoding(t *testing.T) {
	ctx := t.Context()

	t.Run("empty posting", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		err := store.SetVectorIDs(ctx, 1, Posting{})
		require.NoError(t, err)

		_, err = store.Get(ctx, 1)
		require.Equal(t, ErrPostingNotFound, err)
	})

	t.Run("scheme boundaries", func(t *testing.T) {
		tests := []struct {
			name           string
			vectorIDs      []uint64
			expectedScheme Scheme
			versions       []VectorVersion
		}{
			{
				name:           "2-byte scheme - zero",
				vectorIDs:      []uint64{0},
				expectedScheme: schemeID2Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "2-byte scheme - max",
				vectorIDs:      []uint64{65535},
				expectedScheme: schemeID2Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "3-byte scheme - boundary",
				vectorIDs:      []uint64{65536},
				expectedScheme: schemeID3Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "3-byte scheme - max",
				vectorIDs:      []uint64{16777215},
				expectedScheme: schemeID3Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "4-byte scheme - boundary",
				vectorIDs:      []uint64{16777216},
				expectedScheme: schemeID4Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "4-byte scheme - max",
				vectorIDs:      []uint64{4294967295},
				expectedScheme: schemeID4Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "5-byte scheme - boundary",
				vectorIDs:      []uint64{4294967296},
				expectedScheme: schemeID5Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "5-byte scheme - max",
				vectorIDs:      []uint64{1099511627775},
				expectedScheme: schemeID5Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "8-byte scheme - boundary",
				vectorIDs:      []uint64{1099511627776},
				expectedScheme: schemeID8Byte,
				versions:       []VectorVersion{1},
			},
			{
				name:           "8-byte scheme - max uint64",
				vectorIDs:      []uint64{^uint64(0)},
				expectedScheme: schemeID8Byte,
				versions:       []VectorVersion{1},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				scheme := determineScheme(tt.vectorIDs)
				require.Equal(t, tt.expectedScheme, scheme)

				// Test encode/decode round-trip
				encoded := NewPackedPostingMetadata(tt.vectorIDs, tt.versions)

				decodedIDs, decodedVersions := decodePacked(encoded)
				require.Equal(t, tt.vectorIDs, decodedIDs)
				require.Equal(t, tt.versions, decodedVersions)
			})
		}
	})

	t.Run("round-trip through store with various schemes", func(t *testing.T) {
		tests := []struct {
			name      string
			vectorIDs []uint64
			versions  []VectorVersion
		}{
			{
				name:      "2-byte IDs",
				vectorIDs: []uint64{1, 100, 1000, 65535},
				versions:  []VectorVersion{1, 2, 127, 128}, // 128 has tombstone bit set
			},
			{
				name:      "3-byte IDs",
				vectorIDs: []uint64{65536, 100000, 16777215},
				versions:  []VectorVersion{1, 1, 1},
			},
			{
				name:      "4-byte IDs",
				vectorIDs: []uint64{16777216, 100000000, 4294967295},
				versions:  []VectorVersion{5, 10, 15},
			},
			{
				name:      "5-byte IDs",
				vectorIDs: []uint64{4294967296, 500000000000, 1099511627775},
				versions:  []VectorVersion{1, 2, 3},
			},
			{
				name:      "8-byte IDs",
				vectorIDs: []uint64{1099511627776, ^uint64(0) - 1, ^uint64(0)},
				versions:  []VectorVersion{1, 1, 1},
			},
			{
				name:      "mixed small IDs - scheme determined by max",
				vectorIDs: []uint64{1, 2, 3, 16777216}, // forces 4-byte scheme
				versions:  []VectorVersion{1, 2, 3, 4},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				store := makePostingMetadataStore(t)
				postingID := uint64(42)

				// Create posting with vector IDs and versions using NewVector
				posting := make(Posting, len(tt.vectorIDs))
				for i := range tt.vectorIDs {
					posting[i] = NewVector(tt.vectorIDs[i], tt.versions[i], nil)
				}

				err := store.SetVectorIDs(ctx, postingID, posting)
				require.NoError(t, err)

				// Invalidate cache to force disk read
				store.cache.Invalidate(postingID)

				m, err := store.Get(ctx, postingID)
				require.NoError(t, err)
				vectorIDs, versions := decodePacked(m.PackedPostingMetadata)
				require.Equal(t, tt.vectorIDs, vectorIDs)
				require.Equal(t, tt.versions, versions)
			})
		}
	})

	t.Run("version byte encoding preserves all bits", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		postingID := uint64(99)

		// Test all possible version values (0-255)
		vectorIDs := make([]uint64, 256)
		versions := make([]VectorVersion, 256)
		for i := 0; i < 256; i++ {
			vectorIDs[i] = uint64(i + 1)
			versions[i] = VectorVersion(i)
		}

		posting := make(Posting, 256)
		for i := range vectorIDs {
			posting[i] = NewVector(vectorIDs[i], versions[i], nil)
		}

		err := store.SetVectorIDs(ctx, postingID, posting)
		require.NoError(t, err)

		store.cache.Invalidate(postingID)

		m, err := store.Get(ctx, postingID)
		require.NoError(t, err)
		vIDs, vVersions := decodePacked(m.PackedPostingMetadata)
		require.Equal(t, vectorIDs, vIDs)
		require.Equal(t, versions, vVersions)
	})

	t.Run("large posting count", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		postingID := uint64(100)

		// Create a posting with 1000 vectors
		count := 1000
		vectorIDs := make([]uint64, count)
		versions := make([]VectorVersion, count)
		for i := 0; i < count; i++ {
			vectorIDs[i] = uint64(i + 1)
			versions[i] = VectorVersion(i % 128)
		}

		posting := make(Posting, count)
		for i := range vectorIDs {
			posting[i] = NewVector(vectorIDs[i], versions[i], nil)
		}

		err := store.SetVectorIDs(ctx, postingID, posting)
		require.NoError(t, err)

		store.cache.Invalidate(postingID)

		m, err := store.Get(ctx, postingID)
		require.NoError(t, err)
		require.EqualValues(t, count, m.Count())
		vIDs, vVersions := decodePacked(m.PackedPostingMetadata)
		require.Equal(t, vectorIDs, vIDs)
		require.Equal(t, versions, vVersions)
	})
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
		var i int
		for id, v := range m.Iter() {
			require.Equal(t, id, posting[i].ID())
			require.Equal(t, v, posting[i].Version())
			i++
		}

		count, err := store.CountVectorIDs(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)

		store.cache.Invalidate(42)

		m, err = store.Get(ctx, 42)
		require.NoError(t, err)
		i = 0
		for id, v := range m.Iter() {
			require.Equal(t, id, posting[i].ID())
			require.Equal(t, v, posting[i].Version())
			i++
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
		count, err := store.FastAddVectorID(ctx, 42, 100, 1)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)

		count, err = store.FastAddVectorID(ctx, 42, 200, 1)
		require.NoError(t, err)
		require.EqualValues(t, 2, count)

		m, err := store.Get(ctx, 42)
		require.NoError(t, err)
		id, v := m.GetAt(0)
		require.Equal(t, uint64(100), id)
		require.Equal(t, VectorVersion(1), v)
		id, v = m.GetAt(1)
		require.Equal(t, uint64(200), id)
		require.Equal(t, VectorVersion(1), v)
	})
}
