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
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func makePostingMetadataStore(t *testing.T) *PostingMap {
	t.Helper()

	bucket := makePostingMetadataBucket(t)
	return NewPostingMap(bucket)
}

func makePostingMetadataBucket(t *testing.T) *lsmkv.Bucket {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	return bucket
}

var idCounter atomic.Uint64

func makeVectors(t *testing.T, n, dims int) []Vector {
	t.Helper()
	vectors, _ := testinghelpers.RandomVecsFixedSeed(n, 0, dims)
	result := make([]Vector, n)

	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, 42, distancer.NewL2SquaredProvider())
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		compressed := quantizer.CompressedBytes(quantizer.Encode(vectors[i]))
		result[i] = NewVector(idCounter.Add(1), 1, compressed)
	}
	return result
}

func decodePacked(encoded PackedPostingMetadata) []uint64 {
	var ids []uint64
	for id := range encoded.Iter() {
		ids = append(ids, id)
	}
	return ids
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
				encoded := NewPackedPostingMetadata(tt.vectorIDs)

				decodedIDs := decodePacked(encoded)
				require.Equal(t, tt.vectorIDs, decodedIDs)
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

				m, err := store.Get(ctx, postingID)
				require.NoError(t, err)
				vectorIDs := decodePacked(m.PackedPostingMetadata)
				require.Equal(t, tt.vectorIDs, vectorIDs)
			})
		}
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

		m, err := store.Get(ctx, postingID)
		require.NoError(t, err)
		require.EqualValues(t, count, m.Count())
		vIDs := decodePacked(m.PackedPostingMetadata)
		require.Equal(t, vectorIDs, vIDs)
	})

	t.Run("AddVector scheme upgrade with data preservation", func(t *testing.T) {
		// Test that when AddVector needs to upgrade scheme, all previous data is preserved
		var data PackedPostingMetadata

		// Start with 2-byte scheme (small vectors)
		data = data.AddVector(100)
		require.Equal(t, uint32(1), data.Count())

		data = data.AddVector(200)
		require.Equal(t, uint32(2), data.Count())

		// Add a vector that requires 4-byte scheme (should trigger upgrade)
		data = data.AddVector(16777216)
		require.Equal(t, uint32(3), data.Count())

		// Verify all data is preserved in correct order
		ids := decodePacked(data)
		require.Equal(t, []uint64{100, 200, 16777216}, ids)

		// Verify scheme is now 4-byte
		require.Equal(t, schemeID4Byte, Scheme(data[0]))
	})

	t.Run("AddVector multiple consecutive scheme upgrades", func(t *testing.T) {
		// Test that multiple consecutive scheme upgrades preserve all data correctly
		var data PackedPostingMetadata

		// Add in ascending order to trigger multiple upgrades
		testCases := []struct {
			id      uint64
			version VectorVersion
		}{
			{100, 1},                  // 2-byte
			{1000, 2},                 // still 2-byte
			{65536, 3},                // upgrade to 3-byte
			{100000, 4},               // still 3-byte
			{16777216, 5},             // upgrade to 4-byte
			{1000000000, 6},           // still 4-byte
			{4294967296, 7},           // upgrade to 5-byte
			{500000000000, 8},         // still 5-byte
			{1099511627776, 9},        // upgrade to 8-byte
			{9223372036854775807, 10}, // max int64, still 8-byte
		}

		for _, tc := range testCases {
			data = data.AddVector(tc.id)
		}

		// Verify all data is preserved
		ids := decodePacked(data)
		require.Len(t, ids, len(testCases))

		for i, tc := range testCases {
			require.Equal(t, tc.id, ids[i], "ID at index %d", i)
		}

		// Final scheme should be 8-byte
		require.Equal(t, schemeID8Byte, Scheme(data[0]))
	})

	t.Run("AddVector boundary conditions", func(t *testing.T) {
		testCases := []struct {
			name     string
			boundary uint64
			nextSize uint64
		}{
			{"2-byte max boundary", 65535, 65536},
			{"3-byte max boundary", 16777215, 16777216},
			{"4-byte max boundary", 4294967295, 4294967296},
			{"5-byte max boundary", 1099511627775, 1099511627776},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var data PackedPostingMetadata

				// Add at boundary
				data = data.AddVector(tc.boundary)
				ids := decodePacked(data)
				require.Equal(t, []uint64{tc.boundary}, ids)

				// Add just over boundary (should trigger upgrade)
				data = data.AddVector(tc.nextSize)
				ids = decodePacked(data)
				require.Equal(t, []uint64{tc.boundary, tc.nextSize}, ids)
			})
		}
	})

	t.Run("AddVector order preservation through upgrades", func(t *testing.T) {
		var data PackedPostingMetadata

		// Add vectors in specific order
		additions := []struct {
			id      uint64
			version VectorVersion
		}{
			{1000, 10},
			{2000, 20},
			{16777216, 30}, // triggers upgrade to 4-byte
			{3000, 40},
			{4000, 50},
		}

		for _, add := range additions {
			data = data.AddVector(add.id)
		}

		// Verify order is preserved
		ids := decodePacked(data)
		require.Equal(t, []uint64{1000, 2000, 16777216, 3000, 4000}, ids)
	})

	t.Run("AddVector to empty posting", func(t *testing.T) {
		var data PackedPostingMetadata
		require.Equal(t, uint32(0), data.Count())

		// Add first vector
		data = data.AddVector(12345)
		require.Equal(t, uint32(1), data.Count())

		ids := decodePacked(data)
		require.Equal(t, []uint64{12345}, ids)

		// Verify header is correct
		require.Greater(t, len(data), 5)
		scheme := Scheme(data[0])
		require.True(t, scheme >= schemeID2Byte && scheme <= schemeID8Byte)
	})

	t.Run("AddVector avoids large default backing buffer", func(t *testing.T) {
		var data PackedPostingMetadata

		data = data.AddVector(12345)

		require.Equal(t, 7, len(data))
		require.LessOrEqual(t, cap(data), 16)
	})

	t.Run("Compact trims spare capacity", func(t *testing.T) {
		data := PackedPostingMetadata(make([]byte, 8, 128))

		compact := data.Compact()

		require.Equal(t, len(data), len(compact))
		require.Equal(t, len(compact), cap(compact))
	})

	t.Run("AddVector with zero ID", func(t *testing.T) {
		var data PackedPostingMetadata

		// Add with ID = 0
		data = data.AddVector(0)
		require.Equal(t, uint32(1), data.Count())

		ids := decodePacked(data)
		require.Equal(t, []uint64{0}, ids)

		// Add another to ensure 0 doesn't break iteration
		data = data.AddVector(100)
		ids = decodePacked(data)
		require.Equal(t, []uint64{0, 100}, ids)
	})

	t.Run("AddVector interleaved small and large", func(t *testing.T) {
		var data PackedPostingMetadata

		// Interleave small and large IDs
		additions := []struct {
			id      uint64
			version VectorVersion
		}{
			{10, 1},
			{4294967295, 2},    // triggers 4-byte upgrade
			{20, 3},            // small ID after upgrade
			{16777216, 4},      // medium ID
			{1099511627776, 5}, // triggers 5-byte upgrade
			{30, 6},            // small ID at end
		}

		for _, add := range additions {
			data = data.AddVector(add.id)
		}

		ids := decodePacked(data)
		expectedIDs := []uint64{10, 4294967295, 20, 16777216, 1099511627776, 30}

		require.Equal(t, expectedIDs, ids)
	})

	t.Run("legacy ID plus version format normalizes to IDs only", func(t *testing.T) {
		legacy := PackedPostingMetadata{
			byte(schemeID2Byte),
			3, 0, 0, 0,
			10, 0, 1,
			20, 0, 2,
			30, 0, 3,
		}

		normalized := normalizePackedPostingMetadata(legacy, false)

		require.Equal(t, 11, len(normalized))
		require.Equal(t, []uint64{10, 20, 30}, decodePacked(normalized))
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
		posting := Posting(makeVectors(t, 10, 16))
		err := store.SetVectorIDs(ctx, 42, posting)
		require.NoError(t, err)

		m, err := store.Get(ctx, 42)
		require.NoError(t, err)
		var i int
		for id := range m.Iter() {
			require.Equal(t, id, posting[i].ID())
			i++
		}

		require.EqualValues(t, 10, m.Count())

		m, err = store.Get(ctx, 42)
		require.NoError(t, err)
		i = 0
		for id := range m.Iter() {
			require.Equal(t, id, posting[i].ID())
			i++
		}
	})

	t.Run("FastAddVectorID", func(t *testing.T) {
		store := makePostingMetadataStore(t)
		count, err := store.FastAddVectorID(ctx, 42, 100)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)

		count, err = store.FastAddVectorID(ctx, 42, 200)
		require.NoError(t, err)
		require.EqualValues(t, 2, count)

		m, err := store.Get(ctx, 42)
		require.NoError(t, err)
		id := m.GetAt(0)
		require.Equal(t, uint64(100), id)
		id = m.GetAt(1)
		require.Equal(t, uint64(200), id)
	})

	t.Run("Iter with multiple postings", func(t *testing.T) {
		store := makePostingMetadataStore(t)

		posting1 := Posting(makeVectors(t, 5, 16))
		err := store.SetVectorIDs(ctx, 42, posting1)
		require.NoError(t, err)

		posting2 := Posting(makeVectors(t, 5, 16))
		err = store.SetVectorIDs(ctx, 43, posting2)
		require.NoError(t, err)

		var count uint64
		for _, metadata := range store.Iter() {
			count += uint64(metadata.Count())
		}
		require.EqualValues(t, 10, count)
	})

	t.Run("migrates legacy prefix before restore", func(t *testing.T) {
		bucket := makePostingMetadataBucket(t)
		store := NewPostingMapStore(bucket, postingMapBucketPrefixV2)

		legacy := PackedPostingMetadata{
			byte(schemeID2Byte),
			3, 0, 0, 0,
			10, 0, 1,
			20, 0, 2,
			30, 0, 3,
		}
		legacyKey := postingMapKey(postingMapBucketPrefixV1, 42)
		err := bucket.Put(legacyKey, legacy)
		require.NoError(t, err)

		err = migratePostingMapV1ToV2(ctx, bucket, logrus.New())
		require.NoError(t, err)

		pm := NewPostingMap(bucket)
		err = pm.Restore(ctx)
		require.NoError(t, err)

		metadata, err := pm.Get(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, []uint64{10, 20, 30}, decodePacked(metadata.PackedPostingMetadata))

		v2, err := store.Get(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, []uint64{10, 20, 30}, decodePacked(v2))

		size, err := NewPostingSizesStore(bucket, postingSizesBucketPrefix).Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 3, size)

		c := bucket.Cursor()
		defer c.Close()
		k, _ := c.Seek(postingMapBucketPrefixV1)
		require.False(t, bytes.HasPrefix(k, postingMapBucketPrefixV1))

		err = migratePostingMapV1ToV2(ctx, bucket, logrus.New())
		require.NoError(t, err)
	})
}
