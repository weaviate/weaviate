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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BinarySearchTreeMap(t *testing.T) {
	t.Run("single row key, single map key", func(t *testing.T) {
		tree := &binarySearchTreeMap[MapPair]{}
		rowKey := []byte("rowkey")

		pair1 := MapPair{
			Key:   []byte("map-key-1"),
			Value: []byte("map-value-1"),
		}

		tree.insert(rowKey, pair1)

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("map-key-1"),
				Value: []byte("map-value-1"),
			},
		}, res)
	})

	t.Run("single row key, updated map value", func(t *testing.T) {
		tree := &binarySearchTreeMap[MapPair]{}
		rowKey := []byte("rowkey")

		tree.insert(rowKey, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b2"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a2"),
		})

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b2"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)
	})

	t.Run("two row keys, updated map value", func(t *testing.T) {
		tree := &binarySearchTreeMap[MapPair]{}
		rowKey1 := []byte("rowkey")
		rowKey2 := []byte("other-rowkey")

		tree.insert(rowKey1, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("z"),
			Value: []byte("z1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("x"),
			Value: []byte("x1"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("b"),
			Value: []byte("b2"),
		})

		tree.insert(rowKey1, MapPair{
			Key:   []byte("a"),
			Value: []byte("a2"),
		})

		tree.insert(rowKey2, MapPair{
			Key:   []byte("x"),
			Value: []byte("x2"),
		})

		res, err := tree.get(rowKey1)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("a"),
				Value: []byte("a2"),
			},
			{
				Key:   []byte("b"),
				Value: []byte("b2"),
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)

		res, err = tree.get(rowKey2)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:   []byte("x"),
				Value: []byte("x2"),
			},
			{
				Key:   []byte("z"),
				Value: []byte("z1"),
			},
		}, res)
	})

	t.Run("single row key, deleted map values", func(t *testing.T) {
		tree := &binarySearchTreeMap[MapPair]{}
		rowKey := []byte("rowkey")

		tree.insert(rowKey, MapPair{
			Key:   []byte("c"),
			Value: []byte("c1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("a"),
			Value: []byte("a1"),
		})

		tree.insert(rowKey, MapPair{
			Key:   []byte("b"),
			Value: []byte("b1"),
		})

		tree.insert(rowKey, MapPair{
			Key:       []byte("b"),
			Tombstone: true,
		})

		tree.insert(rowKey, MapPair{
			Key:       []byte("a"),
			Tombstone: true,
		})

		res, err := tree.get(rowKey)
		require.Nil(t, err)
		assert.Equal(t, []MapPair{
			{
				Key:       []byte("a"),
				Tombstone: true,
			},
			{
				Key:       []byte("b"),
				Tombstone: true,
			},
			{
				Key:   []byte("c"),
				Value: []byte("c1"),
			},
		}, res)
	})
}

func TestBSTMap_Flatten(t *testing.T) {
	t.Run("flattened bst is snapshot of current bst", func(t *testing.T) {
		rowkey1 := "rowkey-1"
		rowkey2 := "rowkey-2"
		rowkey3 := "rowkey-3"
		rowkey4 := "rowkey-4"

		rowkeys := map[string][]byte{
			rowkey1: []byte(rowkey1),
			rowkey2: []byte(rowkey2),
			rowkey3: []byte(rowkey3),
			rowkey4: []byte(rowkey4),
		}
		pairs := map[string]MapPair{
			rowkey1: {
				Key:       []byte("key-1"),
				Value:     []byte("val-1"),
				Tombstone: false,
			},
			rowkey2: {
				Key:       []byte("key-2"),
				Value:     nil,
				Tombstone: true,
			},
			rowkey3: {
				Key:       []byte("key-3"),
				Value:     []byte("val-3"),
				Tombstone: false,
			},
		}
		pairsUpdated := map[string]MapPair{
			rowkey1: {
				Key:       []byte("key-1"),
				Value:     nil,
				Tombstone: true,
			},
			rowkey2: {
				Key:       []byte("key-2"),
				Value:     []byte("val-22"),
				Tombstone: false,
			},
			rowkey3: {
				Key:       []byte("key-3"),
				Value:     nil,
				Tombstone: true,
			},
			rowkey4: {
				Key:       []byte("key-4"),
				Value:     []byte("val-44"),
				Tombstone: false,
			},
		}

		type expectedFlattened struct {
			rowkey []byte
			pair   MapPair
		}
		assertFlattenedMatches := func(t *testing.T, flattened []*binarySearchNodeMap[MapPair], expected []expectedFlattened) {
			t.Helper()
			require.Len(t, flattened, len(expected))
			for i, exp := range expected {
				assert.Equal(t, exp.rowkey, flattened[i].key)
				require.Len(t, flattened[i].values, 1)
				val := flattened[i].values[0]
				assert.Equal(t, exp.pair.Key, val.Key)
				assert.Equal(t, exp.pair.Value, val.Value)
				assert.Equal(t, exp.pair.Tombstone, val.Tombstone)
			}
		}

		bst := &binarySearchTreeMap[MapPair]{}
		// mixed order
		bst.insert(rowkeys[rowkey3], pairs[rowkey3])
		bst.insert(rowkeys[rowkey1], pairs[rowkey1])
		bst.insert(rowkeys[rowkey2], pairs[rowkey2])

		expectedBeforeUpdate := []expectedFlattened{
			{rowkeys[rowkey1], pairs[rowkey1]},
			{rowkeys[rowkey2], pairs[rowkey2]},
			{rowkeys[rowkey3], pairs[rowkey3]},
		}

		flatBeforeUpdate := bst.flattenInOrder()
		assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)

		t.Run("flattened bst does not change on bst update", func(t *testing.T) {
			// mixed order
			bst.insert(rowkeys[rowkey3], pairsUpdated[rowkey3])
			bst.insert(rowkeys[rowkey4], pairsUpdated[rowkey4])
			bst.insert(rowkeys[rowkey1], pairsUpdated[rowkey1])
			bst.insert(rowkeys[rowkey2], pairsUpdated[rowkey2])

			expectedAfterUpdate := []expectedFlattened{
				{rowkeys[rowkey1], pairsUpdated[rowkey1]},
				{rowkeys[rowkey2], pairsUpdated[rowkey2]},
				{rowkeys[rowkey3], pairsUpdated[rowkey3]},
				{rowkeys[rowkey4], pairsUpdated[rowkey4]},
			}

			flatAfterUpdate := bst.flattenInOrder()
			assertFlattenedMatches(t, flatBeforeUpdate, expectedBeforeUpdate)
			assertFlattenedMatches(t, flatAfterUpdate, expectedAfterUpdate)
		})
	})
}

// at 200k pairs a sort that is not stable, or one the sorted check wrongly
// skips, keeps the wrong write of a re-written doc ID; the tables above are too
// short to expose either
func TestSortAndDedupValuesAtScale(t *testing.T) {
	const docs = 200_000

	docIDs := make([]uint64, 0, docs+docs/5)
	for i := 0; i < docs; i++ {
		docIDs = append(docIDs, uint64(i))
		if i%5 == 0 {
			docIDs = append(docIDs, uint64(i)) // re-written doc
		}
	}

	// the position of the last write of each doc ID, which is what its
	// surviving pair must carry as its value
	lastWrite := make(map[uint64][]byte, docs)
	for i, id := range docIDs {
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(i))
		lastWrite[id] = v
	}

	for _, bigEndian := range []bool{true, false} {
		out := sortAndDedupValues(mapPairsForDocIDs(docIDs, bigEndian))
		require.Len(t, out, docs)

		for i, pair := range out {
			if i > 0 {
				require.Negative(t, out[i-1].keyCompare(pair),
					"output must be sorted by key, big endian: %v", bigEndian)
			}

			id := binary.LittleEndian.Uint64(pair.Key)
			if bigEndian {
				id = binary.BigEndian.Uint64(pair.Key)
			}
			require.Equal(t, lastWrite[id], pair.Value,
				"doc %d kept a write other than its last, big endian: %v", id, bigEndian)
		}
	}
}
