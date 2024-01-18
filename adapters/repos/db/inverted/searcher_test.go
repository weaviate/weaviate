//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/sroar"
)

func TestDocBitmap(t *testing.T) {
	t.Run("empty doc bitmap", func(t *testing.T) {
		dbm := newDocBitmap()

		assert.Equal(t, 0, dbm.count())
		assert.Empty(t, dbm.IDs())
	})

	t.Run("filled doc bitmap", func(t *testing.T) {
		ids := []uint64{1, 2, 3, 4, 5}

		dbm := newDocBitmap()
		dbm.docIDs.SetMany(ids)

		assert.Equal(t, 5, dbm.count())
		assert.ElementsMatch(t, ids, dbm.IDs())
	})
}

func TestDocBitmap_IDsWithLimit(t *testing.T) {
	type test struct {
		name           string
		limit          int
		input          []uint64
		expectedOutput []uint64
	}

	tests := []test{
		{
			name:           "empty bitmap, positive limit",
			input:          []uint64{},
			limit:          7,
			expectedOutput: []uint64{},
		},
		{
			name:           "limit matches bitmap cardinality",
			input:          []uint64{2, 4, 6, 8, 10},
			limit:          5,
			expectedOutput: []uint64{2, 4, 6, 8, 10},
		},
		{
			name:           "limit less than cardinality",
			input:          []uint64{2, 4, 6, 8, 10},
			limit:          3,
			expectedOutput: []uint64{2, 4, 6},
		},
		{
			name:           "limit higher than cardinality",
			input:          []uint64{2, 4, 6, 8, 10},
			limit:          10,
			expectedOutput: []uint64{2, 4, 6, 8, 10},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbm := docBitmap{
				docIDs: sroar.NewBitmap(),
			}

			dbm.docIDs.SetMany(test.input)

			res := dbm.IDsWithLimit(test.limit)
			assert.Equal(t, test.expectedOutput, res)
		})
	}
}

func TestDocIDsIterator_Slice(t *testing.T) {
	t.Run("iterator empty slice", func(t *testing.T) {
		it := newSliceDocIDsIterator([]uint64{})

		id1, ok1 := it.Next()

		assert.Equal(t, 0, it.Len())
		assert.False(t, ok1)
		assert.Equal(t, uint64(0), id1)
	})

	t.Run("iterator step by step", func(t *testing.T) {
		it := newSliceDocIDsIterator([]uint64{3, 1, 0, 2})

		id1, ok1 := it.Next()
		id2, ok2 := it.Next()
		id3, ok3 := it.Next()
		id4, ok4 := it.Next()
		id5, ok5 := it.Next()

		assert.Equal(t, 4, it.Len())
		assert.True(t, ok1)
		assert.Equal(t, uint64(3), id1)
		assert.True(t, ok2)
		assert.Equal(t, uint64(1), id2)
		assert.True(t, ok3)
		assert.Equal(t, uint64(0), id3)
		assert.True(t, ok4)
		assert.Equal(t, uint64(2), id4)
		assert.False(t, ok5)
		assert.Equal(t, uint64(0), id5)
	})

	t.Run("iterator in loop", func(t *testing.T) {
		it := newSliceDocIDsIterator([]uint64{3, 1, 0, 2})
		ids := []uint64{}

		for id, ok := it.Next(); ok; id, ok = it.Next() {
			ids = append(ids, id)
		}

		assert.Equal(t, 4, it.Len())
		assert.Equal(t, []uint64{3, 1, 0, 2}, ids)
	})
}
