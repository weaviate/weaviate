//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
)

func TestDocBitmap(t *testing.T) {
	t.Run("empty doc bitmap", func(t *testing.T) {
		dbm := newDocBitmap()

		assert.Equal(t, 0, dbm.count())
		assert.Empty(t, dbm.IDs())
		assert.Empty(t, dbm.checksum)

		// pointers := dbm.toDocPointers()

		// assert.Equal(t, uint64(0), pointers.count)
		// assert.Empty(t, pointers.docIDs)
		// assert.Empty(t, pointers.checksum)
	})

	t.Run("filled doc bitmap", func(t *testing.T) {
		ids := []uint64{1, 2, 3, 4, 5}
		checksum := []byte("checksum")

		dbm := newDocBitmap()
		dbm.docIDs.SetMany(ids)
		dbm.checksum = checksum

		assert.Equal(t, 5, dbm.count())
		assert.ElementsMatch(t, ids, dbm.IDs())
		assert.Equal(t, checksum, dbm.checksum)

		// pointers := dbm.toDocPointers()

		// assert.Equal(t, uint64(5), pointers.count)
		// assert.ElementsMatch(t, ids, pointers.docIDs)
		// assert.Equal(t, checksum, pointers.checksum)
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
