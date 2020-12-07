//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowCacher(t *testing.T) {
	cacher := NewRowCacher(100)

	rowID := []byte("myRow")
	t.Run("it allow entries which still fit", func(t *testing.T) {
		original := &docPointers{
			count: 5, // =20 bytes
			docIDs: []docPointer{
				docPointer{id: 1},
				docPointer{id: 2},
				docPointer{id: 3},
				docPointer{id: 4},
				docPointer{id: 5},
			},
			checksum: []uint8{0, 1, 2, 3, 4, 5, 6, 7},
		}
		cacher.Store(rowID, original)

		retrieved, ok := cacher.Load(rowID, []uint8{0, 1, 2, 3, 4, 5, 6, 7})
		assert.True(t, ok)
		assert.Equal(t, original, retrieved)
	})

	// TODO: calculate correct size if a prop has a frequency (8 bytes per entry
	// isntead of 4)

	t.Run("it returns nothing if an expected checksum doesn't match", func(t *testing.T) {
		_, ok := cacher.Load(rowID, []uint8{0, 0, 0, 0, 0, 0, 0, 0})
		assert.False(t, ok)
	})

	t.Run("it deletes the corresponding entry so that a future lookup fails even with the right checksum", func(t *testing.T) {
		_, ok := cacher.Load(rowID, []uint8{0, 1, 2, 3, 4, 5, 6, 7})
		assert.False(t, ok)
	})

	// TODO: This should be improved to explicitly remove the oldest value, not
	// just a random value
	t.Run("it removes an old value when a new value wouldn't fit anymore",
		func(t *testing.T) {
			oldEntry := &docPointers{
				count:    20, // =80 bytes
				docIDs:   make([]docPointer, 20),
				checksum: []uint8{0, 1, 2, 3, 4, 5, 6, 7},
			}
			cacher.Store(rowID, oldEntry)

			// validate its there
			_, ok := cacher.Load(rowID, []uint8{0, 1, 2, 3, 4, 5, 6, 7})
			assert.True(t, ok)

			newRowID := []byte("newrow")
			newEntry := &docPointers{
				count:    20, // =80 bytes
				docIDs:   make([]docPointer, 20),
				checksum: []uint8{0, 1, 2, 3, 4, 5, 6, 7},
			}
			cacher.Store(newRowID, newEntry)

			// validate the old is gone
			_, ok = cacher.Load(rowID, []uint8{0, 1, 2, 3, 4, 5, 6, 7})
			assert.False(t, ok)

			// validate the new is here
			_, ok = cacher.Load(newRowID, []uint8{0, 1, 2, 3, 4, 5, 6, 7})
			assert.True(t, ok)
		})

	t.Run("it ignores an entry that would never fit, regardless of deleting",
		func(t *testing.T) {
			tooBig := []byte("tooBig")
			newEntry := &docPointers{
				count:    40, // =160 bytes
				docIDs:   make([]docPointer, 40),
				checksum: []uint8{0, 1, 2, 3, 4, 5, 6, 7},
			}
			cacher.Store(tooBig, newEntry)
			// validate the new is here
			_, ok := cacher.Load(tooBig, []uint8{0, 1, 2, 3, 4, 5, 6, 7})
			assert.False(t, ok)
		})
}
