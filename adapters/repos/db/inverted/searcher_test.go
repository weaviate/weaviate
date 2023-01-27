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
