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

package docid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_InMemDeletedTracker(t *testing.T) {
	t.Run("importing single ids and verifying they're contained", func(t *testing.T) {
		tracker := NewInMemDeletedTracker()

		tracker.Add(24)
		tracker.Add(25)
		tracker.Add(25) // import duplicate to verify idempotent add works
		tracker.Add(26)

		assert.False(t, tracker.Contains(23))
		assert.True(t, tracker.Contains(24))
		assert.True(t, tracker.Contains(25))
		assert.True(t, tracker.Contains(26))
		assert.False(t, tracker.Contains(27))
	})

	t.Run("bulk importing and verifying", func(t *testing.T) {
		tracker := NewInMemDeletedTracker()

		tracker.Add(24)
		tracker.BulkAdd([]uint64{25, 26})

		assert.False(t, tracker.Contains(23))
		assert.True(t, tracker.Contains(24))
		assert.True(t, tracker.Contains(25))
		assert.True(t, tracker.Contains(26))
		assert.False(t, tracker.Contains(27))

		list := tracker.GetAll()
		assert.ElementsMatch(t, []uint64{24, 25, 26}, list)
	})

	t.Run("removing an id", func(t *testing.T) {
		tracker := NewInMemDeletedTracker()

		tracker.BulkAdd([]uint64{25, 26})
		tracker.Remove(25)

		assert.False(t, tracker.Contains(24))
		assert.False(t, tracker.Contains(25))
		assert.True(t, tracker.Contains(26))
		assert.False(t, tracker.Contains(27))
	})

	t.Run("bulk removing ids", func(t *testing.T) {
		tracker := NewInMemDeletedTracker()

		tracker.BulkAdd([]uint64{25, 26, 27})
		tracker.BulkRemove([]uint64{25, 26})

		assert.False(t, tracker.Contains(24))
		assert.False(t, tracker.Contains(25))
		assert.False(t, tracker.Contains(26))
		assert.True(t, tracker.Contains(27))
	})
}
