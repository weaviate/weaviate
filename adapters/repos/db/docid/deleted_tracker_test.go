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
		tracker.BulkAdd([]uint32{25, 26})

		assert.False(t, tracker.Contains(23))
		assert.True(t, tracker.Contains(24))
		assert.True(t, tracker.Contains(25))
		assert.True(t, tracker.Contains(26))
		assert.False(t, tracker.Contains(27))
	})

	t.Run("removing an id", func(t *testing.T) {
		tracker := NewInMemDeletedTracker()

		tracker.BulkAdd([]uint32{25, 26})
		tracker.Remove(25)

		assert.False(t, tracker.Contains(24))
		assert.False(t, tracker.Contains(25))
		assert.True(t, tracker.Contains(26))
		assert.False(t, tracker.Contains(27))
	})
}
