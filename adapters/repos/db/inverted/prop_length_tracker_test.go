package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PropertyLengthTracker(t *testing.T) {
	t.Run("single prop", func(t *testing.T) {
		tracker := NewPropertyLengthTracker("")
		tracker.TrackProperty("my-very-first-prop", 2)
		tracker.TrackProperty("my-very-first-prop", 2)
		tracker.TrackProperty("my-very-first-prop", 3)

		res, err := tracker.PropertyMean("my-very-first-prop")
		require.Nil(t, err)

		assert.Equal(t, float32(7.0/3.0), res)
	})
}
