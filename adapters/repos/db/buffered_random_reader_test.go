package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomGen(t *testing.T) {
	b, err := newBufferedRandomGen(100)
	require.Nil(t, err)

	t.Run("gen multiple random sequences that fit in a single buffer", func(t *testing.T) {
		s1, err := b.Make(30)
		assert.Nil(t, err)
		s2, err := b.Make(30)
		assert.Nil(t, err)
		s3, err := b.Make(30)
		assert.Nil(t, err)

		assert.Len(t, s1, 30)
		assert.Len(t, s2, 30)
		assert.Len(t, s3, 30)
		assert.NotEqual(t, s1, s2)
		assert.NotEqual(t, s1, s3)
		assert.NotEqual(t, s2, s3)
	})

	t.Run("gen multiple random sequences that each use exactly the buffer", func(t *testing.T) {
		s1, err := b.Make(100)
		assert.Nil(t, err)
		s2, err := b.Make(100)
		assert.Nil(t, err)
		s3, err := b.Make(100)
		assert.Nil(t, err)

		assert.Len(t, s1, 100)
		assert.Len(t, s2, 100)
		assert.Len(t, s3, 100)
		assert.NotEqual(t, s1, s2)
		assert.NotEqual(t, s1, s3)
		assert.NotEqual(t, s2, s3)
	})

	t.Run("gen multiple random sequences that each use parts of the buffer", func(t *testing.T) {
		s1, err := b.Make(80)
		assert.Nil(t, err)
		s2, err := b.Make(80)
		assert.Nil(t, err)
		s3, err := b.Make(80)
		assert.Nil(t, err)

		assert.Len(t, s1, 80)
		assert.Len(t, s2, 80)
		assert.Len(t, s3, 80)
		assert.NotEqual(t, s1, s2)
		assert.NotEqual(t, s1, s3)
		assert.NotEqual(t, s2, s3)
	})
}
