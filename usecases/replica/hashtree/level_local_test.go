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

package hashtree

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLevelValidation(t *testing.T) {
	height := 4
	ht, err := NewHashTree(height)
	require.NoError(t, err)

	digests := make([]Digest, LeavesCount(height))

	t.Run("negative level", func(t *testing.T) {
		_, err := ht.Level(-1, NewBitset(1), digests)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("level too high", func(t *testing.T) {
		_, err := ht.Level(height+1, NewBitset(1), digests)
		require.ErrorIs(t, err, ErrIllegalState)
	})

	t.Run("nil discriminant", func(t *testing.T) {
		_, err := ht.Level(0, nil, digests)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("wrong size discriminant — too small", func(t *testing.T) {
		_, err := ht.Level(2, NewBitset(2), digests)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("wrong size discriminant — too large", func(t *testing.T) {
		_, err := ht.Level(2, NewBitset(8), digests)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("output buffer too small", func(t *testing.T) {
		small := make([]Digest, 4)
		_, err := ht.Level(3, NewBitset(8), small)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
}

func TestLevelDiffAtLeafLevel(t *testing.T) {
	d := func(a, b uint64) Digest { return Digest{a, b} }

	t.Run("all match — discriminant cleared, next nil", func(t *testing.T) {
		discriminant := NewBitset(4).SetAll()
		digs1 := []Digest{d(1, 1), d(2, 2), d(3, 3), d(4, 4)}
		digs2 := []Digest{d(1, 1), d(2, 2), d(3, 3), d(4, 4)}

		next, count, err := LevelDiff(2, 2, discriminant, digs1, digs2)
		require.NoError(t, err)
		assert.Nil(t, next)
		assert.Equal(t, 0, count)
		assert.Equal(t, 0, discriminant.SetCount())
	})

	t.Run("all mismatch — discriminant preserved, next nil", func(t *testing.T) {
		discriminant := NewBitset(4).SetAll()
		digs1 := []Digest{d(1, 1), d(2, 2), d(3, 3), d(4, 4)}
		digs2 := []Digest{d(9, 9), d(9, 9), d(9, 9), d(9, 9)}

		next, count, err := LevelDiff(2, 2, discriminant, digs1, digs2)
		require.NoError(t, err)
		assert.Nil(t, next)
		assert.Equal(t, 4, count)
		assert.Equal(t, 4, discriminant.SetCount())
	})

	t.Run("mixed — only matched bits cleared", func(t *testing.T) {
		discriminant := NewBitset(4).SetAll()
		digs1 := []Digest{d(1, 1), d(2, 2), d(3, 3), d(4, 4)}
		digs2 := []Digest{d(1, 1), d(9, 9), d(3, 3), d(8, 8)}

		next, count, err := LevelDiff(2, 2, discriminant, digs1, digs2)
		require.NoError(t, err)
		assert.Nil(t, next)
		assert.Equal(t, 2, count)
		assert.False(t, discriminant.IsSet(0))
		assert.True(t, discriminant.IsSet(1))
		assert.False(t, discriminant.IsSet(2))
		assert.True(t, discriminant.IsSet(3))
	})
}

func TestLevelDiffInnerLevelSetsChildren(t *testing.T) {
	d := func(a, b uint64) Digest { return Digest{a, b} }

	t.Run("mismatched node sets both children in next", func(t *testing.T) {
		discriminant := NewBitset(2)
		discriminant.Set(0)
		discriminant.Set(1)

		digs1 := []Digest{d(1, 1), d(2, 2)}
		digs2 := []Digest{d(1, 1), d(9, 9)}

		next, count, err := LevelDiff(1, 3, discriminant, digs1, digs2)
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, 4, next.Size())
		assert.Equal(t, 1, count)

		assert.False(t, discriminant.IsSet(0))
		assert.False(t, next.IsSet(0))
		assert.False(t, next.IsSet(1))

		assert.True(t, discriminant.IsSet(1))
		assert.True(t, next.IsSet(2))
		assert.True(t, next.IsSet(3))
	})

	t.Run("matched node does not set children", func(t *testing.T) {
		discriminant := NewBitset(2).SetAll()
		digs1 := []Digest{d(1, 1), d(2, 2)}
		digs2 := []Digest{d(1, 1), d(2, 2)}

		next, count, err := LevelDiff(1, 3, discriminant, digs1, digs2)
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, 0, count)
		assert.Equal(t, 0, discriminant.SetCount())
		assert.Equal(t, 0, next.SetCount())
	})
}

func TestLevelDiffValidation(t *testing.T) {
	d := func(a, b uint64) Digest { return Digest{a, b} }

	t.Run("negative level", func(t *testing.T) {
		_, _, err := LevelDiff(-1, 3, NewBitset(1), []Digest{d(1, 1)}, []Digest{d(1, 1)})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("level above height", func(t *testing.T) {
		_, _, err := LevelDiff(4, 3, NewBitset(1), []Digest{d(1, 1)}, []Digest{d(1, 1)})
		require.ErrorIs(t, err, ErrIllegalState)
	})

	t.Run("nil discriminant", func(t *testing.T) {
		_, _, err := LevelDiff(0, 3, nil, []Digest{d(1, 1)}, []Digest{d(1, 1)})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("wrong-sized discriminant", func(t *testing.T) {
		_, _, err := LevelDiff(2, 3, NewBitset(8), make([]Digest, 4), make([]Digest, 4))
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("digests1 too short", func(t *testing.T) {
		disc := NewBitset(4).SetAll()
		_, _, err := LevelDiff(2, 3, disc, make([]Digest, 2), make([]Digest, 4))
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("digests2 too short", func(t *testing.T) {
		disc := NewBitset(4).SetAll()
		_, _, err := LevelDiff(2, 3, disc, make([]Digest, 4), make([]Digest, 2))
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	// At deeper levels the walk is partial: digests are sized to
	// discriminant.SetCount(), not Size().
	t.Run("partial walk — digests sized to SetCount, not Size", func(t *testing.T) {
		d := func(a, b uint64) Digest { return Digest{a, b} }
		disc := NewBitset(4)
		disc.Set(0)
		disc.Set(2)
		digs1 := []Digest{d(1, 1), d(2, 2)}
		digs2 := []Digest{d(1, 1), d(2, 2)}
		_, _, err := LevelDiff(2, 3, disc, digs1, digs2)
		require.NoError(t, err)
	})
}

func TestNewRangeReaderRejectsWrongSize(t *testing.T) {
	height := 3
	ht, err := NewHashTree(height)
	require.NoError(t, err)

	t.Run("full-tree-sized bitset", func(t *testing.T) {
		rr, err := ht.NewRangeReader(NewBitset(NodesCount(height)))
		assert.Nil(t, rr)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
	t.Run("nil discriminant", func(t *testing.T) {
		rr, err := ht.NewRangeReader(nil)
		assert.Nil(t, rr)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
	t.Run("arbitrary wrong size", func(t *testing.T) {
		rr, err := ht.NewRangeReader(NewBitset(7))
		assert.Nil(t, rr)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
}

func TestNewRangeReaderLeafIndices(t *testing.T) {
	height := 4
	ht, err := NewHashTree(height)
	require.NoError(t, err)

	leaf := NewBitset(LeavesCount(height))
	leaf.Set(2)
	leaf.Set(3)
	leaf.Set(4)
	leaf.Set(7)

	rr, err := ht.NewRangeReader(leaf)
	require.NoError(t, err)

	a, b, err := rr.Next()
	require.NoError(t, err)
	assert.EqualValues(t, 2, a)
	assert.EqualValues(t, 4, b)

	a, b, err = rr.Next()
	require.NoError(t, err)
	assert.EqualValues(t, 7, a)
	assert.EqualValues(t, 7, b)

	_, _, err = rr.Next()
	require.True(t, errors.Is(err, ErrNoMoreRanges))
}

// TestHashTreeLevelMarshalSizeScalesWithLevel asserts the per-level wire size
// is nodesAtLevel(level)/8 + header, and strictly smaller than the previous
// full-tree encoding at every non-leaf level.
func TestHashTreeLevelMarshalSizeScalesWithLevel(t *testing.T) {
	height := 16

	// Bitset.Marshal header: uint32 size + uint32 setCount.
	const header = 8

	fullBytes := (NodesCount(height) + 63) / 64 * 8
	require.Equal(t, header+fullBytes, marshalledSize(t, NewBitset(NodesCount(height))))

	for level := 0; level <= height; level++ {
		want := header + (nodesAtLevel(level)+63)/64*8

		buf, err := NewBitset(nodesAtLevel(level)).Marshal()
		require.NoError(t, err)
		assert.Equalf(t, want, len(buf),
			"level %d: marshalled bitset = %d bytes, expected %d", level, len(buf), want)

		if level < height {
			assert.Lessf(t, len(buf), header+fullBytes,
				"level %d: per-level payload (%d) should be < full-tree payload (%d)",
				level, len(buf), header+fullBytes)
		}
	}
}

func marshalledSize(t *testing.T, b *Bitset) int {
	t.Helper()
	buf, err := b.Marshal()
	require.NoError(t, err)
	return len(buf)
}
