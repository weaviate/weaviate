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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitsetExtractSlice(t *testing.T) {
	t.Run("BasicExtraction", func(t *testing.T) {
		bset := NewBitset(10)
		bset.Set(3)
		bset.Set(5)
		bset.Set(7)

		// Extract bits [3, 8) → local indices 0,2,4 should be set
		result := bset.ExtractSlice(3, 5)

		require.Equal(t, 5, result.Size())
		require.True(t, result.IsSet(0))  // global 3
		require.False(t, result.IsSet(1)) // global 4
		require.True(t, result.IsSet(2))  // global 5
		require.False(t, result.IsSet(3)) // global 6
		require.True(t, result.IsSet(4))  // global 7
		require.Equal(t, 3, result.SetCount())
	})

	t.Run("CrossWordBoundary", func(t *testing.T) {
		// Bits straddle the 64-bit word boundary at position 63/64
		bset := NewBitset(128)
		bset.Set(62)
		bset.Set(63)
		bset.Set(64)
		bset.Set(65)

		result := bset.ExtractSlice(60, 10)

		require.Equal(t, 10, result.Size())
		require.True(t, result.IsSet(2)) // global 62
		require.True(t, result.IsSet(3)) // global 63
		require.True(t, result.IsSet(4)) // global 64
		require.True(t, result.IsSet(5)) // global 65
		require.Equal(t, 4, result.SetCount())
	})

	t.Run("FullRange", func(t *testing.T) {
		bset := NewBitset(8)
		bset.Set(1)
		bset.Set(4)
		bset.Set(7)

		result := bset.ExtractSlice(0, 8)

		require.Equal(t, 8, result.Size())
		require.Equal(t, 3, result.SetCount())
		require.True(t, result.IsSet(1))
		require.True(t, result.IsSet(4))
		require.True(t, result.IsSet(7))
	})

	t.Run("EmptySlice", func(t *testing.T) {
		bset := NewBitset(16)
		bset.Set(5)

		result := bset.ExtractSlice(0, 4) // does not include bit 5
		require.Equal(t, 4, result.Size())
		require.Equal(t, 0, result.SetCount())
	})

	t.Run("WordAlignedMultiWord", func(t *testing.T) {
		// offset is word-aligned and count spans more than one 64-bit word,
		// exercising the copy path with dstWords > 1.
		bset := NewBitset(256)
		bset.Set(64)
		bset.Set(65)
		bset.Set(127)
		bset.Set(128)

		result := bset.ExtractSlice(64, 128)

		require.Equal(t, 128, result.Size())
		require.True(t, result.IsSet(0))  // global 64
		require.True(t, result.IsSet(1))  // global 65
		require.True(t, result.IsSet(63)) // global 127
		require.True(t, result.IsSet(64)) // global 128
		require.Equal(t, 4, result.SetCount())
	})

	t.Run("NonAlignedMultiWord", func(t *testing.T) {
		// Non-aligned offset where count spans multiple destination words,
		// exercising the shift loop for dstWords > 1.
		// Range extracted: [5, 200); bit 200 is outside and must not appear.
		bset := NewBitset(256)
		bset.Set(5)
		bset.Set(68)  // 5 + 63
		bset.Set(133) // 5 + 128
		bset.Set(200) // outside [5, 200) — must not be included

		result := bset.ExtractSlice(5, 195)

		require.Equal(t, 195, result.Size())
		require.True(t, result.IsSet(0))   // global 5
		require.True(t, result.IsSet(63))  // global 68
		require.True(t, result.IsSet(128)) // global 133
		require.False(t, result.IsSet(1))
		require.Equal(t, 3, result.SetCount())
	})

	t.Run("NonAlignedSourceEndBoundary", func(t *testing.T) {
		// Non-aligned extraction where the last destination word would require
		// reading past the end of the source's backing slice. The boundary check
		// must fire on the last loop iteration without panicking or producing
		// spurious set bits.
		bset := NewBitset(70) // 2 source words: bits [0,63] and [64,69]
		bset.Set(66)
		bset.Set(69)

		// Extract [5, 70): srcBitOff=5, dstWords=2; on i=1 srcWordStart+i+1=2 == len(bits)=2
		result := bset.ExtractSlice(5, 65)

		require.Equal(t, 65, result.Size())
		require.True(t, result.IsSet(61)) // global 66
		require.True(t, result.IsSet(64)) // global 69
		require.Equal(t, 2, result.SetCount())
	})

	t.Run("LevelLocalEquivalence", func(t *testing.T) {
		// ExtractSlice(InnerNodesCount(l), LeavesCount(l)) must produce the
		// same digest selection as a full-tree discriminant for level l.
		height := 4
		for level := 0; level <= height; level++ {
			global := NewBitset(NodesCount(height))
			// Set every other node at this level in the global discriminant
			offset := InnerNodesCount(level)
			count := LeavesCount(level) // nodesAtLevel(level)
			for i := 0; i < count; i += 2 {
				global.Set(offset + i)
			}

			local := global.ExtractSlice(offset, count)
			require.Equal(t, count, local.Size())
			require.Equal(t, (count+1)/2, local.SetCount()) // ceil(count/2)
			for i := 0; i < count; i++ {
				require.Equal(t, global.IsSet(offset+i), local.IsSet(i),
					"level %d, node %d", level, i)
			}
		}
	})
}

func TestBitSet(t *testing.T) {
	bsetSize := 2 << 15

	bset := NewBitset(bsetSize)

	require.Zero(t, bset.SetCount())

	for i := 0; i < bsetSize; i++ {
		require.False(t, bset.IsSet(i))
	}

	require.False(t, bset.AllSet())

	for i := 0; i < bsetSize; i++ {
		bset.Set(i)
		require.True(t, bset.IsSet(i))
	}

	require.True(t, bset.AllSet())
	require.Equal(t, bsetSize, bset.SetCount())

	bset.Reset()
	require.Zero(t, bset.SetCount())

	require.Panics(t, func() {
		bset.IsSet(bsetSize)
	})

	require.Panics(t, func() {
		bset.Set(bsetSize)
	})

	require.Panics(t, func() {
		bset.Unset(bsetSize)
	})
}
