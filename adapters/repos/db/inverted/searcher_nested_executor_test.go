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

package inverted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// ---- applyDirectAnd --------------------------------------------------------

func TestApplyDirectAnd(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)
	// Positions for doc5 at different root/leaf coordinates.
	pos511 := nested.Encode(1, 1, doc5) // root=1 leaf=1 docID=5
	pos512 := nested.Encode(1, 2, doc5) // root=1 leaf=2 docID=5 — same root+docID, different leaf
	pos521 := nested.Encode(2, 1, doc5) // root=2 leaf=1 docID=5 — different root
	pos711 := nested.Encode(1, 1, doc7) // root=1 leaf=1 docID=7 — different doc

	t.Run("empty input returns empty", func(t *testing.T) {
		assert.True(t, applyDirectAnd(nil).IsEmpty())
	})

	t.Run("single bitmap returns docIDs", func(t *testing.T) {
		assert.Equal(t, []uint64{doc5}, applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511)}).ToArray())
	})

	t.Run("exact same raw position in both bitmaps — match", func(t *testing.T) {
		result := applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511)})
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("same root+docID different leaf — no match (requires exact position)", func(t *testing.T) {
		// directAnd requires identical raw positions; leaf difference breaks the AND.
		result := applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512)})
		assert.True(t, result.IsEmpty())
	})

	t.Run("different root — no match", func(t *testing.T) {
		result := applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos521)})
		assert.True(t, result.IsEmpty())
	})

	t.Run("different docID — no match", func(t *testing.T) {
		result := applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos711)})
		assert.True(t, result.IsEmpty())
	})

	t.Run("three bitmaps — intersection of all", func(t *testing.T) {
		result := applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511, pos521), roaringset.NewBitmap(pos511, pos711), roaringset.NewBitmap(pos511)})
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("multiple matching docs", func(t *testing.T) {
		result := applyDirectAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511, pos711), roaringset.NewBitmap(pos511, pos711)})
		assert.Equal(t, []uint64{doc5, doc7}, result.ToArray())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a, b := roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511)
		applyDirectAnd([]*sroar.Bitmap{a, b})
		assert.Equal(t, []uint64{pos511}, a.ToArray())
		assert.Equal(t, []uint64{pos511}, b.ToArray())
	})
}

// ---- applyMaskLeafAnd ------------------------------------------------------

func TestApplyMaskLeafAnd(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)
	pos511 := nested.Encode(1, 1, doc5)
	pos512 := nested.Encode(1, 2, doc5) // same root+docID, different leaf
	pos513 := nested.Encode(1, 3, doc5) // same root+docID, yet another leaf
	pos521 := nested.Encode(2, 1, doc5) // different root, same docID
	pos711 := nested.Encode(1, 1, doc7) // same root+leaf, different docID

	t.Run("empty input returns empty", func(t *testing.T) {
		assert.True(t, applyMaskLeafAnd(nil).IsEmpty())
	})

	t.Run("single bitmap returns docIDs", func(t *testing.T) {
		assert.Equal(t, []uint64{doc5}, applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511)}).ToArray())
	})

	t.Run("same raw position — match", func(t *testing.T) {
		result := applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511)})
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("same root+docID different leaf — match (leaf is zeroed before AND)", func(t *testing.T) {
		// This is the key difference from directAnd: leaf bits are erased so
		// positions that differ only in leaf_idx — i.e. siblings within the same
		// element — are treated as identical.
		result := applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512)})
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("three bitmaps same root+docID different leaves — match", func(t *testing.T) {
		result := applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512), roaringset.NewBitmap(pos513)})
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("different root same docID — no match (root encodes element identity)", func(t *testing.T) {
		result := applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos521)})
		assert.True(t, result.IsEmpty())
	})

	t.Run("different docID — no match", func(t *testing.T) {
		result := applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos711)})
		assert.True(t, result.IsEmpty())
	})

	t.Run("multiple matching docs", func(t *testing.T) {
		pos712 := nested.Encode(1, 2, doc7)
		result := applyMaskLeafAnd([]*sroar.Bitmap{roaringset.NewBitmap(pos511, pos711), roaringset.NewBitmap(pos512, pos712)})
		assert.Equal(t, []uint64{doc5, doc7}, result.ToArray())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a, b := roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512)
		applyMaskLeafAnd([]*sroar.Bitmap{a, b})
		assert.Equal(t, []uint64{pos511}, a.ToArray())
		assert.Equal(t, []uint64{pos512}, b.ToArray())
	})
}

// ---- applyIdxLoop preconditions (no lsmkv required) ------------------------

func TestApplyIdxLoopPreconditions(t *testing.T) {
	pos511 := nested.Encode(1, 1, 5)
	pos512 := nested.Encode(1, 2, 5)

	t.Run("nil metaBucket returns error regardless of bitmaps", func(t *testing.T) {
		_, err := applyIdxLoop(context.Background(), nil, "cars", []*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "meta bucket is nil")
		assert.Contains(t, err.Error(), "cars")
	})

	t.Run("nil metaBucket checked before bitmap count", func(t *testing.T) {
		// Even with zero bitmaps the nil-bucket error fires first.
		_, err := applyIdxLoop(context.Background(), nil, "cars", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "meta bucket is nil")
	})
}
