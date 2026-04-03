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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// ---------------------------------------------------------------------------
// Plan-building helpers
// ---------------------------------------------------------------------------

// directAndPlan builds a directAnd plan and bitmapsByPath from the given
// bitmaps. Paths are auto-generated as "p1", "p2", …
func directAndPlan(bitmaps ...*sroar.Bitmap) (*resolutionPlan, map[string]*positionBitmaps) {
	paths := make([]string, len(bitmaps))
	positionsByPath := make(map[string]*positionBitmaps, len(bitmaps))
	for i, bm := range bitmaps {
		path := fmt.Sprintf("p%d", i+1)
		paths[i] = path
		positionsByPath[path] = &positionBitmaps{independent: []*sroar.Bitmap{bm}}
	}
	return &resolutionPlan{op: directAnd, paths: paths}, positionsByPath
}

// maskLeafAndPlan builds a maskLeafAnd interior plan where each bitmap becomes
// one directAnd sub-group.
func maskLeafAndPlan(bitmaps ...*sroar.Bitmap) (*resolutionPlan, map[string]*positionBitmaps) {
	groups := make([]*resolutionPlan, len(bitmaps))
	positionsByPath := make(map[string]*positionBitmaps, len(bitmaps))
	for i, bm := range bitmaps {
		path := fmt.Sprintf("p%d", i+1)
		groups[i] = &resolutionPlan{op: directAnd, paths: []string{path}}
		positionsByPath[path] = &positionBitmaps{independent: []*sroar.Bitmap{bm}}
	}
	return &resolutionPlan{op: maskLeafAnd, groups: groups}, positionsByPath
}

// idxLoopAndPlan builds an idxLoopAnd plan where each bitmap becomes one
// directAnd sub-group.
func idxLoopAndPlan(lcaPath string, bitmaps ...*sroar.Bitmap) (*resolutionPlan, map[string]*positionBitmaps) {
	groups := make([]*resolutionPlan, len(bitmaps))
	positionsByPath := make(map[string]*positionBitmaps, len(bitmaps))
	for i, bm := range bitmaps {
		path := fmt.Sprintf("p%d", i+1)
		groups[i] = &resolutionPlan{op: directAnd, paths: []string{path}}
		positionsByPath[path] = &positionBitmaps{independent: []*sroar.Bitmap{bm}}
	}
	return &resolutionPlan{op: idxLoopAnd, lcaPath: lcaPath, groups: groups}, positionsByPath
}

// exec is a shorthand for executeResolutionPlan with no meta bucket.
func exec(t *testing.T, plan *resolutionPlan, bitmapsByPath map[string]*positionBitmaps) *sroar.Bitmap {
	t.Helper()
	result, err := newResolutionPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
	require.NoError(t, err)
	return result
}

// ---------------------------------------------------------------------------
// directAnd
// ---------------------------------------------------------------------------

func TestExecuteResolutionPlanDirectAnd(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)
	pos511 := nested.Encode(1, 1, doc5) // root=1 leaf=1 docID=5
	pos512 := nested.Encode(1, 2, doc5) // same root+docID, different leaf
	pos521 := nested.Encode(2, 1, doc5) // different root
	pos711 := nested.Encode(1, 1, doc7) // different doc

	t.Run("empty input returns empty", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan()
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("single bitmap returns docIDs", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("exact same raw position in both bitmaps — match", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("same root+docID different leaf — no match (requires exact position)", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("different root — no match", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos521))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("different docID — no match", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos711))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("three bitmaps — intersection of all", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(
			roaringset.NewBitmap(pos511, pos521),
			roaringset.NewBitmap(pos511, pos711),
			roaringset.NewBitmap(pos511),
		)
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("multiple matching docs", func(t *testing.T) {
		plan, bitmapsByPath := directAndPlan(
			roaringset.NewBitmap(pos511, pos711),
			roaringset.NewBitmap(pos511, pos711),
		)
		assert.Equal(t, []uint64{doc5, doc7}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a := roaringset.NewBitmap(pos511)
		b := roaringset.NewBitmap(pos511)
		plan, bitmapsByPath := directAndPlan(a, b)
		exec(t, plan, bitmapsByPath)
		assert.Equal(t, []uint64{pos511}, a.ToArray())
		assert.Equal(t, []uint64{pos511}, b.ToArray())
	})
}

// ---------------------------------------------------------------------------
// maskLeafAnd
// ---------------------------------------------------------------------------

func TestExecuteResolutionPlanMaskLeafAnd(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)
	pos511 := nested.Encode(1, 1, doc5)
	pos512 := nested.Encode(1, 2, doc5) // same root+docID, different leaf
	pos513 := nested.Encode(1, 3, doc5) // same root+docID, yet another leaf
	pos521 := nested.Encode(2, 1, doc5) // different root, same docID
	pos711 := nested.Encode(1, 1, doc7) // same root+leaf, different docID

	t.Run("single bitmap returns docIDs", func(t *testing.T) {
		plan, bitmapsByPath := maskLeafAndPlan(roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("same raw position — match", func(t *testing.T) {
		plan, bitmapsByPath := maskLeafAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("same root+docID different leaf — match (leaf is zeroed before AND)", func(t *testing.T) {
		plan, bitmapsByPath := maskLeafAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("three bitmaps same root+docID different leaves — match", func(t *testing.T) {
		plan, bitmapsByPath := maskLeafAndPlan(
			roaringset.NewBitmap(pos511),
			roaringset.NewBitmap(pos512),
			roaringset.NewBitmap(pos513),
		)
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("different root same docID — no match (root encodes element identity)", func(t *testing.T) {
		plan, bitmapsByPath := maskLeafAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos521))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("different docID — no match", func(t *testing.T) {
		plan, bitmapsByPath := maskLeafAndPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos711))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("multiple matching docs", func(t *testing.T) {
		pos712 := nested.Encode(1, 2, doc7)
		plan, bitmapsByPath := maskLeafAndPlan(
			roaringset.NewBitmap(pos511, pos711),
			roaringset.NewBitmap(pos512, pos712),
		)
		assert.Equal(t, []uint64{doc5, doc7}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a := roaringset.NewBitmap(pos511)
		b := roaringset.NewBitmap(pos512)
		plan, bitmapsByPath := maskLeafAndPlan(a, b)
		exec(t, plan, bitmapsByPath)
		assert.Equal(t, []uint64{pos511}, a.ToArray())
		assert.Equal(t, []uint64{pos512}, b.ToArray())
	})
}

// ---------------------------------------------------------------------------
// idxLoopAnd preconditions (no lsmkv required)
// ---------------------------------------------------------------------------

func TestExecuteResolutionPlanIdxLoopPreconditions(t *testing.T) {
	pos511 := nested.Encode(1, 1, 5)
	pos512 := nested.Encode(1, 2, 5)

	t.Run("nil metaBucket returns error", func(t *testing.T) {
		plan, bitmapsByPath := idxLoopAndPlan("cars",
			roaringset.NewBitmap(pos511),
			roaringset.NewBitmap(pos512),
		)
		_, err := newResolutionPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "meta bucket is nil")
		assert.Contains(t, err.Error(), "cars")
	})
}
