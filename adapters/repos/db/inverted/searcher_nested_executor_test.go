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
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// ---------------------------------------------------------------------------
// Plan-building helpers
// ---------------------------------------------------------------------------

// andAllPlan builds a groupAndAll plan from the given bitmaps.
// Paths are auto-generated as "p1", "p2", …
func andAllPlan(bitmaps ...*sroar.Bitmap) (executionPlan, map[string]*positionBitmaps) {
	paths := make([]string, len(bitmaps))
	positionsByPath := make(map[string]*positionBitmaps, len(bitmaps))
	for i, bm := range bitmaps {
		path := fmt.Sprintf("p%d", i+1)
		paths[i] = path
		positionsByPath[path] = &positionBitmaps{independent: []*sroar.Bitmap{bm}}
	}
	return executionPlan{{op: groupAndAll, paths: paths}}, positionsByPath
}

// andAllMaskLeafPlan builds a groupAndAllMaskLeaf plan from the given bitmaps.
func andAllMaskLeafPlan(bitmaps ...*sroar.Bitmap) (executionPlan, map[string]*positionBitmaps) {
	paths := make([]string, len(bitmaps))
	positionsByPath := make(map[string]*positionBitmaps, len(bitmaps))
	for i, bm := range bitmaps {
		path := fmt.Sprintf("p%d", i+1)
		paths[i] = path
		positionsByPath[path] = &positionBitmaps{independent: []*sroar.Bitmap{bm}}
	}
	return executionPlan{{op: groupAndAllMaskLeaf, paths: paths}}, positionsByPath
}

// idxLoopPlan builds a groupRunIdxLoop plan from the given bitmaps.
func idxLoopPlan(lcaPath string, bitmaps ...*sroar.Bitmap) (executionPlan, map[string]*positionBitmaps) {
	paths := make([]string, len(bitmaps))
	positionsByPath := make(map[string]*positionBitmaps, len(bitmaps))
	for i, bm := range bitmaps {
		path := fmt.Sprintf("p%d", i+1)
		paths[i] = path
		positionsByPath[path] = &positionBitmaps{independent: []*sroar.Bitmap{bm}}
	}
	return executionPlan{{op: groupRunIdxLoop, lcaPath: lcaPath, paths: paths}}, positionsByPath
}

// exec is a shorthand for executing a plan with no meta bucket.
func exec(t *testing.T, plan executionPlan, bitmapsByPath map[string]*positionBitmaps) *sroar.Bitmap {
	t.Helper()
	result, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
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
	pos511 := invnested.Encode(1, 1, doc5) // root=1 leaf=1 docID=5
	pos512 := invnested.Encode(1, 2, doc5) // same root+docID, different leaf
	pos521 := invnested.Encode(2, 1, doc5) // different root
	pos711 := invnested.Encode(1, 1, doc7) // different doc

	t.Run("empty input returns empty", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan()
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("single bitmap returns docIDs", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan(roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("exact same raw position in both bitmaps — match", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("same root+docID different leaf — no match (leaf distinguishes sub-elements)", func(t *testing.T) {
		// directAnd uses AndAll (exact raw positions). Two conditions at different
		// leaf positions represent different sub-elements of the same parent
		// (e.g. make in cars[0] vs model in cars[1]). Raw AND correctly excludes
		// them; masking leaves would lose this distinction and produce a false match.
		plan, bitmapsByPath := andAllPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("different root — no match", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos521))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("different docID — no match", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos711))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("three bitmaps — intersection of all", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan(
			roaringset.NewBitmap(pos511, pos521),
			roaringset.NewBitmap(pos511, pos711),
			roaringset.NewBitmap(pos511),
		)
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("multiple matching docs", func(t *testing.T) {
		plan, bitmapsByPath := andAllPlan(
			roaringset.NewBitmap(pos511, pos711),
			roaringset.NewBitmap(pos511, pos711),
		)
		assert.Equal(t, []uint64{doc5, doc7}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a := roaringset.NewBitmap(pos511)
		b := roaringset.NewBitmap(pos511)
		plan, bitmapsByPath := andAllPlan(a, b)
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
	pos511 := invnested.Encode(1, 1, doc5)
	pos512 := invnested.Encode(1, 2, doc5) // same root+docID, different leaf
	pos513 := invnested.Encode(1, 3, doc5) // same root+docID, yet another leaf
	pos521 := invnested.Encode(2, 1, doc5) // different root, same docID
	pos711 := invnested.Encode(1, 1, doc7) // same root+leaf, different docID

	t.Run("single bitmap returns docIDs", func(t *testing.T) {
		plan, bitmapsByPath := andAllMaskLeafPlan(roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("same raw position — match", func(t *testing.T) {
		plan, bitmapsByPath := andAllMaskLeafPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos511))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("same root+docID different leaf — match (leaf is zeroed before AND)", func(t *testing.T) {
		plan, bitmapsByPath := andAllMaskLeafPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512))
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("three bitmaps same root+docID different leaves — match", func(t *testing.T) {
		plan, bitmapsByPath := andAllMaskLeafPlan(
			roaringset.NewBitmap(pos511),
			roaringset.NewBitmap(pos512),
			roaringset.NewBitmap(pos513),
		)
		assert.Equal(t, []uint64{doc5}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("different root same docID — no match (root encodes element identity)", func(t *testing.T) {
		plan, bitmapsByPath := andAllMaskLeafPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos521))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("different docID — no match", func(t *testing.T) {
		plan, bitmapsByPath := andAllMaskLeafPlan(roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos711))
		assert.True(t, exec(t, plan, bitmapsByPath).IsEmpty())
	})

	t.Run("multiple matching docs", func(t *testing.T) {
		pos712 := invnested.Encode(1, 2, doc7)
		plan, bitmapsByPath := andAllMaskLeafPlan(
			roaringset.NewBitmap(pos511, pos711),
			roaringset.NewBitmap(pos512, pos712),
		)
		assert.Equal(t, []uint64{doc5, doc7}, exec(t, plan, bitmapsByPath).ToArray())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a := roaringset.NewBitmap(pos511)
		b := roaringset.NewBitmap(pos512)
		plan, bitmapsByPath := andAllMaskLeafPlan(a, b)
		exec(t, plan, bitmapsByPath)
		assert.Equal(t, []uint64{pos511}, a.ToArray())
		assert.Equal(t, []uint64{pos512}, b.ToArray())
	})
}

// ---------------------------------------------------------------------------
// preconditions (no lsmkv required)
// ---------------------------------------------------------------------------

func TestExecuteResolutionPlanPreconditions(t *testing.T) {
	pos511 := invnested.Encode(1, 1, 5)
	pos512 := invnested.Encode(1, 2, 5)

	t.Run("groupAndAll: two independents at different leaves — no match (raw AndAll, leaf-aware)", func(t *testing.T) {
		// groupAndAll uses raw AndAll. Two bitmaps at DIFFERENT leaf positions
		// represent conditions in DIFFERENT sub-elements — they should NOT match.
		plan := executionPlan{{op: groupAndAll, paths: []string{"p1"}}}
		bitmapsByPath := map[string]*positionBitmaps{
			"p1": {independent: []*sroar.Bitmap{
				roaringset.NewBitmap(pos511),
				roaringset.NewBitmap(pos512),
			}},
		}
		result, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.NoError(t, err)
		assert.True(t, result.IsEmpty())
	})

	t.Run("groupRunIdxLoop: nil metaBucket returns error", func(t *testing.T) {
		plan, bitmapsByPath := idxLoopPlan("cars",
			roaringset.NewBitmap(pos511),
			roaringset.NewBitmap(pos512),
		)
		_, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "meta bucket is nil")
	})

	t.Run("collectRaw: missing path in positionsByPath returns error", func(t *testing.T) {
		plan := executionPlan{{op: groupAndAll, paths: []string{"p1", "p_missing"}}}
		bitmapsByPath := map[string]*positionBitmaps{
			"p1": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos511)}},
			// "p_missing" intentionally absent
		}
		_, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "p_missing")
	})
}

// ---------------------------------------------------------------------------
// Multi-group execution
// ---------------------------------------------------------------------------

func TestExecuteMultiGroupPlan(t *testing.T) {
	const (
		doc5 = uint64(5)
		doc7 = uint64(7)
	)
	pos511 := invnested.Encode(1, 1, doc5) // root=1, leaf=1, doc=5
	pos521 := invnested.Encode(2, 1, doc5) // root=2, leaf=1, doc=5
	pos711 := invnested.Encode(1, 1, doc7) // root=1, leaf=1, doc=7
	pos721 := invnested.Encode(2, 1, doc7) // root=2, leaf=1, doc=7

	t.Run("two groupAndAll groups — cross-group AndAllMaskLeaf aligns on root+docID", func(t *testing.T) {
		// Group 1 (addresses): city bitmap at pos511 (root=1, doc=5) and pos711 (root=1, doc=7)
		// Group 2 (cars):      make bitmap at pos521 (root=2, doc=5) and pos721 (root=2, doc=7)
		// After execute: group1→AndAll([city])=cityBm; group2→AndAll([make])=makeBm
		// Final: AndAllMaskLeaf([cityBm,makeBm]) masks both to root+docID level, then AND.
		// city: root=1 for both docs; make: root=2 for both docs
		// → {E(1,0,doc5),E(1,0,doc7)} ∩ {E(2,0,doc5),E(2,0,doc7)} = {} — no matching root.
		// If city and make were in the SAME root element they would match.
		plan := executionPlan{
			{op: groupAndAll, paths: []string{"city"}},
			{op: groupAndAll, paths: []string{"make"}},
		}
		bitmapsByPath := map[string]*positionBitmaps{
			"city": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos511, pos711)}},
			"make": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos521, pos721)}},
		}
		result, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.NoError(t, err)
		assert.True(t, result.IsEmpty()) // different root → cross-group AND gives empty
	})

	t.Run("two groupAndAll groups — same root matches across groups", func(t *testing.T) {
		// city and make are both in root=1 for doc5.
		// Group1→AndAll([city])={E(1,1,doc5)}; Group2→AndAll([make])={E(1,1,doc5)}
		// AndAllMaskLeaf → both mask to E(1,0,doc5) → match → MaskRootLeaf → {doc5}
		plan := executionPlan{
			{op: groupAndAll, paths: []string{"city"}},
			{op: groupAndAll, paths: []string{"make"}},
		}
		bitmapsByPath := map[string]*positionBitmaps{
			"city": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos511)}},
			"make": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos511)}},
		}
		result, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})

	t.Run("groupAndAll + groupAndAllMaskLeaf — masked result aligned at root+docID", func(t *testing.T) {
		// Group1 (scalar, raw): city at root=1, leaf=1 for doc5
		// Group2 (masked, multi-ind): tags at root=1 leaf=1 AND leaf=2 for doc5
		// Group2→AndAllMaskLeaf([tag1,tag2])={E(1,0,doc5)}
		// Final: AndAllMaskLeaf([{E(1,1,doc5)},{E(1,0,doc5)}]) = both mask to E(1,0,doc5) → doc5
		pos512 := invnested.Encode(1, 2, doc5)
		plan := executionPlan{
			{op: groupAndAll, paths: []string{"city"}},
			{op: groupAndAllMaskLeaf, paths: []string{"tags"}},
		}
		bitmapsByPath := map[string]*positionBitmaps{
			"city": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos511)}},
			"tags": {independent: []*sroar.Bitmap{roaringset.NewBitmap(pos511), roaringset.NewBitmap(pos512)}},
		}
		result, err := newPlanExecutor(plan, bitmapsByPath, nil).execute(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []uint64{doc5}, result.ToArray())
	})
}
