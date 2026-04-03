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
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// bitmapsByCard implements sort.Interface to sort a bitmap slice and its
// precomputed cardinality slice together in a single pass, avoiding repeated
// GetCardinality calls during comparison.
type bitmapsByCard struct {
	bitmaps []*sroar.Bitmap
	cards   []int
}

func (s bitmapsByCard) Len() int           { return len(s.bitmaps) }
func (s bitmapsByCard) Less(i, j int) bool { return s.cards[i] < s.cards[j] }
func (s bitmapsByCard) Swap(i, j int) {
	s.bitmaps[i], s.bitmaps[j] = s.bitmaps[j], s.bitmaps[i]
	s.cards[i], s.cards[j] = s.cards[j], s.cards[i]
}

// runIdxLoop iterates _idx.{lcaPath}[N] entries and verifies all conditions
// fall within the same array element. Returns a root+docID bitmap (leaf bits
// zeroed, root bits preserved); the caller applies MaskRootLeaf if needed.
//
// Optimisations applied:
//  1. Conditions are sorted by cardinality (most selective first) so the
//     early-exit check triggers as soon as possible in the inner loop.
//  2. MaskLeaf is computed once per condition before the loop and
//     reused for the global pre-filter, avoiding repeated masking.
//  3. Per-element pre-check: preFilter AND maskedElem is computed (1 alloc)
//     before the expensive K-condition processing (2K allocs). Elements where
//     no document can match all conditions are skipped cheaply. Because
//     preFilter ⊆ MaskLeaf(sorted[i]) for all i, this also implies each
//     individual condition overlaps the element — no per-condition skip is
//     needed inside the inner loop.
//  4. Early termination when result already contains all documents in preFilter.
//
// Algorithm:
//
//	preFilter = AndAllMaskLeaf(sorted)
//	if preFilter empty → return empty
//	for each _idx.{lcaPath}[N] entry:
//	    if preFilter AND MaskLeaf(elem[N]) empty → skip element cheaply
//	    partial = MaskLeafAnd(bm[0], elem[N]) AND MaskLeafAnd(bm[1], elem[N]) AND ...
//	    result = result OR partial
//	    if result covers all docs in preFilter → stop early
//	return result  (root+docID; caller applies MaskRootLeaf if needed)
func (e *resolutionPlanExecutor) runIdxLoop(
	ctx context.Context,
	lcaPath string,
	positionBitmaps []*sroar.Bitmap,
) (*sroar.Bitmap, error) {
	if len(positionBitmaps) == 0 {
		return nil, fmt.Errorf("runIdxLoop: no position bitmaps provided for path %q", lcaPath)
	}
	if len(positionBitmaps) == 1 {
		return nested.MaskLeaf(positionBitmaps[0]), nil
	}

	// Optimisation 1: sort conditions by cardinality ascending so the most
	// selective condition is checked first and triggers early exit sooner.
	// Cardinalities are precomputed into a parallel slice so GetCardinality
	// (O(containers)) is called exactly once per bitmap rather than O(n log n)
	// times during sorting. Both slices are kept in sync via bitmapsByCard.
	sorted := slices.Clone(positionBitmaps)
	cards := make([]int, len(sorted))
	for i, bm := range sorted {
		cards[i] = bm.GetCardinality()
	}
	sort.Sort(bitmapsByCard{sorted, cards})

	// Optimisation 2: build global pre-filter as the AND of all leaf-masked
	// conditions. Using the sorted order means the most selective bitmap is
	// ANDed first, shrinking the result fastest and reducing subsequent work.
	preFilter := nested.AndAllMaskLeaf(sorted)
	preFilterCard := preFilter.GetCardinality()
	if preFilterCard == 0 {
		return sroar.NewBitmap(), nil
	}

	if err := ctxExpired(ctx); err != nil {
		return nil, err
	}

	idxPrefix := nested.PathPrefix("_idx." + lcaPath)
	result := sroar.NewBitmap()

	c := e.metaBucket.CursorRoaringSet()
	defer c.Close()

	for k, elemBitmap := c.Seek(nested.IdxKey(lcaPath, 0)); k != nil; k, elemBitmap = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, idxPrefix) {
			break
		}
		if elemBitmap.IsEmpty() {
			continue
		}

		// Optimisation 3: per-element pre-check using the precomputed masked
		// element. One allocation to skip the entire K-condition processing when
		// no document has all conditions satisfied within this element.
		if nested.AndWithMaskLeaf(preFilter, elemBitmap).IsEmpty() {
			continue
		}

		// Full K-condition processing: intersect each condition with element N.
		// No per-condition pre-check is needed: the global pre-check above
		// guarantees every condition overlaps this element, because
		// preFilter ⊆ MaskLeaf(sorted[i]) for all i.
		partial := nested.MaskLeafAnd(sorted[0], elemBitmap)
		empty := partial.IsEmpty()
		for _, bm := range sorted[1:] {
			if empty {
				break
			}
			partial.And(nested.MaskLeafAnd(bm, elemBitmap))
			empty = partial.IsEmpty()
		}

		if !empty {
			result.Or(partial)
			// Optimisation 4: stop early if result already covers all docs that
			// preFilter could yield — further elements can only add duplicates.
			if result.GetCardinality() >= preFilterCard {
				break
			}
		}
	}

	return result, nil
}

// resolutionPlanExecutor executes a resolutionPlan for a correlated nested AND.
// positionsByPath maps each relative nested path to its pre-computed raw position
// bitmap. metaBucket is the _idx meta bucket for the root nested property; it
// may be nil when no idxLoopAnd node is in the plan.
//
// TODO aliszka:nested_filtering positionBitmaps bitmaps have no associated release
// functions. Bitmaps fetched via fetchRawPositions may be backed by pooled
// buffers that must be returned to the pool after use. Add a releases []func()
// field and defer-call all of them at the end of execute() so pooled buffers
// are correctly returned. See Option A in notes.
type resolutionPlanExecutor struct {
	plan          *resolutionPlan
	positionsByPath map[string]*positionBitmaps
	metaBucket    *lsmkv.Bucket
}

// newResolutionPlanExecutor returns an executor ready to run a resolutionPlan.
func newResolutionPlanExecutor(
	plan *resolutionPlan,
	positionsByPath map[string]*positionBitmaps,
	metaBucket *lsmkv.Bucket,
) *resolutionPlanExecutor {
	return &resolutionPlanExecutor{plan: plan, positionsByPath: positionsByPath, metaBucket: metaBucket}
}

// execute runs the plan and returns a docID-only bitmap.
// MaskRootLeaf is applied once at the top; all recursive calls work with
// partially-preserved position bits.
func (e *resolutionPlanExecutor) execute(ctx context.Context) (*sroar.Bitmap, error) {
	bm, err := e.executeNode(ctx, e.plan)
	if err != nil {
		return nil, err
	}
	return nested.MaskRootLeaf(bm), nil
}

// executeNode dispatches to the appropriate handler based on plan.op.
// Each handler owns both the interior-groups and leaf-paths cases for its op.
func (e *resolutionPlanExecutor) executeNode(ctx context.Context, plan *resolutionPlan) (*sroar.Bitmap, error) {
	switch plan.op {
	case directAnd:
		return e.executeDirectAnd(plan)
	case maskLeafAnd:
		return e.executeMaskLeafAnd(ctx, plan)
	case idxLoopAnd:
		return e.executeIdxLoopAnd(ctx, plan)
	}
	return nil, fmt.Errorf("executeNode: unhandled op %d", plan.op)
}

// executeDirectAnd handles directAnd nodes. Positions are naturally aligned
// (scalar siblings or ancestor/descendant), so a plain AND suffices.
// directAnd is always a leaf node — no groups.
func (e *resolutionPlanExecutor) executeDirectAnd(plan *resolutionPlan) (*sroar.Bitmap, error) {
	bitmaps, err := e.fetchBitmaps(plan.paths)
	if err != nil {
		return nil, err
	}
	return nested.AndAll(bitmaps), nil
}

// executeMaskLeafAnd handles maskLeafAnd nodes. Zeros the leaf bits of each
// input bitmap and ANDs them, aligning on root+docID. Handles both interior
// nodes (sub-groups are recursively executed) and leaf nodes (paths fetched).
func (e *resolutionPlanExecutor) executeMaskLeafAnd(ctx context.Context, plan *resolutionPlan) (*sroar.Bitmap, error) {
	bitmaps, err := e.collectBitmaps(ctx, plan)
	if err != nil {
		return nil, err
	}
	return nested.AndAllMaskLeaf(bitmaps), nil
}

// executeIdxLoopAnd handles idxLoopAnd nodes. Verifies that all conditions land
// within the same array element by iterating _idx.{lcaPath}[N] meta entries.
// Handles both interior nodes (sub-groups executed first) and leaf nodes
// (paths fetched directly). Returns root+docID.
func (e *resolutionPlanExecutor) executeIdxLoopAnd(ctx context.Context, plan *resolutionPlan) (*sroar.Bitmap, error) {
	if e.metaBucket == nil {
		return nil, fmt.Errorf("executeIdxLoopAnd: meta bucket is nil for path %q", plan.lcaPath)
	}
	bitmaps, err := e.collectBitmaps(ctx, plan)
	if err != nil {
		return nil, err
	}
	return e.runIdxLoop(ctx, plan.lcaPath, bitmaps)
}

// collectBitmaps executes each sub-group of an interior maskLeafAnd or
// idxLoopAnd node and returns the resulting bitmaps. maskLeafAnd and
// idxLoopAnd nodes always have groups — flat paths are only used by directAnd,
// which never calls this method.
func (e *resolutionPlanExecutor) collectBitmaps(ctx context.Context, plan *resolutionPlan) ([]*sroar.Bitmap, error) {
	if len(plan.groups) == 0 {
		return nil, fmt.Errorf("collectBitmaps: %d node has no groups", plan.op)
	}
	bitmaps := make([]*sroar.Bitmap, 0, len(plan.groups))
	for _, group := range plan.groups {
		bm, err := e.executeNode(ctx, group)
		if err != nil {
			return nil, err
		}
		bitmaps = append(bitmaps, bm)
	}
	return bitmaps, nil
}

// fetchBitmaps returns one combined position bitmap per path by calling
// combinePositionBitmaps. All merging logic is encapsulated here in the executor.
func (e *resolutionPlanExecutor) fetchBitmaps(paths []string) ([]*sroar.Bitmap, error) {
	bitmaps := make([]*sroar.Bitmap, 0, len(paths))
	for _, path := range paths {
		positions, ok := e.positionsByPath[path]
		if !ok {
			return nil, fmt.Errorf("fetchBitmaps: no positions for path %q", path)
		}
		bitmaps = append(bitmaps, combinePositionBitmaps(positions))
	}
	return bitmaps, nil
}

// combinePositionBitmaps merges the pre-fetched bitmaps in a positionBitmaps into one bitmap:
//   - tokens are combined with AndAll — all tokens must share the same leaf
//     position (multi-token text values like "New York" → ["new", "york"]).
//   - A single independent bitmap is returned raw — no combining needed.
//   - Multiple independent bitmaps are combined with MaskLeafAndAll — the
//     values may be at different leaf positions (scalar array elements) but
//     must belong to the same parent element (same root_idx).
//   - When both tokens and independent bitmaps are present, the two partial
//     results are aligned at root+docID level via MaskLeafAndAll.
func combinePositionBitmaps(positions *positionBitmaps) *sroar.Bitmap {
	var tokensResult *sroar.Bitmap
	if len(positions.tokens) > 0 {
		tokensResult = nested.AndAll(positions.tokens)
	}

	var independentResult *sroar.Bitmap
	switch len(positions.independent) {
	case 0:
		// nothing
	case 1:
		independentResult = positions.independent[0] // single value: keep raw
	default:
		independentResult = nested.AndAllMaskLeaf(positions.independent)
	}

	switch {
	case tokensResult == nil:
		return independentResult
	case independentResult == nil:
		return tokensResult
	default:
		// Both present: align at root+docID so they can be in different
		// leaf positions within the same parent element.
		return nested.AndAllMaskLeaf([]*sroar.Bitmap{tokensResult, independentResult})
	}
}
