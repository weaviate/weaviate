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
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// planExecutor executes an executionPlan for a correlated nested AND filter.
//
// TODO aliszka:nested_filtering positionBitmaps bitmaps have no associated release
// functions. Bitmaps fetched via fetchRawPositions may be backed by pooled
// buffers that must be returned to the pool after use. Add a releases []func()
// field and defer-call all of them at the end of execute() so pooled buffers
// are correctly returned. See Option A in notes.
type planExecutor struct {
	plan            executionPlan
	positionsByPath map[string]*positionBitmaps
	metaBucket      *lsmkv.Bucket
}

func newPlanExecutor(
	plan executionPlan,
	positionsByPath map[string]*positionBitmaps,
	metaBucket *lsmkv.Bucket,
) *planExecutor {
	return &planExecutor{
		plan:            plan,
		positionsByPath: positionsByPath,
		metaBucket:      metaBucket,
	}
}

// execute runs every condition group and strips position bits to return plain docIDs.
//
// groupAndAllMaskLeaf raw bitmaps are folded directly into the final AndAllMaskLeaf
// call — no intermediate bitmap is produced for those groups. groupAndAll results
// (full-position bitmaps from AndAll) and groupRunIdxLoop results (already
// leaf-masked) are each added as single entries and leaf-masked in the final step.
func (e *planExecutor) execute(ctx context.Context) (*sroar.Bitmap, error) {
	// finalBitmaps collects one entry per groupAndAll/groupRunIdxLoop result, and
	// the raw bitmaps from each groupAndAllMaskLeaf group directly — avoiding the
	// intermediate AndAllMaskLeaf allocation for those groups.
	finalBitmaps := make([]*sroar.Bitmap, 0, len(e.plan))
	for _, g := range e.plan {
		raw, err := e.collectRaw(g.paths)
		if err != nil {
			return nil, err
		}
		switch g.op {
		case groupAndAll:
			finalBitmaps = append(finalBitmaps, invnested.AndAll(raw))
		case groupAndAllMaskLeaf:
			// Fold raw bitmaps directly into the final AndAllMaskLeaf step.
			// The final step masks each bitmap's leaf bits before ANDing, which
			// is identical to running AndAllMaskLeaf here and then re-masking
			// the result (a no-op) in the final step.
			finalBitmaps = append(finalBitmaps, raw...)
		case groupRunIdxLoop:
			if e.metaBucket == nil {
				return nil, fmt.Errorf("execute: meta bucket is nil for idxLoop on %q", g.lcaPath)
			}
			result, err := e.runIdxLoop(ctx, g.lcaPath, raw)
			if err != nil {
				return nil, err
			}
			finalBitmaps = append(finalBitmaps, result)
		default:
			return nil, fmt.Errorf("execute: unhandled group op %d", g.op)
		}
	}
	return invnested.MaskRootLeaf(invnested.AndAllMaskLeaf(finalBitmaps)), nil
}

// collectRaw returns individual raw position bitmaps for all conditions across
// the given paths: tokens for a path are pre-combined with AndAll (they must
// share the exact same leaf position), each independent contributes its own
// raw bitmap.
func (e *planExecutor) collectRaw(paths []string) ([]*sroar.Bitmap, error) {
	var bitmaps []*sroar.Bitmap
	for _, path := range paths {
		positions, ok := e.positionsByPath[path]
		if !ok {
			return nil, fmt.Errorf("collectRaw: no positions for path %q", path)
		}
		if len(positions.tokens) > 0 {
			// Tokens must share the exact same leaf position — combine first.
			bitmaps = append(bitmaps, invnested.AndAll(positions.tokens))
		}
		bitmaps = append(bitmaps, positions.independent...)
	}
	return bitmaps, nil
}

// ---------------------------------------------------------------------------
// runIdxLoop — verifies that all condition bitmaps land in the same element
// of the intermediate object[] identified by lcaPath.
// ---------------------------------------------------------------------------

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
func (e *planExecutor) runIdxLoop(
	ctx context.Context,
	lcaPath string,
	positionBitmaps []*sroar.Bitmap,
) (*sroar.Bitmap, error) {
	if len(positionBitmaps) == 0 {
		return nil, fmt.Errorf("runIdxLoop: no position bitmaps provided for path %q", lcaPath)
	}
	if len(positionBitmaps) == 1 {
		return invnested.MaskLeaf(positionBitmaps[0]), nil
	}

	sorted := slices.Clone(positionBitmaps)
	cards := make([]int, len(sorted))
	for i, bm := range sorted {
		cards[i] = bm.GetCardinality()
	}
	sort.Sort(bitmapsByCard{sorted, cards})

	preFilter := invnested.AndAllMaskLeaf(sorted)
	preFilterCard := preFilter.GetCardinality()
	if preFilterCard == 0 {
		return sroar.NewBitmap(), nil
	}

	if err := ctxExpired(ctx); err != nil {
		return nil, err
	}

	idxPrefix := invnested.PathPrefix("_idx." + lcaPath)
	result := sroar.NewBitmap()

	c := e.metaBucket.CursorRoaringSet()
	defer c.Close()

	for k, elemBitmap := c.Seek(invnested.IdxKey(lcaPath, 0)); k != nil; k, elemBitmap = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, idxPrefix) {
			break
		}
		if elemBitmap.IsEmpty() {
			continue
		}

		if invnested.AndWithMaskLeaf(preFilter, elemBitmap).IsEmpty() {
			continue
		}

		partial := invnested.MaskLeafAnd(sorted[0], elemBitmap)
		empty := partial.IsEmpty()
		for _, bm := range sorted[1:] {
			if empty {
				break
			}
			partial.And(invnested.MaskLeafAnd(bm, elemBitmap))
			empty = partial.IsEmpty()
		}

		if !empty {
			result.Or(partial)
			if result.GetCardinality() >= preFilterCard {
				break
			}
		}
	}

	return result, nil
}
