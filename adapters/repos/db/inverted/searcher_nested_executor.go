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

// applyDirectAnd intersects all raw position bitmaps and strips to docIDs.
// Implements the directAnd operation: positions are naturally aligned so a
// plain bitmap AND followed by MaskRootLeaf gives correct results.
func applyDirectAnd(positionBitmaps []*sroar.Bitmap) *sroar.Bitmap {
	return nested.MaskRootLeaf(nested.AndAll(positionBitmaps))
}

// applyMaskLeafAnd zeroes leaf bits on each bitmap, ANDs them, and strips to
// docIDs. Implements the maskLeafAnd operation: aligns on root+docID so paths
// from different subtrees or a single-object LCA are correctly combined.
func applyMaskLeafAnd(positionBitmaps []*sroar.Bitmap) *sroar.Bitmap {
	return nested.MaskRootLeaf(nested.AndAllMaskLeaf(positionBitmaps))
}

// applyIdxLoop implements the idxLoopAnd operation. It iterates over all
// _idx.{lcaPath}[N] entries in the meta bucket and verifies that all
// conditions fall under the same array element N before accumulating results.
//
// Optimisations applied:
//  1. Conditions are sorted by cardinality (most selective first) so the
//     early-exit check triggers as soon as possible in the inner loop.
//  2. MaskLeaf is computed once per condition before the loop and
//     reused for the global pre-filter, avoiding repeated masking.
//  3. Per-element pre-check: preFilter AND maskedElem is computed (1 alloc)
//     before the expensive K-condition processing (2K allocs). Elements where
//     no document can match all conditions are skipped cheaply. Because
//     preFilter ⊆ maskedBitmaps[i] for all i, this also implies each
//     individual condition overlaps the element — no per-condition skip is
//     needed inside the inner loop.
//  4. Early termination when result already contains all documents in preFilter.
//
// Algorithm:
//
//	maskedBitmaps[i] = MaskLeaf(positionBitmaps[i])  for each i
//	preFilter = AND of all maskedBitmaps
//	if preFilter empty → return empty
//	for each _idx.{lcaPath}[N] entry:
//	    maskedElem = MaskLeaf(elem[N])
//	    if preFilter AND maskedElem empty → skip element cheaply
//	    partial = MaskLeaf(bm[0] AND elem[N]) AND ...
//	    result = result OR partial
//	    if result covers all docs in preFilter → stop early
//	return MaskRootLeaf(result)
func applyIdxLoop(
	ctx context.Context,
	metaBucket *lsmkv.Bucket,
	lcaPath string,
	positionBitmaps []*sroar.Bitmap,
) (*sroar.Bitmap, error) {
	if metaBucket == nil {
		return nil, fmt.Errorf("applyIdxLoop: meta bucket is nil for path %q", lcaPath)
	}
	if len(positionBitmaps) == 0 {
		return nil, fmt.Errorf("applyIdxLoop: no position bitmaps provided for path %q", lcaPath)
	}
	if len(positionBitmaps) == 1 {
		return nested.MaskRootLeaf(positionBitmaps[0]), nil
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

	c := metaBucket.CursorRoaringSet()
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

	return nested.MaskRootLeaf(result), nil
}
