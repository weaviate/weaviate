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
type planExecutor struct {
	plan            executionPlan
	positionsByPath map[string]*positionBitmaps
	metaBucket      *lsmkv.Bucket
	bitmapOps       *invnested.BitmapOps
	maxConcurrency  int
}

func newPlanExecutor(
	plan executionPlan,
	positionsByPath map[string]*positionBitmaps,
	metaBucket *lsmkv.Bucket,
	bitmapOps *invnested.BitmapOps,
	maxConcurrency int,
) *planExecutor {
	return &planExecutor{
		plan:            plan,
		positionsByPath: positionsByPath,
		metaBucket:      metaBucket,
		bitmapOps:       bitmapOps,
		maxConcurrency:  maxConcurrency,
	}
}

// execute runs every condition group and strips position bits to return plain docIDs.
//
// groupAndAllMaskLeaf raw bitmaps are folded directly into the final AndAllMaskLeaf
// call — no intermediate bitmap is produced for those groups. groupAndAll results
// (full-position bitmaps from AndAll) and groupRunIdxLoop results (already
// leaf-masked) are each added as single entries and leaf-masked in the final step.
func (e *planExecutor) execute(ctx context.Context) (*sroar.Bitmap, func(), error) {
	// intermediateReleases collects pool-buffer releases for all bitmaps
	// created within execute — token-combined bitmaps from collectRaw and
	// groupAndAll results. They are all released after MaskRootLeaf produces
	// the final result, at which point the executor no longer reads them.
	var intermediateReleases []func()
	defer func() {
		for _, rel := range intermediateReleases {
			rel()
		}
	}()

	// finalBitmaps collects one entry per groupAndAll/groupRunIdxLoop result, and
	// the raw bitmaps from each groupAndAllMaskLeaf group directly — avoiding the
	// intermediate AndAllMaskLeaf allocation for those groups.
	finalBitmaps := make([]*sroar.Bitmap, 0, len(e.plan))
	for _, g := range e.plan {
		raw, rawReleases, err := e.collectRaw(g.paths)
		if err != nil {
			return nil, nil, err
		}
		intermediateReleases = append(intermediateReleases, rawReleases...)

		switch g.op {
		case groupAndAll:
			if len(raw) == 1 {
				finalBitmaps = append(finalBitmaps, raw[0])
			} else {
				result, release := e.bitmapOps.AndAll(raw, e.maxConcurrency)
				intermediateReleases = append(intermediateReleases, release)
				finalBitmaps = append(finalBitmaps, result)
			}
		case groupAndAllMaskLeaf:
			// Fold raw bitmaps directly into the final AndAllMaskLeaf step.
			// The final step masks each bitmap's leaf bits before ANDing, which
			// is identical to running AndAllMaskLeaf here and then re-masking
			// the result (a no-op) in the final step.
			finalBitmaps = append(finalBitmaps, raw...)
		case groupRunIdxLoop:
			if e.metaBucket == nil {
				return nil, nil, fmt.Errorf("execute: meta bucket is nil for idxLoop on %q", g.lcaPath)
			}
			result, release, err := e.runIdxLoop(ctx, g.lcaPath, raw)
			if err != nil {
				return nil, nil, err
			}
			intermediateReleases = append(intermediateReleases, release)
			finalBitmaps = append(finalBitmaps, result)
		default:
			return nil, nil, fmt.Errorf("execute: unhandled group op %d", g.op)
		}
	}
	masked, maskedRelease := e.bitmapOps.AndAllMaskLeaf(finalBitmaps, e.maxConcurrency)
	intermediateReleases = append(intermediateReleases, maskedRelease)
	final, finalRelease := e.bitmapOps.MaskRootLeaf(masked)
	return final, finalRelease, nil
}

// collectRaw returns individual raw position bitmaps for all conditions across
// the given paths: tokens for a path are pre-combined with AndAll (they must
// share the exact same leaf position), each independent contributes its own
// raw bitmap.
//
// The second return value contains release functions for the pool-backed
// token-combined bitmaps created here. Independent bitmaps are direct
// references to input bitmaps owned by the caller — no releases for those.
func (e *planExecutor) collectRaw(paths []string) ([]*sroar.Bitmap, []func(), error) {
	var bitmaps []*sroar.Bitmap
	var releases []func()
	for _, path := range paths {
		positions, ok := e.positionsByPath[path]
		if !ok {
			return nil, nil, fmt.Errorf("collectRaw: no positions for path %q", path)
		}
		switch len(positions.tokens) {
		case 0:
			// nothing
		case 1:
			// Single token: no merge needed, reuse directly without cloning.
			bitmaps = append(bitmaps, positions.tokens[0])
		default:
			// Multiple tokens must share the exact same leaf position — combine first.
			combined, release := e.bitmapOps.AndAll(positions.tokens, e.maxConcurrency)
			bitmaps = append(bitmaps, combined)
			releases = append(releases, release)
		}
		bitmaps = append(bitmaps, positions.independent...)
	}
	return bitmaps, releases, nil
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
) (*sroar.Bitmap, func(), error) {
	if len(positionBitmaps) == 0 {
		return nil, nil, fmt.Errorf("runIdxLoop: no position bitmaps provided for path %q", lcaPath)
	}
	if len(positionBitmaps) == 1 {
		result, release := e.bitmapOps.MaskLeaf(positionBitmaps[0])
		return result, release, nil
	}

	sorted := slices.Clone(positionBitmaps)
	cards := make([]int, len(sorted))
	for i, bm := range sorted {
		cards[i] = bm.GetCardinality()
	}
	sort.Sort(bitmapsByCard{sorted, cards})

	preFilter, releasePreFilter := e.bitmapOps.AndAllMaskLeaf(sorted, e.maxConcurrency)
	defer releasePreFilter()

	preFilterCard := preFilter.GetCardinality()
	if preFilterCard == 0 {
		return sroar.NewBitmap(), func() {}, nil
	}

	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}

	idxPrefix := invnested.PathPrefix("_idx." + lcaPath)
	// result accumulates via Or and is bounded by preFilter (result ⊆ preFilter),
	// so preFilter's size is a reliable upper bound for the pool buffer.
	result, releaseResult := e.bitmapOps.NewEmpty(preFilter.LenInBytes())
	succeeded := false
	defer func() {
		if !succeeded {
			releaseResult()
		}
	}()

	c := e.metaBucket.CursorRoaringSet()
	defer c.Close()

	for k, elemBitmap := c.Seek(invnested.IdxKey(lcaPath, 0)); k != nil; k, elemBitmap = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return nil, nil, err
		}
		if !bytes.HasPrefix(k, idxPrefix) {
			break
		}
		if elemBitmap.IsEmpty() {
			continue
		}
		if !e.matchElement(result, sorted, preFilter, elemBitmap) {
			continue
		}
		if result.GetCardinality() >= preFilterCard {
			break
		}
	}

	succeeded = true
	return result, releaseResult, nil
}

// matchElement checks whether all conditions in sorted satisfy the same array
// element (represented by elemBitmap) and, if so, ORs the per-element result
// into acc. Returns true when at least one document matched.
//
// All intermediate pool buffers (pre-check, partial accumulator, per-condition
// temporaries) are released before the method returns, including on panic.
func (e *planExecutor) matchElement(acc *sroar.Bitmap, sorted []*sroar.Bitmap, preFilter, elemBitmap *sroar.Bitmap) bool {
	// Pre-check: skip element if it cannot possibly satisfy all conditions.
	// preFilter is already leaf-masked; IntersectsMaskedLeaf zeroes elemBitmap's
	// leaf bits and checks for overlap without allocating — cheap fast-reject.
	if !e.bitmapOps.IntersectsMaskedLeaf(preFilter, elemBitmap) {
		return false
	}

	// Compute the intersection of all conditions within this element.
	partial, releasePartial := e.bitmapOps.MaskLeafAnd(sorted[0], elemBitmap)
	defer releasePartial()

	for _, bm := range sorted[1:] {
		if partial.IsEmpty() {
			break
		}
		func() {
			inner, releaseInner := e.bitmapOps.MaskLeafAnd(bm, elemBitmap)
			defer releaseInner()
			partial.AndConc(inner, e.maxConcurrency)
		}()
	}

	if partial.IsEmpty() {
		return false
	}
	acc.OrConc(partial, e.maxConcurrency)
	return true
}
