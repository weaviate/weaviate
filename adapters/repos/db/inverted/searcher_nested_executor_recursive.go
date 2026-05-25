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

	"github.com/weaviate/sroar"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// recExclude carries an IsNull=true raw _exists.{path} bitmap together with the
// deepest object[] LCA of the leaf it was fetched for. lcaPath drives where the
// exclude is applied: when lcaPath ∈ planLCAs the exclude is subtracted at raw
// level inside the matching group(s) (per-element, leaf-precise per §8.5).
// Otherwise it is subtracted at rootDoc level by execute() — root_idx then
// encodes the exclude's outer scope, so per-(root,docID) granularity matches.
type recExclude struct {
	bitmap  *sroar.Bitmap
	lcaPath string
}

// recExecutor evaluates a recPlanNode tree to produce the docID bitmap of
// matching documents.
//
// Each leaf condition in the plan (a *propValuePair appearing in some
// recGroupNode.here) maps to a raw position bitmap via rawsByCond. The
// metaBucket is used to read _idx entries when iterating array elements
// (recGroupNode at intermediate scope) or pinning to a specific element
// (recSplitNode branches).
//
// excludes and rootAnchor handle IsNull=true and the no-positive case.
// excludes carry raw _exists.{path} bitmaps plus the leaf's deepest object[]
// LCA. When the LCA matches a group's lcaPath in the plan (planLCAs), the
// exclude is subtracted raw-level inside that group — preserving leaf-precise
// per-element semantics for §8.5 sibling-element negation. Otherwise it is
// subtracted at rootDoc level via MaskLeaf+AndNot in execute(). rootAnchor is
// the seed for the no-positive path: when plan is nil, the executor clones
// the anchor, AndNots excludes at raw level (preserving leaf alignment), then
// MaskLeaf to produce a rootDoc bitmap.
//
// Position lifecycle: every bitmap returned to a caller comes with a release
// function. Internal intermediates are released before this method returns.
type recExecutor struct {
	rawsByCond     map[*propValuePair]*sroar.Bitmap
	excludes       []recExclude
	planLCAs       map[string]struct{}
	rootAnchor     *sroar.Bitmap
	metaBucket     *lsmkv.Bucket
	bitmapOps      *invnested.BitmapOps
	maxConcurrency int
	returnMasked   bool
}

func newRecExecutor(
	rawsByCond map[*propValuePair]*sroar.Bitmap,
	metaBucket *lsmkv.Bucket,
	bitmapOps *invnested.BitmapOps,
	maxConcurrency int,
) *recExecutor {
	return &recExecutor{
		rawsByCond:     rawsByCond,
		metaBucket:     metaBucket,
		bitmapOps:      bitmapOps,
		maxConcurrency: maxConcurrency,
	}
}

// withExcludes attaches IsNull=true raw position bitmaps to the executor.
// Bitmaps are caller-owned; the executor reads them but does not release.
// Each entry carries the leaf's deepest object[] LCA so execute() can route
// raw-level subtraction (handled inside groups) vs rootDoc subtraction.
func (e *recExecutor) withExcludes(excludes []recExclude) *recExecutor {
	e.excludes = excludes
	return e
}

// withPlanLCAs records the set of lcaPaths visited by recGroupNode entries in
// the plan. An exclude whose lcaPath is in this set is consumed raw-level
// inside the matching group(s) (evalGroupRoot / runIdxLoopRecursive) and is
// skipped by the rootDoc subtraction loop in execute(). Excludes with empty
// lcaPath always go through rootDoc subtraction.
func (e *recExecutor) withPlanLCAs(lcas map[string]struct{}) *recExecutor {
	e.planLCAs = lcas
	return e
}

// excludeMatchesGroup reports whether the given exclude must be subtracted
// inside this group at raw level. True iff exclude.lcaPath is non-empty and
// equals g.lcaPath. The caller still has to be in a code path that can mix
// raw-shape exclude bitmaps with raw-shape inputs (canUseRawAndAll path or
// runIdxLoopRecursive); other paths fall back to rootDoc subtraction.
func (e *recExecutor) excludeMatchesGroup(excl recExclude, g *recGroupNode) bool {
	return excl.lcaPath != "" && excl.lcaPath == g.lcaPath
}

// excludeConsumedByPlan reports whether the exclude is subtracted somewhere
// inside the plan tree (so execute() must NOT also subtract it at rootDoc).
// True iff exclude.lcaPath is non-empty and present in planLCAs.
func (e *recExecutor) excludeConsumedByPlan(excl recExclude) bool {
	if excl.lcaPath == "" {
		return false
	}
	_, ok := e.planLCAs[excl.lcaPath]
	return ok
}

// withRootAnchor attaches the no-positive seed bitmap. When plan is nil, the
// executor uses this bitmap as the element universe before subtracting excludes.
func (e *recExecutor) withRootAnchor(anchor *sroar.Bitmap) *recExecutor {
	e.rootAnchor = anchor
	return e
}

// withReturnMasked toggles masked-output mode. When true, execute returns a
// root+docID position bitmap (leaf bits zeroed) instead of plain docIDs — i.e.
// it skips the trailing MaskRootLeaf. Used by resolveMultiGroupRootDocIDAnd to
// AND multiple groups at root+docID level before stripping root bits.
func (e *recExecutor) withReturnMasked(returnMasked bool) *recExecutor {
	e.returnMasked = returnMasked
	return e
}

// execute runs the plan and returns either the final docID bitmap (root and
// leaf bits stripped) or the intermediate root+docID position bitmap when
// returnMasked is set. The caller must invoke the returned release.
//
// When plan is nil the executor takes the rootAnchor path: clone the anchor,
// AndNot each exclude at raw position level (preserves leaf alignment with the
// anchor's positions), MaskLeaf to a rootDoc bitmap, then MaskRootLeaf (skipped
// if returnMasked).
//
// When plan is non-nil: evalNode produces a rootDoc bitmap; each exclude that
// is NOT consumed inside the plan tree is MaskLeaf'd and AndNot'd from it
// (root+docID-level subtraction). Excludes with lcaPath ∈ planLCAs were already
// applied raw-level inside the matching group(s) — see §8.5. MaskRootLeaf
// produces the final docID set (skipped if returnMasked).
func (e *recExecutor) execute(ctx context.Context, plan recPlanNode) (*sroar.Bitmap, func(), error) {
	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}
	if plan == nil {
		return e.executeRootAnchor()
	}

	bm, release, err := e.evalNode(ctx, plan, nil)
	if err != nil {
		return nil, nil, err
	}

	for _, excl := range e.excludes {
		if e.excludeConsumedByPlan(excl) {
			continue
		}
		maskedExcl, relExcl := e.bitmapOps.MaskLeaf(excl.bitmap)
		bm.AndNotConc(maskedExcl, e.maxConcurrency)
		relExcl()
	}

	if e.returnMasked {
		// Caller takes ownership of bm — its release is the returned release.
		return bm, release, nil
	}
	defer release()
	docs, docRel := e.bitmapOps.MaskRootLeaf(bm)
	return docs, docRel, nil
}

// executeRootAnchor handles the no-positive path. The anchor is the universe
// of element positions (from _exists.{scope}); excludes are subtracted at raw
// level so leaf-position alignment with the anchor is preserved before the
// per-element MaskLeaf collapses the result to (root, 0, docID). The trailing
// MaskRootLeaf is skipped when returnMasked is set.
func (e *recExecutor) executeRootAnchor() (*sroar.Bitmap, func(), error) {
	if e.rootAnchor == nil {
		return nil, nil, fmt.Errorf("recExecutor.execute: nil plan requires a non-nil rootAnchor")
	}
	anchor := e.rootAnchor.Clone()
	for _, excl := range e.excludes {
		anchor.AndNotConc(excl.bitmap, e.maxConcurrency)
	}
	masked, maskedRel := e.bitmapOps.MaskLeaf(anchor)
	if e.returnMasked {
		return masked, maskedRel, nil
	}
	defer maskedRel()
	docs, docRel := e.bitmapOps.MaskRootLeaf(masked)
	return docs, docRel, nil
}

// evalNode dispatches by node type. parentScope (a raw position bitmap) limits
// evaluation to positions within those array elements; nil means no restriction.
//
// The returned bitmap is a position bitmap with leaf bits zeroed. Most nodes
// return a rootDoc bitmap (root_idx + docID). The single exception is a
// recSplitNode at lcaPath="" — its branches must be ANDed at docID level
// because they pin to different root_idx values, so the result has both root
// and leaf bits zeroed. MaskRootLeaf in execute is idempotent in that case.
func (e *recExecutor) evalNode(ctx context.Context, node recPlanNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	switch n := node.(type) {
	case *recGroupNode:
		return e.evalGroup(ctx, n, parentScope)
	case *recSplitNode:
		return e.evalSplit(ctx, n, parentScope)
	default:
		return nil, nil, fmt.Errorf("recExecutor: unknown node type %T", node)
	}
}

// evalGroup picks a combining strategy based on the group shape:
//
//   - canUseRawAndAll: raw AndAll preserves leaf-precise same-element
//     semantics. The idx loop is unnecessary because every here leaf's
//     position is naturally aligned (terminates at lcaPath depth, single
//     condition per path, no subs). Also covers the "single contributor"
//     short-circuit (1 here leaf, 0 subs).
//   - lcaPath == "" (and not raw-AndAll-eligible): AndAllMaskLeaf — root_idx
//     encodes element identity at root scope, no per-element loop needed.
//   - lcaPath != "" with multiple contributors and at least one masked input
//     (sub plan, or here paths going deeper): per-element idx loop — each
//     element of lcaPath is a candidate; here MaskLeafAnd and sub plans must
//     all match within the same element.
//
// The here=0, subs=1 case is collapsed away by the planner so it does not
// reach evalGroup (see buildGroup).
func (e *recExecutor) evalGroup(ctx context.Context, g *recGroupNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if canUseRawAndAll(g) || g.lcaPath == "" {
		return e.evalGroupRoot(ctx, g, parentScope)
	}
	return e.runIdxLoopRecursive(ctx, g, parentScope)
}

// evalGroupRoot collects raw bitmaps for here conditions and rootDoc bitmaps
// for sub results, plus parentScope when set, and combines them. It handles
// both the canUseRawAndAll path (any lcaPath) and the lcaPath="" non-raw path.
//
// Two combining strategies:
//
//   - canUseRawAndAll (no subs, every here path unique): raw positions are
//     naturally leaf-aligned, so raw AndAll keeps only positions where every
//     condition lands on the exact same leaf. The result is then MaskLeaf'd
//     to the rootDoc shape evalNode promises. parentScope, when set, is also
//     a raw bitmap and is folded into the AndAll — it acts as an
//     element-membership filter without disturbing same-element semantics.
//
//   - Otherwise (subs present, or a path appears more than once in here):
//     raw positions cannot match — different sub elements / different array
//     values share the same root_idx but different leaf_idx, so we mask
//     leaves before ANDing. Only valid at lcaPath="" because root_idx alone
//     identifies the element at root scope.
//
// Already-leaf-masked bitmaps (sub results) are unaffected by the additional
// MaskLeaf — masking is idempotent — so they can be mixed in with raws safely.
func (e *recExecutor) evalGroupRoot(ctx context.Context, g *recGroupNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	var bitmaps []*sroar.Bitmap
	var releases []func()
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range releases {
				rel()
			}
		}
	}()

	for _, leaf := range g.here {
		bm, ok := e.rawsByCond[leaf]
		if !ok {
			return nil, nil, fmt.Errorf("evalGroupRoot: no raw bitmap for leaf %q", childRelPath(leaf))
		}
		bitmaps = append(bitmaps, bm)
	}
	for _, sub := range g.subs {
		subResult, subRelease, err := e.evalNode(ctx, sub, parentScope)
		if err != nil {
			return nil, nil, err
		}
		releases = append(releases, subRelease)
		bitmaps = append(bitmaps, subResult)
	}
	if parentScope != nil {
		bitmaps = append(bitmaps, parentScope)
	}
	if len(bitmaps) == 0 {
		return nil, nil, fmt.Errorf("evalGroupRoot: no inputs for group at lcaPath=%q", g.lcaPath)
	}

	if canUseRawAndAll(g) {
		anded, andedRel := e.bitmapOps.AndAll(bitmaps, e.maxConcurrency)
		// Apply matching excludes (lcaPath == g.lcaPath) at raw level — the
		// anded bitmap is still raw-shape (leaf-precise) here, so AndNot'ing a
		// raw _exists.{path} bitmap removes only the exact same-element leaf
		// position that carries the absent property's existence. This is the
		// §8.5 sibling-element semantics: a sibling element at the same LCA
		// without the absent property's existence survives.
		for _, excl := range e.excludes {
			if e.excludeMatchesGroup(excl, g) {
				anded.AndNotConc(excl.bitmap, e.maxConcurrency)
			}
		}
		result, resultRel := e.bitmapOps.MaskLeaf(anded)
		andedRel()
		succeeded = true
		for _, rel := range releases {
			rel()
		}
		return result, resultRel, nil
	}

	result, resultRel := e.bitmapOps.AndAllMaskLeaf(bitmaps, e.maxConcurrency)
	succeeded = true
	for _, rel := range releases {
		rel()
	}
	return result, resultRel, nil
}

// canUseRawAndAll reports whether evalGroupRoot can combine bitmaps with raw
// AndAll (preserving leaf-precise same-element semantics) instead of
// AndAllMaskLeaf. True only when every here path is unique and there are no
// subs; subs produce already-masked rootDoc bitmaps that cannot AND against
// raw leaf-precise positions.
func canUseRawAndAll(g *recGroupNode) bool {
	if len(g.subs) > 0 || len(g.here) == 0 {
		return false
	}
	if len(g.here) == 1 {
		return true
	}
	seen := make(map[string]struct{}, len(g.here))
	for _, leaf := range g.here {
		path := childRelPath(leaf)
		if _, dup := seen[path]; dup {
			return false
		}
		seen[path] = struct{}{}
	}
	return true
}

// runIdxLoopRecursive iterates _idx.{lcaPath}[K] entries and, for each element
// K, evaluates here MaskLeafAnd and sub plans within the element's positions
// (constrained by parentScope when set). Per-element rootDoc results are OR'd
// into the accumulator.
func (e *recExecutor) runIdxLoopRecursive(ctx context.Context, g *recGroupNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if e.metaBucket == nil {
		return nil, nil, fmt.Errorf("runIdxLoopRecursive: meta bucket is nil for %q", g.lcaPath)
	}
	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}

	idxPrefix := invnested.PathPrefix("_idx." + g.lcaPath)

	capHint := 64
	for _, leaf := range g.here {
		if bm, ok := e.rawsByCond[leaf]; ok {
			if l := bm.LenInBytes(); l > capHint {
				capHint = l
			}
		}
	}

	result, releaseResult := e.bitmapOps.NewEmpty(capHint)
	succeeded := false
	defer func() {
		if !succeeded {
			releaseResult()
		}
	}()

	c := e.metaBucket.CursorRoaringSet()
	defer c.Close()

	for k, elemBitmap := c.Seek(invnested.IdxKey(g.lcaPath, 0)); k != nil; k, elemBitmap = c.Next() {
		if err := ctxExpired(ctx); err != nil {
			return nil, nil, err
		}
		if !bytes.HasPrefix(k, idxPrefix) {
			break
		}
		if elemBitmap.IsEmpty() {
			continue
		}

		effective, effRelease := e.intersectScope(elemBitmap, parentScope)
		if effective.IsEmpty() {
			effRelease()
			continue
		}
		// Apply matching excludes (lcaPath == g.lcaPath) at raw level on a
		// per-element copy of effective. AndNot'ing the raw _exists.{path}
		// bitmap removes leaf positions that carry the absent property's
		// existence, so matchElementRecursive only intersects positives
		// against positions where the absent property is missing within this
		// element K. intersectScope may return the cursor's own bitmap when
		// parentScope is nil, so a clone is required to avoid mutating it.
		effective, effRelease = e.applyMatchingExcludes(g, effective, effRelease)
		if effective.IsEmpty() {
			effRelease()
			continue
		}
		if err := e.matchElementRecursive(ctx, g, effective, result); err != nil {
			effRelease()
			return nil, nil, err
		}
		effRelease()
	}

	succeeded = true
	return result, releaseResult, nil
}

// applyMatchingExcludes returns a new effective scope with every matching
// exclude AndNot'd at raw level. When no exclude matches, effective is
// returned unchanged together with its original release. When at least one
// matches, a fresh pool buffer is allocated, the effective contents are copied
// in, every matching raw exclude is AndNot'd, and the original release is
// invoked before returning the new buffer + its release.
func (e *recExecutor) applyMatchingExcludes(g *recGroupNode, effective *sroar.Bitmap, release func()) (*sroar.Bitmap, func()) {
	hasMatch := false
	for _, excl := range e.excludes {
		if e.excludeMatchesGroup(excl, g) {
			hasMatch = true
			break
		}
	}
	if !hasMatch {
		return effective, release
	}
	cloned, clonedRel := e.bitmapOps.AndAll([]*sroar.Bitmap{effective}, e.maxConcurrency)
	release()
	for _, excl := range e.excludes {
		if e.excludeMatchesGroup(excl, g) {
			cloned.AndNotConc(excl.bitmap, e.maxConcurrency)
		}
	}
	return cloned, clonedRel
}

// matchElementRecursive computes the per-element rootDoc as the intersection of
// all here MaskLeafAnd results and all sub plan results, evaluated under
// effective. On any empty intermediate it short-circuits and the element is
// skipped. On success the per-element rootDoc is OR'd into result.
func (e *recExecutor) matchElementRecursive(ctx context.Context, g *recGroupNode, effective, result *sroar.Bitmap) error {
	var partial *sroar.Bitmap
	var partialRel func()
	releasePartial := func() {
		if partialRel != nil {
			partialRel()
		}
	}

	addRootDoc := func(bm *sroar.Bitmap, rel func()) (empty bool) {
		if partial == nil {
			partial = bm
			partialRel = rel
			return partial.IsEmpty()
		}
		partial.AndConc(bm, e.maxConcurrency)
		rel()
		return partial.IsEmpty()
	}

	for _, leaf := range g.here {
		rawLeaf, ok := e.rawsByCond[leaf]
		if !ok {
			releasePartial()
			return fmt.Errorf("matchElementRecursive: no raw bitmap for leaf %q", childRelPath(leaf))
		}
		bm, rel := e.bitmapOps.MaskLeafAnd(rawLeaf, effective)
		if addRootDoc(bm, rel) {
			releasePartial()
			return nil
		}
	}

	for _, sub := range g.subs {
		subResult, subRelease, err := e.evalNode(ctx, sub, effective)
		if err != nil {
			releasePartial()
			return err
		}
		if addRootDoc(subResult, subRelease) {
			releasePartial()
			return nil
		}
	}

	if partial == nil {
		// A group with no here and no subs is a builder bug. Treat as no match.
		return nil
	}

	result.OrConc(partial, e.maxConcurrency)
	releasePartial()
	return nil
}

// evalSplit reads each branch's _idx[lcaPath, branch.index] entry, composes
// branchScope = idx ∩ parentScope, evaluates branch.plan under branchScope, and
// combines branch results.
//
// Combining strategy:
//   - len(branches)==1: return the branch result directly. The single branch
//     pins to one root_idx value and the result is already a rootDoc bitmap.
//   - len(branches)>1 at lcaPath=="": branches pin to disjoint root_idx values,
//     so ANDing rootDocs yields empty. MaskRootLeaf each branch and AND at
//     docID level.
//   - len(branches)>1 at intermediate lcaPath: all branches share the same
//     root, so a plain rootDoc AND gives the correct same-root semantics.
func (e *recExecutor) evalSplit(ctx context.Context, n *recSplitNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if e.metaBucket == nil {
		return nil, nil, fmt.Errorf("evalSplit: meta bucket is nil for %q", n.lcaPath)
	}
	if len(n.branches) == 0 {
		return nil, nil, fmt.Errorf("evalSplit: split with no branches at %q", n.lcaPath)
	}

	branchResults := make([]*sroar.Bitmap, 0, len(n.branches))
	var branchReleases []func()
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range branchReleases {
				rel()
			}
		}
	}()

	for _, br := range n.branches {
		if err := ctxExpired(ctx); err != nil {
			return nil, nil, err
		}
		bm, rel, err := e.evalSplitBranch(ctx, n.lcaPath, br, parentScope)
		if err != nil {
			return nil, nil, err
		}
		branchResults = append(branchResults, bm)
		branchReleases = append(branchReleases, rel)
	}

	if len(n.branches) == 1 {
		succeeded = true
		return branchResults[0], branchReleases[0], nil
	}

	if n.lcaPath == "" {
		return e.andBranchesAtDocID(branchResults, branchReleases, &succeeded)
	}
	result, resultRel := e.bitmapOps.AndAll(branchResults, e.maxConcurrency)
	succeeded = true
	for _, rel := range branchReleases {
		rel()
	}
	return result, resultRel, nil
}

// evalSplitBranch resolves the branch's _idx entry, composes the effective
// branchScope, and recurses into branch.plan. The returned bitmap and release
// belong to the branch result; idxK is released internally.
func (e *recExecutor) evalSplitBranch(ctx context.Context, lcaPath string, br recSplitBranch, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	idxK, idxRel, err := e.metaBucket.RoaringSetGet(invnested.IdxKey(lcaPath, br.index))
	if err != nil {
		return nil, nil, fmt.Errorf("evalSplit: read _idx[%q,%d]: %w", lcaPath, br.index, err)
	}
	defer idxRel()

	if idxK.IsEmpty() {
		empty, emptyRel := e.bitmapOps.NewEmpty(64)
		return empty, emptyRel, nil
	}

	branchScope, branchScopeRel := e.intersectScope(idxK, parentScope)
	defer branchScopeRel()
	if branchScope.IsEmpty() {
		empty, emptyRel := e.bitmapOps.NewEmpty(64)
		return empty, emptyRel, nil
	}

	return e.evalNode(ctx, br.plan, branchScope)
}

// andBranchesAtDocID is the SPLIT@"" combiner: MaskRootLeaf each branch result
// then AND them together as plain docIDs. Releases all inputs before returning.
func (e *recExecutor) andBranchesAtDocID(branchResults []*sroar.Bitmap, branchReleases []func(), succeeded *bool) (*sroar.Bitmap, func(), error) {
	masked := make([]*sroar.Bitmap, 0, len(branchResults))
	maskedReleases := make([]func(), 0, len(branchResults))
	for _, br := range branchResults {
		m, mRel := e.bitmapOps.MaskRootLeaf(br)
		masked = append(masked, m)
		maskedReleases = append(maskedReleases, mRel)
	}

	result, resultRel := e.bitmapOps.AndAll(masked, e.maxConcurrency)
	for _, rel := range maskedReleases {
		rel()
	}
	*succeeded = true
	for _, rel := range branchReleases {
		rel()
	}
	return result, resultRel, nil
}

// intersectScope returns scope ∩ parentScope. When parentScope is nil the input
// scope is returned with a no-op release — the caller does NOT own it. When
// parentScope is non-nil a fresh pool buffer is allocated and the caller must
// invoke release.
func (e *recExecutor) intersectScope(scope, parentScope *sroar.Bitmap) (*sroar.Bitmap, func()) {
	if parentScope == nil {
		return scope, func() {}
	}
	return e.bitmapOps.AndAll([]*sroar.Bitmap{scope, parentScope}, e.maxConcurrency)
}
