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
	"github.com/weaviate/weaviate/entities/models"
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
	// props is the nested schema of the root property the executor operates
	// on. Used by collectFlatSubtree to identify scalar-array (text[], int[],
	// uuid[], …) here paths — those values get distinct leaves per element
	// from walkScalarArray and break the inheritance bridge that flat raw
	// AndAll relies on.
	props []*models.NestedProperty
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
// equals g.lca. The caller still has to be in a code path that can mix
// raw-shape exclude bitmaps with raw-shape inputs (canUseRawAndAll path or
// runIdxLoopRecursive); other paths fall back to rootDoc subtraction.
func (e *recExecutor) excludeMatchesGroup(excl recExclude, g *recGroupNode) bool {
	return excl.lcaPath != "" && excl.lcaPath == g.lca
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

// withProps attaches the root property's nested schema. Required for the
// flat raw-AndAll path in evalGroup to detect scalar-array (narrow-leaf)
// here paths.
func (e *recExecutor) withProps(props []*models.NestedProperty) *recExecutor {
	e.props = props
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
// level so leaf-position alignment with the anchor is preserved. The trailing
// MaskRootLeaf produces docIDs (skipped when returnMasked is set, in which
// case the caller receives a leaf-precise raw bitmap and is responsible for
// collapsing to docIDs itself).
//
// Phase 2a of the position-level eval rollout: dropped the prior MaskLeaf
// step before MaskRootLeaf — it was redundant since MaskRootLeaf zeros both
// root and leaf, and removing it lets the raw bitmap flow through to the
// boundary unchanged.
func (e *recExecutor) executeRootAnchor() (*sroar.Bitmap, func(), error) {
	if e.rootAnchor == nil {
		return nil, nil, fmt.Errorf("recExecutor.execute: nil plan requires a non-nil rootAnchor")
	}
	// Clone the anchor into a pool buffer so AndNot mutations don't disturb
	// the shared rootAnchor; the clone becomes the result.
	raw, rawRel := e.bitmapOps.AndAll([]*sroar.Bitmap{e.rootAnchor}, e.maxConcurrency)
	for _, excl := range e.excludes {
		raw.AndNotConc(excl.bitmap, e.maxConcurrency)
	}
	if e.returnMasked {
		// returnMasked callers expect a leaf-zeroed bitmap. Apply MaskLeaf
		// here to preserve that contract until Step 4 phase 2b refactors
		// returnMasked to mean "skip MaskRootLeaf, return raw".
		masked, maskedRel := e.bitmapOps.MaskLeaf(raw)
		rawRel()
		return masked, maskedRel, nil
	}
	defer rawRel()
	docs, docRel := e.bitmapOps.MaskRootLeaf(raw)
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
	case *recOrNode:
		return e.evalOr(ctx, n, parentScope)
	case *recNotNode:
		return e.evalNot(ctx, n, parentScope)
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
//   - flat subtree (no SPLITs, every here path globally unique, ≤1 deeper
//     group with here, no scalar-array terminals): fold the entire subtree
//     into one raw AndAll. Same-element semantics is preserved by the
//     analyzer's scalar inheritance — a scalar at depth d inherits its
//     parent element's positions, which equal the descendant leaves when
//     descendants exist, so leaves of conditions on different paths within
//     the same physical element coincide. Avoids the idx loop, which is
//     otherwise expensive and (for non-canUseRawAndAll groups at non-root
//     lcaPath) loses leaf precision when _idx.{lca}[K] lumps multiple
//     physical instances at the same K under different parents.
//   - lcaPath != "" with multiple contributors and at least one masked input
//     (sub plan, or here paths going deeper) AND not flat: per-element idx
//     loop — each element of lcaPath is a candidate; here MaskLeafAnd and
//     sub plans must all match within the same element.
//
// The here=0, subs=1 case is collapsed away by the planner so it does not
// reach evalGroup (see buildGroup).
func (e *recExecutor) evalGroup(ctx context.Context, g *recGroupNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if e.canUseRawAndAll(g) || g.lca == "" {
		return e.evalGroupRoot(ctx, g, parentScope)
	}
	if flat, ok := e.collectFlatSubtree(g); ok {
		return e.evalFlatRawAndAll(ctx, flat, parentScope)
	}
	return e.runIdxLoopRecursive(ctx, g, parentScope)
}

// flatSubtree summarizes a recGroupNode tree where raw AndAll over the
// union of all here leaves is guaranteed to enforce same-element semantics
// correctly. leaves is the union of here leaves; lcaPaths is the set of
// lcaPaths covered by the subtree's groups (used to match excludes).
type flatSubtree struct {
	leaves   []*propValuePair
	lcaPaths map[string]struct{}
}

// collectFlatSubtree walks the subtree rooted at g and returns a flatSubtree
// when every node is a recGroupNode in a single chain (≤1 sub per group),
// every here path is globally unique, no here targets a scalar-array path,
// and at most one group below the root carries here conditions. Bails
// (returns ok=false) on any of:
//   - empty props (without schema we cannot identify scalar-array paths;
//     stay on runIdxLoopRecursive)
//   - recSplitNode anywhere (arr[N] requires per-element evaluation)
//   - duplicate here paths (each text[]/scalar-array value lives at a distinct
//     leaf assigned by walkScalarArray, so duplicate-path filters over a scalar
//     array cannot match at the same leaf via raw AndAll — they need
//     per-element evaluation. Regular scalars under Phase-3 inheritance can
//     legitimately share leaves, but the planner only emits duplicate here
//     paths when at least one terminal is a scalar-array, so this bail is safe)
//   - any group with ≥2 subs (sibling sub-arrays produce conditions at
//     distinct leaves with no inheritance bridge — raw AndAll fails)
//   - any here condition on a scalar-array (text[], int[], uuid[], …) path.
//     These values are written by walkScalarArray with one leaf per element
//     (assign.go), so they don't share leaves via Phase-3 inheritance with
//     other conditions.
//   - more than one group below the root carrying here conditions. A
//     deeper-group here may introduce a narrow leaf bitmap; combining two
//     such narrow leaves at distinct sub-paths fails raw AndAll. (One
//     deeper group is fine because the root's wide here bitmaps inherit
//     through to the deeper leaf.)
func (e *recExecutor) collectFlatSubtree(g *recGroupNode) (*flatSubtree, bool) {
	if len(e.props) == 0 {
		return nil, false
	}
	out := &flatSubtree{lcaPaths: map[string]struct{}{}}
	seen := map[string]struct{}{}
	deeperWithHere := 0
	var walk func(n recPlanNode, isRoot bool) bool
	walk = func(n recPlanNode, isRoot bool) bool {
		grp, ok := n.(*recGroupNode)
		if !ok {
			return false
		}
		if len(grp.subs) > 1 {
			return false
		}
		if !isRoot && len(grp.here) > 0 {
			deeperWithHere++
			if deeperWithHere > 1 {
				return false
			}
		}
		out.lcaPaths[grp.lca] = struct{}{}
		for _, leaf := range grp.here {
			path := childRelPath(leaf)
			if _, dup := seen[path]; dup {
				return false
			}
			seen[path] = struct{}{}
			if pathTerminalIsScalarArray(e.props, path) {
				return false
			}
			out.leaves = append(out.leaves, leaf)
		}
		for _, sub := range grp.subs {
			if !walk(sub, false) {
				return false
			}
		}
		return true
	}
	if !walk(g, true) {
		return nil, false
	}
	return out, true
}

// evalFlatRawAndAll evaluates a flat subtree by raw-AndAll'ing every here
// leaf's bitmap together with parentScope. Same-element semantics holds via
// scalar inheritance — a position survives raw AndAll only when every
// condition lands on a leaf shared with all the others, which by the
// analyzer's DFS encoding means they belong to the same physical element.
//
// Excludes whose lcaPath is in the subtree's lcaPaths are AndNot'd at raw
// level (sibling-element semantics, §8.5). Excludes whose lcaPath is outside
// the subtree's lcaPaths are left for execute()'s rootDoc subtraction.
func (e *recExecutor) evalFlatRawAndAll(ctx context.Context, flat *flatSubtree, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}
	bitmaps := make([]*sroar.Bitmap, 0, len(flat.leaves)+1)
	for _, leaf := range flat.leaves {
		bm, ok := e.rawsByCond[leaf]
		if !ok {
			return nil, nil, fmt.Errorf("evalFlatRawAndAll: no raw bitmap for leaf %q", childRelPath(leaf))
		}
		bitmaps = append(bitmaps, bm)
	}
	if parentScope != nil {
		bitmaps = append(bitmaps, parentScope)
	}
	if len(bitmaps) == 0 {
		return nil, nil, fmt.Errorf("evalFlatRawAndAll: no inputs")
	}
	anded, andedRel := e.bitmapOps.AndAll(bitmaps, e.maxConcurrency)
	for _, excl := range e.excludes {
		if excl.lcaPath == "" {
			continue
		}
		if _, ok := flat.lcaPaths[excl.lcaPath]; ok {
			anded.AndNotConc(excl.bitmap, e.maxConcurrency)
		}
	}
	result, resultRel := e.bitmapOps.MaskLeaf(anded)
	andedRel()
	return result, resultRel, nil
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
		return nil, nil, fmt.Errorf("evalGroupRoot: no inputs for group at lcaPath=%q", g.lca)
	}

	if e.canUseRawAndAll(g) {
		anded, andedRel := e.bitmapOps.AndAll(bitmaps, e.maxConcurrency)
		// Apply matching excludes (lcaPath == g.lca) at raw level — the
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
// AndAllMaskLeaf. True only when every here path is unique, at most one path
// is a scalar-array terminal, and there are no subs. Subs produce already-
// masked rootDoc bitmaps that cannot AND against raw leaf-precise positions.
//
// Scalar-array terminals (text[], int[], uuid[], …) get distinct leaves per
// element from walkScalarArray. A single scalar-array sibling combined with
// regular scalars works under raw AndAll because Phase-3 inheritance makes
// the regular scalars' leaves the union of all descendants — which includes
// the scalar-array's leaves. But two scalar-array siblings never share a
// leaf even within the same parent element, so raw AndAll would yield empty
// for genuine same-element matches. Routing to runIdxLoopRecursive's
// MaskLeafAnd preserves correctness by comparing rootDoc after stripping
// leaves.
func (e *recExecutor) canUseRawAndAll(g *recGroupNode) bool {
	if len(g.subs) > 0 || len(g.here) == 0 {
		return false
	}
	if len(g.here) == 1 {
		return true
	}
	if len(e.props) == 0 {
		// Without schema we cannot identify scalar-array terminals, and the
		// raw AndAll fast path silently produces empty results for ≥2 scalar
		// arrays in the same group. Bail to runIdxLoopRecursive's masked path.
		return false
	}
	seen := make(map[string]struct{}, len(g.here))
	scalarArrays := 0
	for _, leaf := range g.here {
		path := childRelPath(leaf)
		if _, dup := seen[path]; dup {
			return false
		}
		seen[path] = struct{}{}
		if pathTerminalIsScalarArray(e.props, path) {
			scalarArrays++
			if scalarArrays >= 2 {
				return false
			}
		}
	}
	return true
}

// runIdxLoopRecursive iterates _idx.{lcaPath}[K] entries and, for each element
// K, evaluates here MaskLeafAnd and sub plans within the element's positions
// (constrained by parentScope when set). Per-element rootDoc results are OR'd
// into the accumulator.
func (e *recExecutor) runIdxLoopRecursive(ctx context.Context, g *recGroupNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if e.metaBucket == nil {
		return nil, nil, fmt.Errorf("runIdxLoopRecursive: meta bucket is nil for %q", g.lca)
	}
	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}

	idxPrefix := invnested.PathPrefix("_idx." + g.lca)

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

	for k, elemBitmap := c.Seek(invnested.IdxKey(g.lca, 0)); k != nil; k, elemBitmap = c.Next() {
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
		// Apply matching excludes (lcaPath == g.lca) at raw level on a
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
		return nil, nil, fmt.Errorf("evalSplit: meta bucket is nil for %q", n.lca)
	}
	if len(n.branches) == 0 {
		return nil, nil, fmt.Errorf("evalSplit: split with no branches at %q", n.lca)
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
		bm, rel, err := e.evalSplitBranch(ctx, n.lca, br, parentScope)
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

	if n.lca == "" {
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

// evalOr evaluates each child of an OR node and returns their union via OrAll.
// Output shape matches the children's shape — OrAll preserves position bits, so
// the OR works regardless of whether children return raw or rootDoc bitmaps
// (transitional during the position-level eval rollout: Phase 1 inner functions
// still emit rootDoc; Phase 2 flips them to raw and OR semantics are unchanged).
//
// parentScope is passed through to each child's evalNode unchanged. Children's
// own evaluation logic applies the scope; the OR doesn't need to re-apply.
func (e *recExecutor) evalOr(ctx context.Context, n *recOrNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}
	if len(n.children) == 0 {
		return nil, nil, fmt.Errorf("evalOr: OR with zero children at lca=%q", n.lca)
	}

	bitmaps := make([]*sroar.Bitmap, 0, len(n.children))
	releases := make([]func(), 0, len(n.children))
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range releases {
				rel()
			}
		}
	}()

	for _, child := range n.children {
		bm, rel, err := e.evalNode(ctx, child, parentScope)
		if err != nil {
			return nil, nil, err
		}
		bitmaps = append(bitmaps, bm)
		releases = append(releases, rel)
	}

	result, resultRel := e.bitmapOps.OrAll(bitmaps, e.maxConcurrency)
	succeeded = true
	for _, rel := range releases {
		rel()
	}
	return result, resultRel, nil
}

// evalNot inverts its operand at the operand's natural LCA against the
// per-LCA universe (`_exists.{lca}`). pins (when present) intersect the
// universe with `_idx.{pin.RelPath}[pin.Index]` slices — for example, pins
// for `NOT cars.tires[1].width=205` restrict the universe to tire-position
// entries at index 1. parentScope further narrows the universe so per-element
// inversion respects an enclosing idx-loop's effective scope.
//
// The output shape matches the operand's shape (transitional during Phase 1:
// operands return rootDoc, so this method MaskLeaf's the universe to match
// before AndNot. Phase 2 will refactor the rest of the executor to emit raw;
// the MaskLeaf step in evalNot can be dropped then, and the universe stays
// raw end-to-end).
func (e *recExecutor) evalNot(ctx context.Context, n *recNotNode, parentScope *sroar.Bitmap) (*sroar.Bitmap, func(), error) {
	if err := ctxExpired(ctx); err != nil {
		return nil, nil, err
	}
	if e.metaBucket == nil {
		return nil, nil, fmt.Errorf("evalNot: meta bucket is nil at lca=%q", n.lca)
	}

	// 1. Read the universe of positions at the operand's LCA. Empty universe
	// means the operand can't match anything in any element — NOT result is
	// also empty (existential per-element: no element exists to satisfy
	// "this element does not satisfy operand").
	universeBucket, universeBucketRel, err := e.metaBucket.RoaringSetGet(invnested.ExistsKey(n.lca))
	if err != nil {
		return nil, nil, fmt.Errorf("evalNot: read _exists for %q: %w", n.lca, err)
	}
	if universeBucket == nil || universeBucket.IsEmpty() {
		if universeBucketRel != nil {
			universeBucketRel()
		}
		empty, emptyRel := e.bitmapOps.NewEmpty(0)
		return empty, emptyRel, nil
	}

	// 2. Clone the universe into a pool buffer so subsequent steps can mutate
	// it without disturbing the bucket-owned bitmap. We can release the
	// bucket entry as soon as the clone is made.
	universe, universeRel := e.bitmapOps.AndAll([]*sroar.Bitmap{universeBucket}, e.maxConcurrency)
	universeBucketRel()
	succeeded := false
	defer func() {
		if !succeeded {
			universeRel()
		}
	}()

	// 3. Apply pin restrictions. Each pin intersects the universe with the
	// _idx slice for that arr[N] tuple.
	var keyBuf [invnested.IdxKeySize]byte
	for _, pin := range n.pins {
		pinBm, pinRel, err := e.metaBucket.RoaringSetGet(
			invnested.IdxKeyToBuf(pin.RelPath, pin.Index, keyBuf[:]))
		if err != nil {
			return nil, nil, fmt.Errorf("evalNot: read _idx for pin %q[%d]: %w", pin.RelPath, pin.Index, err)
		}
		if pinBm == nil {
			if pinRel != nil {
				pinRel()
			}
			// Pin slice not present in any doc — universe becomes empty.
			empty, emptyRel := e.bitmapOps.NewEmpty(0)
			succeeded = true
			universeRel()
			return empty, emptyRel, nil
		}
		universe.AndConc(pinBm, e.maxConcurrency)
		pinRel()
		if universe.IsEmpty() {
			succeeded = true
			return universe, universeRel, nil
		}
	}

	// 4. Apply parentScope. parentScope is a raw bitmap from an enclosing
	// idx-loop; AND'ing narrows the universe to that loop's element scope.
	if parentScope != nil {
		universe.AndConc(parentScope, e.maxConcurrency)
		if universe.IsEmpty() {
			succeeded = true
			return universe, universeRel, nil
		}
	}

	// 5. Evaluate operand under parentScope. Operand evaluation handles its
	// own scope restrictions internally.
	operand, operandRel, err := e.evalNode(ctx, n.operand, parentScope)
	if err != nil {
		return nil, nil, err
	}
	defer operandRel()

	// 6. AndNot universe ∖ operand. Under Phase 1 the operand returns
	// rootDoc, so MaskLeaf the universe to match before subtraction. Phase 2
	// will refactor the operand path to return raw and the MaskLeaf step
	// here becomes unnecessary.
	universeMatched, universeMatchedRel := e.bitmapOps.MaskLeaf(universe)
	universeRel()
	universeMatched.AndNotConc(operand, e.maxConcurrency)

	succeeded = true
	return universeMatched, universeMatchedRel, nil
}
