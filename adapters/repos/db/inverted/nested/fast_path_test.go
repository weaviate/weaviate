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

package nested

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// fast_path_test.go is the pre-DB harness for the fast-path nested filter
// execution model. Tests exercise the (Bitmap, Witnesses, Scope,
// CleanAbove) composition rules directly against AssignPositions output,
// skipping the parser / planner / shard write layers.

// ---------------------------------------------------------------------------
// AND merge dispatch
// ---------------------------------------------------------------------------
//
// andLeaves dispatches on the relationship between the two inputs'
// TruthScopes (same TS, ancestor+child, siblings). The branch picked
// determines merge scope + lift strategy; result.CleanAbove is computed
// uniformly inside andAtScope as the deepest of (merge scope, left.CS,
// right.CS).
//
// Notation: A = ancestor (shallower TS), C = child (deeper TS).
//
//   scope relation       branch condition                                          merge scope   action
//   ──────────────────────────────────────────────────────────────────────────────────────────────────
//   same TS              (always)                                                  left.TS       andAtScope
//   ──────────────────────────────────────────────────────────────────────────────────────────────────
//   ancestor + child     A.CleanBelow && (A.aboveScopeClean || A.Scope==propName)    C.Scope          andAtScope
//   ancestor + child     depth(C.CS) ≤ depth(A.Scope) (branch 1 missed)               A.Scope          andAtScope (no lift)
//   ancestor + child     C not clean at A.Scope                                       A.Scope          liftToScope(C, A.Scope) then andAtScope
//   ──────────────────────────────────────────────────────────────────────────────────────────────────
//   siblings (LCA above) (always)                                                  LCA           lift both to LCA, recurse same-TS
//
// The first (broadcasting) branch needs TWO independent claims: //   - CleanBelow — A.Bitmap has authentic descendant bits down through
//     C.Scope via the elementPositions encoding. Without this, the intersection
//     at C.Scope would pick up bogus descendants of A's non-matching elements.
//   - aboveScopeClean OR A.Scope==propName — A.Bitmap is authentic at every scope
//     above A.Scope (aboveScopeClean), or there's no scope above A.Scope to be unclean
//     about (A.Scope is the property root, e.g. L0-style schema).
//
// At L2 these two flags are practically correlated for positive operands
// (aboveScopeClean=true, CleanBelow=true) and for negate (both false). At
// L0 they diverge: a positive leaf has CleanBelow=true but
// aboveScopeClean=false because there's no scope above propName — without the
// explicit propName check the branch would miss the case and land at A.Scope
// instead of C.Scope.
//
// The second ancestor+child branch uses depth(C.CS) ≤ depth(A.Scope),
// NOT just C.aboveScopeClean. C may be clean somewhere above its own TS
// without that cleanness reaching A.Scope — e.g. an ancestor+child AND
// result whose CS sits at an intermediate scope between its TS and A.Scope.
// In that case C's bits at A.Scope aren't authentic and a direct merge
// would let ghosts leak; the third branch lifts C to A.Scope instead.
//
// Cleanness ceilings: //
//   - Same TS / siblings: ceiling = merge scope (TS or LCA). Chain bits
//     at parent are shared across elements, so the same-scope AND can
//     produce owner-level ghosts strictly above the merge scope. The
//     result is clean at the merge scope itself and nowhere above.
//
//   - Ancestor+child where A is clean above A.Scope: A.Bitmap broadcasts down
//     through descendantSelves to every scope reachable from A's matching
//     elements. Intersection at C.Scope picks per-element same-element
//     witnesses; intersection at any scope in [C.Scope, A.Scope] is per-element
//     because the chain bits at those scopes are still per-element
//     selfMarkers. Above A.Scope the bits collapse to shared chain — ghost
//     prone. result.CleanAbove = A.Scope via deepestPath(A.Scope, A.CS, C.CS).
//
//   - Ancestor+child where C is clean above C.Scope but A isn't: C's chain
//     bits at A.Scope are still authentic, so direct intersection at A.Scope is
//     correct. A's ghosts above A.Scope persist; result.CleanAbove = A.Scope.
//
//   - Ancestor+child, neither clean above TS: C.Bitmap may carry ghosts at
//     A.Scope that would coincide with A's authentic ES and leak. Lift C up
//     first — LiftToAncestor reconstructs C.Bitmap at A.Scope from authentic
//     Witnesses, wiping ghost bits — then merge same-scope at A.Scope.
//
// The core merge formula (rich = left.Bitmap ∩ right.Bitmap, support = rich ∩
// _anchor(mergeScope)) is identical in every branch; only the merge scope
// and lift strategy vary.

// ---------------------------------------------------------------------------
// OR merge dispatch matrix
// ---------------------------------------------------------------------------
//
// orLeaves dispatches on the same axes as andLeaves: scope relation × CP
// combinations. Unlike AND, OR never creates new ghosts — a chain bit at
// parent in (A.Bitmap ∪ B.Bitmap) legitimately means "this parent contains a
// matching element of either kind." OR always lands at the LCA: merging
// below A.Scope would lose A's "container exists" semantic (a doc with a
// warsaw garage but zero cars must still satisfy `warsaw OR cars.X`).
//
//   scope relation       (A.CP, B.CP)   merge scope   result.CP        per-operand action
//   ─────────────────────────────────────────────────────────────────────────────────────
//   same TS              (any, any)     A.Scope          A.CP && B.CP     none
//   ─────────────────────────────────────────────────────────────────────────────────────
//   ancestor + child     (any,  true)   A.Scope          A.CP             C: no lift (cheap anchor-intersect)
//   ancestor + child     (any,  false)  A.Scope          false            C: LiftToAncestor on C.ES
//   ─────────────────────────────────────────────────────────────────────────────────────
//   siblings (LCA above) (true,  true)  LCA           true             both: cheap anchor-intersect
//   siblings             (true,  false) LCA           false            A: cheap; B: real lift
//   siblings             (false, true)  LCA           false            A: real lift; B: cheap
//   siblings             (false, false) LCA           false            both: real lift
//
// "Cheap anchor-intersect" = X.Bitmap ∩ _anchor(LCA) — works because X.CP=true
// guarantees X.Bitmap is clean at every scope from X.Scope up to root.
// "Real lift" = LiftToAncestor on X.ES + Bitmap reconstruction at LCA — needed
// when X.CP=false because X.Bitmap at LCA carries ghost chain bits that would
// leak into the union's ES.
//
// Why each rule: //
//   - Same TS: union preserves authentic chain bits from each side; no new
//     ghosts. Result CP = A.CP && B.CP — whatever ghosts an operand carried
//     above its TS persist in the union.
//
//   - Ancestor+child, C.CP=true: C.Bitmap is authentic at every scope ≥ C.Scope
//     up to root, including A.Scope. Direct union at A.Scope works without
//     lifting. Result CP = A.CP (∧ true) — only A's ghost characteristics
//     above A.Scope matter.
//
//   - Ancestor+child, C.CP=false: C.Bitmap at A.Scope may have ghosts that would
//     leak into ES at A.Scope via the anchor intersection. Lift C up so its
//     A.Scope bits are rebuilt from authentic ES. Result CP=false because
//     C's ghosts above A.Scope persist in lifted.Bitmap (lift only fixes target
//     scope and below).
//
//   - Siblings: same logic per-operand — each CP=true operand contributes
//     authentically at LCA without lifting; each CP=false operand needs
//     a real lift to clean its LCA bits. CP=true on both sides preserves
//     CP=true overall (rare but real); any CP=false makes the result
//     CP=false.
//
// The core merge formula (rich = lifted_A.Bitmap ∪ lifted_B.Bitmap, support =
// lifted_A.ES ∪ lifted_B.ES) is identical in every branch; only the per-
// operand lift strategy and result CP vary. The shortcut for ES (union of
// per-operand ES instead of rich ∩ _anchor(LCA)) is cheaper than the AND
// formula and correct because union of authentic sets is authentic.

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// fastPathIndex aggregates AssignPositions output across many docs into the
// four bucket families a nested filter reader sees on disk: value, _exists,
// _idx, _anchor. All bitmaps are keyed by *logical* path (property name
// prepended).
//
// ops uses a noop pool — buffer lifecycle is not what these tests are about.
type fastPathIndex struct {
	propName string
	values   map[string]map[any]*sroar.Bitmap // path -> raw value -> positions
	exists   map[string]*sroar.Bitmap         // path -> positions
	idx      map[string]map[int]*sroar.Bitmap // path -> array index -> positions
	anchor   map[string]*sroar.Bitmap         // path -> self-marker positions
	ops      *BitmapOps
}

func newFastPathIndex(propName string) *fastPathIndex {
	return &fastPathIndex{
		propName: propName,
		values:   map[string]map[any]*sroar.Bitmap{},
		exists:   map[string]*sroar.Bitmap{},
		idx:      map[string]map[int]*sroar.Bitmap{},
		anchor:   map[string]*sroar.Bitmap{},
		ops:      NewBitmapOps(roaringset.NewBitmapBufPoolNoop()),
	}
}

// qualify converts a property-relative path (as emitted by AssignPositions)
// to the logical path used throughout these tests.
func (i *fastPathIndex) qualify(path string) string {
	if path == "" {
		return i.propName
	}
	return i.propName + "." + path
}

func (i *fastPathIndex) addDoc(t *testing.T, prop *models.Property, docID uint64, value any) {
	t.Helper()
	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	for _, v := range result.Values {
		path := i.qualify(v.Path)
		if i.values[path] == nil {
			i.values[path] = map[any]*sroar.Bitmap{}
		}
		bm, ok := i.values[path][v.Value]
		if !ok {
			bm = sroar.NewBitmap()
			i.values[path][v.Value] = bm
		}
		bm.SetMany(OrDocID(v.Positions, docID))
	}
	for _, e := range result.Exists {
		path := i.qualify(e.Path)
		bm, ok := i.exists[path]
		if !ok {
			bm = sroar.NewBitmap()
			i.exists[path] = bm
		}
		bm.SetMany(OrDocID(e.Positions, docID))
	}
	for _, x := range result.Idx {
		path := i.qualify(x.Path)
		if i.idx[path] == nil {
			i.idx[path] = map[int]*sroar.Bitmap{}
		}
		bm, ok := i.idx[path][x.Index]
		if !ok {
			bm = sroar.NewBitmap()
			i.idx[path][x.Index] = bm
		}
		bm.SetMany(OrDocID(x.Positions, docID))
	}
	for _, a := range result.Anchors {
		path := i.qualify(a.Path)
		bm, ok := i.anchor[path]
		if !ok {
			bm = sroar.NewBitmap()
			i.anchor[path] = bm
		}
		bm.SetMany(OrDocID(a.Positions, docID))
	}
}

// ---------------------------------------------------------------------------
// Result model
// ---------------------------------------------------------------------------

// fastPathResult is the test-side mirror of the proposed runtime NestedResult
// type. Tests build instances by hand using the leaf and merge helpers below.
// Renamed from NestedResult to avoid a future collision when the runtime type
// lands in the same package.
//
// The struct encodes three distinct claims about Bitmap: //
//
//  1. Scope: the scope at which the predicate's natural witnesses live.
//     Used for Witnesses computation, dispatch (commonScope), and as the
//     scope `negate` inverts at.
//
//  2. CleanAbove: the SHALLOWEST scope at which Bitmap is still ghost-free
//     ABOVE Scope. Equivalently, Bitmap is authentic at every scope from
//     Scope up to and including CleanAbove. Above CleanAbove, Bitmap may
//     carry owner-level chain bits that don't correspond to per-element
//     witnesses. Convention: CleanAbove == propName means "clean all the way
//     up to the property root" (the strongest single-leaf claim);
//     CleanAbove == Scope means "no cleanness claim above Scope".
//
//  3. CleanBelow: whether Bitmap's bits at scopes BELOW Scope are
//     authentic descendants of real matching elements (as produced by the
//     elementPositions = chain + selfMarker + descendantSelves encoding in
//     assign.go). Positive leaves and AND/OR compositions of positives all
//     have CleanBelow=true. `negate` does not — its Bitmap = _exists(Scope)
//     AndNot Witnesses carries descendant markers of every Scope-element
//     including non-matching ones.
//
// The two claims (CleanAbove and CleanBelow) are independent. They look
// correlated in mid-tree cases — `aboveScopeClean()` implies CleanBelow in
// practice for L2-style queries — but they diverge at the property root: // a positive leaf with Scope=propName has CleanAbove=propName=Scope, so
// `aboveScopeClean()` returns false even though descendants are authentic.
// CleanBelow captures this directly so the dispatch can fire the
// broadcasting branch at the root boundary.
//
// Downstream rule for lifting: //   - To project Bitmap to a target T at-or-below CleanAbove: anchor-intersect
//
//	  (cheap, equivalent to a clean lift).
//	- To project to T strictly above CleanAbove: must use LiftToAncestor
//	  (rebuild from Witnesses).
//
// Downstream rule for ancestor+child AND dispatch: the broadcasting branch
// (merge at child.Scope) fires when ancestor.CleanBelow is true AND
// ancestor's Bitmap is clean at-or-above ancestor.Scope (either
// aboveScopeClean, or ancestor.Scope is the property root and there's no
// scope above to be unclean at).
type fastPathResult struct {
	Scope      string
	Bitmap     *sroar.Bitmap
	Witnesses  *sroar.Bitmap
	CleanAbove string
	CleanBelow bool
}

// aboveScopeClean reports whether the result's Bitmap is authentic at one or
// more scopes strictly above Scope. This is the "above-Scope" half of the
// authenticity claim — separate from CleanBelow which covers below-Scope.
// Returns false when Scope is the property root (no scope above), so
// callers that want to allow the property-root case must combine this with
// an explicit `Scope == idx.propName` check.
func (r *fastPathResult) aboveScopeClean() bool {
	return pathDepth(r.CleanAbove) < pathDepth(r.Scope)
}

// deepestPath returns whichever scope has the greatest depth (= furthest
// from the property root). All inputs must lie on the same root-to-leaf
// chain so depth comparison is meaningful. Used to combine candidate
// CleanAbove values during merge.
func deepestPath(scopes ...string) string {
	best := scopes[0]
	bestDepth := pathDepth(best)
	for _, s := range scopes[1:] {
		if d := pathDepth(s); d > bestDepth {
			best = s
			bestDepth = d
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Leaf builders
// ---------------------------------------------------------------------------

// leafPositive realizes a positive value leaf `path = value`: //
//
//	Scope     = parent(path)
//	Bitmap           = value(path, value)
//	Witnesses   = Bitmap ∩ _anchor(Scope)
//	CleanAbove     = propName (clean all the way up to the property root —
//	                   every bit in Bitmap traces to an authentic matching
//	                   element's elementPositions, including the full
//	                   ancestorChain).
//	CleanBelow = true (Bitmap at scopes below TS includes only
//	                   descendantSelves of matching elements via the
//	                   elementPositions encoding in assign.go).
func leafPositive(idx *fastPathIndex, path string, value any) *fastPathResult {
	truthScope := parentPath(path)
	rich := cloneOrEmpty(idx.values[path][value])
	support := rich.Clone().And(idx.anchor[truthScope])
	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  support,
		CleanAbove: idx.propName,
		CleanBelow: true,
	}
}

// leafIsNullFalse realizes `path IS NULL false` (equivalently exists(path)).
// Mirrors leafPositive but reads from _exists instead of a value-keyed bucket
// so the leaf fires for every emission regardless of the recorded value.
//
//	Scope     = parent(path)
//	Bitmap           = _exists(path)
//	Witnesses   = Bitmap ∩ _anchor(Scope)
//	CleanAbove     = propName
//	CleanBelow = true (same authentic-descendant encoding as positive)
//
// TODO aliszka:nested_filtering — for object / object-array paths
// specifically, an alternative encoding would set Scope to the array's
// own scope (= path itself), not its parent. The optimization considered
// and DEFERRED was: //
//
//   - For object[] path `countries.garages`, set TS=`countries.garages`
//     (the array scope), with ES = `_anchor[garages]` ∩ Bitmap = every
//     emitted garage selfMarker. MaskRootLeaf yields the same docs as
//     today's TS=`countries`, but a deeper TS lets compound merges with
//     cars-or-below predicates skip a `LiftToAncestor` they would
//     otherwise need.
//
//   - The largest practical win is for the pinned variant (see
//     leafPinnedIsNullFalse below): pinnedFromValueSet does a real
//     LiftToAncestor per call today, which the array-scope convention
//     would eliminate.
//
// Why we kept TS=parent for now: //
//
//   - The symmetric pairing `negate(leafIsNullFalse) ≡ leafIsNullTrue`
//     ONLY works when both live at the same Scope. At TS=array-scope
//     the positive ES = every emitted element (vacuous), and
//     `negate` would produce `_anchor[scope] AndNot ES` = ∅. Always
//     empty. WRONG predicate. The user-intended IS NULL true ("owner
//     lacks the array") only makes sense at the owner (parent) scope.
//
//   - To enable array-scope positive IS NULL false, IS NULL true would
//     need its own builder (not derived via negate), and the planner
//     would have to route every `NOT (arr IS NULL false)` directly to
//     that builder rather than wrapping the positive leaf in NOT. That
//     planner discipline is a real risk: silently wrapping in NOT would
//     compute ∅ instead of "owner without array", and the bug is easy
//     to miss in tests.
//
//   - Scalars naturally stay at TS=parent (their owner is the meaningful
//     scope for both directions), so the asymmetry would be between
//     scalar IS NULL helpers and object IS NULL helpers. Sustained
//     asymmetry needs documentation and discipline — kept symmetric
//     until the optimization clearly pays for itself.
//
// When to revisit: profile real-world queries where pinned
// `arr[K].subarr IS NULL false` shows up in hot paths. If the
// pinnedFromValueSet lift is a measurable cost, do a focused pass that
// (a) adds path-aware variants of leafIsNullFalse and leafIsNullTrue,
// (b) tightens the planner's NOT-of-IS-NULL-false lowering, (c) adds
// regression tests that fail under naive `negate(positive)` lowering.
func leafIsNullFalse(idx *fastPathIndex, path string) *fastPathResult {
	truthScope := parentPath(path)
	rich := cloneOrEmpty(idx.exists[path])
	support := rich.Clone().And(idx.anchor[truthScope])
	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  support,
		CleanAbove: idx.propName,
		CleanBelow: true,
	}
}

// negate produces the support-first negation of any positive leaf result.
// The transformation is identical across every NOT-style leaf (unpinned NOT,
// IS NULL true, pinned variants): the positive helper's Witnesses is
// always the canonical positive support at Scope, and the negation is
// `universe \ pos.Witnesses` for both Witnesses (over _anchor) and
// Bitmap (over _exists).
//
// Both authenticity claims collapse here: //   - CleanAbove = Scope: Bitmap = _exists(TS) AndNot ES carries chain
//
//	    bits from every doc with any TS-element, regardless of whether a
//	    NON-matching witness exists, so scopes above TS carry owner-level
//	    ghosts.
//	  - CleanBelow = false: _exists(TS) also carries descendant markers
//	    of every TS-element, including matching ones (which are NOT witnesses
//	    for the negation). Below-TS bits aren't authentic for the negation
//	    predicate.
//
//		Scope     = pos.Scope
//		Witnesses   = _anchor(Scope) ANDNOT pos.Witnesses
//		Bitmap           = _exists(Scope) ANDNOT pos.Witnesses
//		CleanAbove     = Scope (no claim above)
//		CleanBelow = false (no claim below)
func negate(idx *fastPathIndex, pos *fastPathResult) *fastPathResult {
	truthAnchor := idx.anchor[pos.Scope]
	return &fastPathResult{
		Scope:      pos.Scope,
		Witnesses:  cloneOrEmpty(truthAnchor).AndNot(pos.Witnesses),
		Bitmap:     cloneOrEmpty(idx.exists[pos.Scope]).AndNot(pos.Witnesses),
		CleanAbove: pos.Scope,
		CleanBelow: false,
	}
}

// leafIsNullTrue realizes `path IS NULL true` — universe-subtract of
// leafIsNullFalse's positive support.
func leafIsNullTrue(idx *fastPathIndex, path string) *fastPathResult {
	return negate(idx, leafIsNullFalse(idx, path))
}

// leafNot realizes `NOT path = value` — universe-subtract of leafPositive's
// positive support. Empty array scope (no elements) does NOT count as a
// witness — that falls out because pos.Witnesses already requires at
// least one Scope-element to exist.
func leafNot(idx *fastPathIndex, path string, value any) *fastPathResult {
	return negate(idx, leafPositive(idx, path, value))
}

// leafPinnedIsNullFalse realizes `<pinned path>.x IS NULL false` using the
// same unified pipeline as leafPinnedPositive, just reading from _exists
// instead of a value-keyed bucket. Works for any pin layout.
//
//	A              = _exists(valuePath) ∩ _idx(pin_0) ∩ … ∩ _idx(pin_N-1)
//	mPin           = A ∩ _anchor(parent(valuePath))
//	Scope     = parent(pins[0].path)
//	narrowedAnchor = _anchor(Scope) ∩ A
//	Witnesses   = LiftToAncestor(mPin, narrowedAnchor)
//	Bitmap           = (A ANDNOT _anchor(Scope)) ∪ Witnesses
//	CanProjectParentByAnchor = false
//
// TODO aliszka:nested_filtering — make the lift lazy when downstream only
// consumes doc-level output; MaskRootLeaf(mPin) suffices in that case.
//
// TODO aliszka:nested_filtering — when valuePath denotes an object /
// object-array (not a scalar leaf), the array-scope Scope alternative
// described in leafIsNullFalse's comment applies here too, and is where
// the BIGGEST practical win lives (eliminates the LiftToAncestor inside
// pinnedFromValueSet for every pinned object/object[] IS NULL false). See
// the rationale and deferral reasons documented at leafIsNullFalse.
func leafPinnedIsNullFalse(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned IS NULL false needs at least one pin")
	}
	a := cloneOrEmpty(idx.exists[valuePath])
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// leafPinnedIsNullTrue realizes `<pinned path>.x IS NULL true`. Dispatches
// internally based on whether the pin chain has a gap between Scope
// and the value path's element scope: //
//
//   - No gap (fully pinned at every array level): universe-subtract of
//     leafPinnedIsNullFalse. Owner-level coincides with per-element here
//     because the pinned slot is uniquely identified per TS-element.
//
//   - Gap present (intermediate pin — some array level between TS and
//     ElementScope is unpinned): owner-level subtraction would diverge
//     from the natural per-element reading (e.g. `g[1].cars.year IS NULL
//     true` should pass any car under g[1] missing year, not require ALL
//     cars under g[1] to be missing year). Use per-element computation
//     directly: ElementWitnesses ∪ TS_missingPin lifted to TS.
//
// Missing pinned slot still counts as success in both branches — covered
// by TS_missingPin in the per-element formula and by universe-subtract in
// the no-gap branch.
func leafPinnedIsNullTrue(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if !hasPinGap(pins, valuePath) {
		return negate(idx, leafPinnedIsNullFalse(idx, valuePath, pins))
	}
	return perElementNotExists(idx, valuePath, pins)
}

// pinSpec describes one pin in a multi-pin path, e.g.,
// `garages[1].cars[2].year=2020` carries two pins: `{path:"countries.garages",
// index:1}` and `{path:"countries.garages.cars", index:2}`.
type pinSpec struct {
	path  string // qualified path of the pinned array
	index int    // pinned index within that array
}

// leafPinnedPositive realizes a multi-pin positive value leaf using a
// unified pipeline that works for any pin layout (innermost-pinned,
// intermediate-pinned, all-pinned multi-level) with a single LiftToAncestor
// regardless of pin count.
//
//	A              = value(valuePath, value) ∩ _idx(pin_0) ∩ … ∩ _idx(pin_N-1)
//	mPin           = A ∩ _anchor(parent(valuePath))                  // drops chain bits
//	Scope     = parent(pins[0].path)
//	narrowedAnchor = _anchor(Scope) ∩ A                          // optimization
//	Witnesses   = LiftToAncestor(mPin, narrowedAnchor)
//	Bitmap           = (A ANDNOT _anchor(Scope)) ∪ Witnesses
//	CanProjectParentByAnchor = false
//
// A carries chain bits inside the pinned subtree. Some are authentic
// ancestors of real matches; others are ghost bits left over when value
// matches at a sibling slot of a pin (e.g., value match at cars[3] while pin
// requires cars[2] keeps the shared country/garage chain bits).
// mPin's `∩ _anchor(parent(valuePath))` drops every chain bit (both
// authentic and ghost), leaving only leaf-scope selves where every pin
// constraint holds at a single concrete element. That's the canonical
// Witnesses once lifted to Scope.
//
// narrowedAnchor: every authentic Scope ancestor of an mPin element is
// already in A (chain bit of the same emission), so pre-intersecting the
// parent anchor with A doesn't drop any required predecessor — but it does
// shrink the scan to only buckets that survived the pin intersections.
// For sparse filters this is a significant cut to the LiftToAncestor cost.
//
// Bitmap preserves chain structure within the pinned subtree by stripping only
// Scope-level bits before OR-ing Witnesses back. Intermediate-level
// ghosts (e.g., garages self bits in a sibling-mismatch case) can persist
// below Scope; CanProjectParentByAnchor=false documents that downstream
// composition above Scope via anchor is unsafe.
//
// Does not support a root-level pin (Scope would resolve to "" with no
// corresponding parent anchor in the index). Add separate handling if a
// 3-pin path including the root array is needed.
//
// TODO aliszka:nested_filtering — make the lift lazy when downstream only
// consumes doc-level output; MaskRootLeaf(mPin) suffices in that case.
func leafPinnedPositive(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}
	a := cloneOrEmpty(idx.values[valuePath][value])
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// pinnedFromValueSet runs the shared pin-lift / Bitmap-reconstruction
// stage of every value-based pinned helper (leafPinnedPositive plus
// the pinned ContainsAny / ContainsAll variants below). Callers
// supply the already-aggregated value bitmap `a` — single bucket,
// union of buckets, or intersection of buckets — narrowed by every
// pin's _idx bitmap. The function takes care of mPin, the narrowed-
// anchor lift, and Bitmap = (a ANDNOT truthAnchor) ∪ Witnesses.
//
// Result shape matches leafPinnedPositive: TS at the outermost pin's
// parent, Witnesses at TS, Bitmap preserving chain bits inside the
// pinned subtree, CleanAbove = TS (no claim above — intermediate-
// level ghosts below TS can persist, and the chain bits stripped /
// re-added by the formula don't preserve above-TS authenticity).
func pinnedFromValueSet(idx *fastPathIndex, a *sroar.Bitmap, valuePath string, pins []pinSpec) *fastPathResult {
	innerAnchor := idx.anchor[parentPath(valuePath)]
	truthScope := parentPath(pins[0].path)
	truthAnchor := idx.anchor[truthScope]

	mPin := a.Clone().And(innerAnchor)
	narrowedAnchor := cloneOrEmpty(truthAnchor).And(a)
	exactSupport, _ := idx.ops.LiftToAncestor(mPin, narrowedAnchor)
	rich := a.AndNot(truthAnchor).Or(exactSupport)

	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  exactSupport,
		CleanAbove: truthScope,
		CleanBelow: true,
	}
}

// leafPinnedNot realizes `NOT <pinned path>.x = value`. Dispatches
// internally based on the pin chain layout — same shape as
// leafPinnedIsNullTrue: //
//
//   - No gap (fully pinned at every array level between TS and the value
//     path's element scope): universe-subtract of leafPinnedPositive.
//     The pinned slot is unique per TS-element, so owner-level NOT
//     coincides with per-element NOT.
//
//   - Gap present (intermediate pin): use the per-element NOT formula —
//     witnesses are the elements under the pinned subtree that fail the
//     predicate, lifted to TS, unioned with TS-elements that lack the
//     outermost pin slot entirely.
//
// Missing pinned slot counts as success in both branches.
func leafPinnedNot(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if !hasPinGap(pins, valuePath) {
		return negate(idx, leafPinnedPositive(idx, valuePath, value, pins))
	}
	return perElementNotValue(idx, valuePath, value, pins)
}

// hasPinGap reports whether the pin chain has an unpinned array level
// between the Scope and the value path's element scope. The pin
// count must equal the number of array levels descending from TS+1
// down to ElementScope; any mismatch means an intermediate level is
// unpinned. Used to choose between owner-level negation (no gap) and
// per-element negation (gap present).
func hasPinGap(pins []pinSpec, valuePath string) bool {
	if len(pins) == 0 {
		return false
	}
	elementScope := parentPath(valuePath)
	truthScope := parentPath(pins[0].path)
	levelsBetween := pathDepth(elementScope) - pathDepth(truthScope)
	return len(pins) != levelsBetween
}

// perElementNotValue computes `NOT <pinned path>.x = value` directly at
// the per-element scope when the pin chain has a gap. Wraps
// perElementNotFromSubtractands with the value-keyed bucket as the
// subtractand: witnesses are the elements that don't match the given
// value within the pinned subtree.
//
// Worked example: `NOT garages[1].cars.make=bmw` on the L2 schema —
// ElementScope=cars (unpinned), pins=[(garages, 1)], TS=countries.
// A country passes if it contains any car under garages[1] whose make is
// not bmw, OR if it has no garages[1] slot at all.
func perElementNotValue(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	return perElementNotFromSubtractands(idx, valuePath, pins, idx.values[valuePath][value])
}

// perElementNotExists is the IS NULL true analogue of perElementNotValue: // witnesses are elements within the pinned subtree whose value-path
// leaf is absent. The subtractand is the _exists bucket of the value
// path instead of a specific value bucket.
func perElementNotExists(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	return perElementNotFromSubtractands(idx, valuePath, pins, idx.exists[valuePath])
}

// perElementNotFromSubtractands is the shared body of perElementNotValue,
// perElementNotExists, and leafPinnedContainsNone. Callers differ in how
// many bitmaps they subtract from the pin-narrowed element universe: // single-value NOT passes one, IS NULL true passes one, ContainsNone
// passes one per listed value.
//
// Bitmap algebra: //
//
//	witnesses      := ⋂_i _idx[pins[i]] ∩ _anchor(ElementScope) ANDNOT ⋃ subtractands
//	                  // candidates inside the pinned subtree that the
//	                  // caller wants to negate (don't match any value,
//	                  // or lack the value-path leaf)
//	witnessesAtTS  := LiftToAncestor(witnesses, _anchor(TS))
//	                  // TS-elements with at least one such candidate
//	tsMissingPin   := _anchor(TS) ANDNOT _idx[outermost pin]
//	                  // TS-elements that lack the outermost pin slot
//	Witnesses   := witnessesAtTS ∪ tsMissingPin
//	Bitmap           := (_exists(TS) ANDNOT _anchor(TS)) ∪ Witnesses
//	                  // satisfies trustworthy-scope invariant
//	                  // Bitmap ∩ _anchor(TS) == Witnesses
//
// The union of subtractands is computed implicitly via chained AndNot: // A − (B ∪ C) = (A − B) − C, so we never materialise the union.
//
// Algebraic shortcuts (vs. the textbook formula): //
//
//   - Skipping elementPositive: `(pinNarrow ∩ _anchor) ANDNOT
//     elementPositive` simplifies to `(pinNarrow ∩ _anchor) ANDNOT
//     subtractand` because the subtractand's `pinNarrow ∩ _anchor`
//     factors are already present in the minuend.
//
//   - Skipping tsWithPin: `A ANDNOT (A ∩ B) = A ANDNOT B`, so
//     `tsMissingPin = _anchor(TS) ANDNOT _idx[outermost]` directly.
//
//   - Skipping the second LiftToAncestor: the outermost pin's _idx
//     bitmap already carries chain bits through TS, so the anchor
//     intersection is equivalent and cheaper than a full lift.
//
// Net: one LiftToAncestor and three clones, no per-element correlation
// trade-off.
func perElementNotFromSubtractands(idx *fastPathIndex, valuePath string, pins []pinSpec, subtractands ...*sroar.Bitmap) *fastPathResult {
	elementScope := parentPath(valuePath)
	truthScope := parentPath(pins[0].path)
	truthAnchor := idx.anchor[truthScope]

	// witnesses = ⋂_i _idx[pins[i]] ∩ _anchor(ElementScope) ANDNOT ⋃ subtractands
	witnesses := cloneOrEmpty(idx.idx[pins[0].path][pins[0].index])
	for _, p := range pins[1:] {
		witnesses.And(idx.idx[p.path][p.index])
	}
	witnesses.And(idx.anchor[elementScope])
	for _, sub := range subtractands {
		if sub != nil {
			witnesses.AndNot(sub)
		}
	}

	// witnessesAtTS = LiftToAncestor(witnesses, _anchor(TS))
	witnessesAtTS, _ := idx.ops.LiftToAncestor(witnesses, truthAnchor)

	// tsMissingPin = _anchor(TS) ANDNOT _idx[outermost pin]
	outermost := pins[0]
	tsMissingPin := cloneOrEmpty(truthAnchor).AndNot(idx.idx[outermost.path][outermost.index])

	// Witnesses = witnessesAtTS ∪ tsMissingPin (mutate witnessesAtTS).
	es := witnessesAtTS.Or(tsMissingPin)
	// Bitmap = (_exists(TS) ANDNOT _anchor(TS)) ∪ Witnesses
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(truthAnchor).Or(es)

	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  es,
		CleanAbove: truthScope,
		CleanBelow: false, // negation shape — _exists carries descendants of all TS-elements
	}
}

// leafContainsAny realizes `path ContainsAny [values]` directly as
// the union of the per-value buckets. Every sub-leaf shares the same
// TS (parent of path) and would be CP=true at construction, so
// dispatching through orN would only add overhead: sort the slice
// (no-op when all keys are equal), allocate a fastPathResult per
// value, then fold via the same-TS branch of orLeaves. Computing the
// bitmap union directly skips all of that.
//
// Semantics: a TS-element satisfies ContainsAny if it contains at
// least one of the listed values. For scalar arrays (e.g. text[]),
// "contains" means a per-element match somewhere in the array; for
// scalar fields it means the field equals one of the values.
//
// Result shape matches what orN over leafPositive(path, v) would
// produce: TS=parent(path), Bitmap=union of value buckets, ES=Bitmap ∩
// _anchor(TS), CleanAbove=propName.
//
// Panics on an empty value list — ContainsAny over an empty set has
// no positive support and is better caught at the call site.
func leafContainsAny(idx *fastPathIndex, path string, values ...any) *fastPathResult {
	if len(values) == 0 {
		panic("leafContainsAny requires at least one value")
	}
	truthScope := parentPath(path)
	rich := sroar.NewBitmap()
	for _, v := range values {
		if bm := idx.values[path][v]; bm != nil {
			rich.Or(bm)
		}
	}
	support := rich.Clone().And(idx.anchor[truthScope])
	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  support,
		CleanAbove: idx.propName,
		CleanBelow: true, // union of value buckets — each bucket has authentic descendants
	}
}

// leafContainsAll realizes `path ContainsAll [values]` directly as
// the intersection of the per-value buckets. Same reasoning as
// leafContainsAny: every sub-leaf shares TS and CP, so andN's sort
// and sequential fold are pure overhead. Intersecting the value
// bitmaps directly enforces per-TS-element co-occurrence — only
// chain bits that appear in EVERY value's elementPositions survive,
// which means the same TS-element must match every value.
//
// CleanAbove = TS in the result: same-TS AND structurally produces
// clean only at TS because chain bits at parent can survive even
// when different sub-elements satisfy different values.
//
// For scalar arrays (e.g. text[]), this means the same array must
// contain every listed value (across its elements).
//
// Bails out early when any value is missing from the index — the
// intersection would be empty regardless of the remaining values.
//
// Panics on an empty value list — ContainsAll over an empty set is
// vacuously true and should be optimised away upstream.
func leafContainsAll(idx *fastPathIndex, path string, values ...any) *fastPathResult {
	if len(values) == 0 {
		panic("leafContainsAll requires at least one value")
	}
	truthScope := parentPath(path)
	rich := cloneOrEmpty(idx.values[path][values[0]])
	for _, v := range values[1:] {
		bm := idx.values[path][v]
		if bm == nil {
			rich = sroar.NewBitmap()
			break
		}
		rich.And(bm)
	}
	support := rich.Clone().And(idx.anchor[truthScope])
	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  support,
		CleanAbove: truthScope,
		CleanBelow: true, // intersection of authentic-descendant buckets stays authentic
	}
}

// leafPinnedContainsAny realizes `<pinned path>.x ContainsAny [values]`
// — for example `garages[1].cars[2].colors ContainsAny [red, blue]`.
// Mirrors the optimisation used by the unpinned leafContainsAny: // compute the union of per-value buckets directly, narrow with every
// pin's _idx bitmap, then hand off to the shared pin-lift stage.
//
// Panics on an empty pin list (the function would have no Scope)
// or an empty value list (no positive support possible).
func leafPinnedContainsAny(idx *fastPathIndex, valuePath string, pins []pinSpec, values ...any) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned ContainsAny needs at least one pin")
	}
	if len(values) == 0 {
		panic("pinned ContainsAny requires at least one value")
	}
	a := sroar.NewBitmap()
	for _, v := range values {
		if bm := idx.values[valuePath][v]; bm != nil {
			a.Or(bm)
		}
	}
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// leafPinnedContainsAll realizes `<pinned path>.x ContainsAll [values]`
// — for example `cars[1].colors ContainsAll [red, blue]`, "cars[1]'s
// colors array contains both red and blue."
//
// Computes the intersection of per-value buckets directly, then
// narrows with every pin's _idx bitmap before the pin-lift stage.
// Bails out early when any value is absent from the index — the
// intersection is already empty.
//
// Panics on an empty pin list or empty value list.
func leafPinnedContainsAll(idx *fastPathIndex, valuePath string, pins []pinSpec, values ...any) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned ContainsAll needs at least one pin")
	}
	if len(values) == 0 {
		panic("pinned ContainsAll requires at least one value")
	}
	a := cloneOrEmpty(idx.values[valuePath][values[0]])
	for _, v := range values[1:] {
		bm := idx.values[valuePath][v]
		if bm == nil {
			a = sroar.NewBitmap()
			break
		}
		a.And(bm)
	}
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}
	return pinnedFromValueSet(idx, a, valuePath, pins)
}

// leafContainsNone realizes `path ContainsNone [values]` as a direct
// chained subtraction. Equivalent to negate(leafContainsAny(...)) but
// computed in one accumulator pass without materialising the value
// union.
//
// Algebra (using the identity A ANDNOT (B ∪ C) = (A ANDNOT B) ANDNOT C): //
//
//	Witnesses = _anchor(TS) ANDNOT ⋃_v _value(path, v)
//	             = ((_anchor(TS) ANDNOT v_1) ANDNOT v_2) ... ANDNOT v_N
//	Bitmap         = (_exists(TS) ANDNOT _anchor(TS)) ∪ Witnesses
//	             (satisfies Bitmap ∩ _anchor(TS) == Witnesses)
//
// Semantics: a TS-element satisfies ContainsNone if it contains none of
// the listed values. For scalar arrays (e.g. text[]) that means no
// element in the array matches any listed value. CP=false (chain bits
// above TS aren't authentic — same as any owner-level negation).
//
// Panics on an empty value list — ContainsNone over an empty set is
// vacuously true and should be optimised away upstream.
func leafContainsNone(idx *fastPathIndex, path string, values ...any) *fastPathResult {
	if len(values) == 0 {
		panic("leafContainsNone requires at least one value")
	}
	truthScope := parentPath(path)
	truthAnchor := idx.anchor[truthScope]

	// Witnesses = _anchor(TS) ANDNOT each value bucket (in order).
	es := cloneOrEmpty(truthAnchor)
	for _, v := range values {
		if bm := idx.values[path][v]; bm != nil {
			es.AndNot(bm)
		}
	}

	// Bitmap = (_exists(TS) ANDNOT _anchor(TS)) ∪ Witnesses.
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(truthAnchor).Or(es)

	return &fastPathResult{
		Scope:      truthScope,
		Bitmap:     rich,
		Witnesses:  es,
		CleanAbove: truthScope,
		CleanBelow: false, // negation shape — _exists carries descendants of all TS-elements
	}
}

// leafPinnedContainsNone realizes `<pinned path>.x ContainsNone [values]`
// as owner-level negation of pinned ContainsAny: a TS-element passes
// only if NO descendant under the pinned subtree matches any of the
// listed values.
//
// Unlike leafPinnedNot / leafPinnedIsNullTrue, this helper does NOT
// dispatch on hasPinGap. The operator name "Contains None" is a
// universal claim about the entire array — every element must not
// match — which is the owner-level reading. A per-element dispatch
// would let a single non-matching element witness pass the predicate
// even when other elements match, contradicting the "none" semantic.
// The corresponding per-element-existential reading is available
// explicitly via `ANY(... : NOT ...)` style queries.
//
// Panics on an empty pin list or empty value list.
func leafPinnedContainsNone(idx *fastPathIndex, valuePath string, pins []pinSpec, values ...any) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned ContainsNone needs at least one pin")
	}
	if len(values) == 0 {
		panic("pinned ContainsNone requires at least one value")
	}
	return negate(idx, leafPinnedContainsAny(idx, valuePath, pins, values...))
}

// ---------------------------------------------------------------------------
// Merge builders
// ---------------------------------------------------------------------------

// commonScope returns the longest common ancestor of two qualified path
// strings (segments delimited by "."). For schemas with linear hierarchy
// it's either a prefix of both inputs or equal to one of them.
func commonScope(a, b string) string {
	if a == b {
		return a
	}
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")
	n := min(len(bParts), len(aParts))
	i := 0
	for i < n && aParts[i] == bParts[i] {
		i++
	}
	return strings.Join(aParts[:i], ".")
}

// andLeaves performs an AND merge of two leaf results. See the AND merge
// dispatch matrix near the top of this file for the full table of cases;
// inline comments below mark each branch.
func andLeaves(idx *fastPathIndex, left, right *fastPathResult) *fastPathResult {
	if left.Scope == right.Scope {
		return andAtScope(idx, left, right, left.Scope, left.Scope)
	}

	common := commonScope(left.Scope, right.Scope)

	// Siblings (LCA is above both) → lift both up, recurse as same-scope.
	if common != left.Scope && common != right.Scope {
		return andLeaves(idx,
			liftToScope(idx, left, common),
			liftToScope(idx, right, common))
	}

	// One is ancestor of the other.
	var ancestor, child *fastPathResult
	if common == left.Scope {
		ancestor, child = left, right
	} else {
		ancestor, child = right, left
	}

	if ancestor.CleanBelow && (ancestor.aboveScopeClean() || ancestor.Scope == idx.propName) {
		// A broadcasts authentic descendants down through child.TS, AND its
		// Bitmap is clean at-or-above A.Scope (either there's a real cleanness
		// ceiling above A.Scope, OR A.Scope is the property root and there's no
		// scope above to be unclean at). Merge at child.TS. The structural
		// cleanness CEILING for the AND is ancestor.TS (not the merge scope
		// C.Scope): at scopes in [C.Scope, A.Scope] the chain bits in either operand
		// are per-element selfMarkers and the intersection is genuine
		// same-element correlation; strictly above A.Scope the chain bits are
		// shared and the AND can produce owner-level ghosts.
		//
		// The `|| ancestor.TS == propName` clause matters at the property
		// root (L0-style schema): a positive leaf there has CS=TS=propName,
		// so aboveScopeClean() is false even though CleanBelow=true.
		// Without this clause the broadcasting branch would never fire at
		// L0 even for trivially-authentic positive operands.
		return andAtScope(idx, child, ancestor, child.Scope, ancestor.Scope)
	}
	if pathDepth(child.CleanAbove) <= pathDepth(ancestor.Scope) {
		// C's clean range reaches A.Scope — its chain bits at A.Scope are
		// already authentic. Intersect directly at A.Scope, no lift needed.
		// A.Bitmap is trustworthy at A.Scope by the scope invariant. Ceiling =
		// merge scope (= A.Scope): above A.Scope, A's own ghosts persist and the
		// merge can't recover them.
		//
		// Distinct from the simpler `child.aboveScopeClean()` test because we
		// need C to be clean SPECIFICALLY AT A.Scope, not just somewhere
		// between C.Scope and root. A compound C (e.g. the result of an
		// ancestor+child AND with CS=somewhere-between) may be
		// aboveScopeClean yet still have ghosts at A.Scope — in which case we
		// must lift.
		return andAtScope(idx, ancestor, child, ancestor.Scope, ancestor.Scope)
	}
	// C's clean range stops below A.Scope (or C is fully unclean above TS).
	// C's bits at A.Scope may include ghosts; ANDing directly would let
	// those coincide with A's authentic ES and leak. Lift C up to A.Scope
	// so its A.Scope bits are rebuilt from authentic Witnesses
	// (LiftToAncestor wipes ghost bits), then merge same-scope at A.Scope.
	lifted := liftToScope(idx, child, ancestor.Scope)
	return andAtScope(idx, ancestor, lifted, ancestor.Scope, ancestor.Scope)
}

// andAtScope is the shared core merge formula used by every branch of
// andLeaves once the merge scope and cleanness ceiling are known.
// `scope` is the merge scope (= result.Scope). `ceiling` is the
// shallowest scope at which the AND's bitmap intersection is still
// per-element clean — usually equal to `scope`, but for ancestor+child
// where the ancestor is clean above its own TS the ceiling RISES to
// ancestor.TS even though the merge happens at the deeper child.TS.
// result.CleanAbove is the deepest of (ceiling, left.CleanAbove,
// right.CleanAbove) — the AND can only be clean at scopes where the
// structural ceiling AND both operands provide cleanness.
// result.CleanBelow survives only when both operands broadcast down —
// the intersection of two authentic-descendant bitmaps is authentic at
// every scope below scope, but if either operand carries bogus
// descendants the intersection may pick them up.
func andAtScope(idx *fastPathIndex, left, right *fastPathResult, scope, ceiling string) *fastPathResult {
	rich := left.Bitmap.Clone().And(right.Bitmap)
	support := rich.Clone().And(idx.anchor[scope])
	return &fastPathResult{
		Scope:      scope,
		Bitmap:     rich,
		Witnesses:  support,
		CleanAbove: deepestPath(ceiling, left.CleanAbove, right.CleanAbove),
		CleanBelow: left.CleanBelow && right.CleanBelow,
	}
}

// liftToScope re-wraps a leaf result at a higher (ancestor) Scope.
// Useful when sibling leaves with different natural witness scopes share a
// common ancestor where the merge should land (e.g. `cars.accessories.type`
// at Scope=accessories and `cars.tires.width` at Scope=tires both
// merge at cars).
//
// The lift method depends on whether targetScope falls within the operand's
// clean range: //
//
//   - depth(leaf.CleanAbove) <= depth(targetScope) (= targetScope is at-or-
//     below leaf.CleanAbove, i.e. inside the clean range): Bitmap is already
//     authentic at targetScope, so anchor intersection suffices — no ghost
//     ancestors to worry about. Bitmap is reused unchanged. The lifted
//     result KEEPS the operand's higher CleanAbove: Bitmap is byte-for-byte
//     the same bitmap, so every cleanness claim the original made still
//     holds. Only Scope changes.
//
//   - depth(leaf.CleanAbove) > depth(targetScope) (= targetScope is strictly
//     above leaf.CleanAbove, outside the clean range): Bitmap carries ghosts
//     at intermediate scopes. Use a real predecessor lift (LiftToAncestor)
//     on Witnesses to project to targetScope, then reconstruct Bitmap at
//     the new scope by stripping target-scope ghost bits and OR-ing back
//     the authentic lifted ES. The lift only rebuilds the targetScope-level
//     bits; scopes strictly above targetScope keep whatever ghosts the
//     original Bitmap carried. The lifted result claims cleanness only at
//     targetScope itself — CleanAbove = targetScope.
//
// targetScope must be an ancestor of leaf.Scope in the schema (or
// equal, in which case the input is returned unchanged). Caller is
// responsible for ensuring this — no schema lookup happens here.
func liftToScope(idx *fastPathIndex, leaf *fastPathResult, targetScope string) *fastPathResult {
	if leaf.Scope == targetScope {
		return leaf
	}
	targetAnchor := idx.anchor[targetScope]
	var rich, support *sroar.Bitmap
	var cleanScope string
	if pathDepth(leaf.CleanAbove) <= pathDepth(targetScope) {
		// Cheap lift: Bitmap is unchanged, so it stays authentic at every
		// scope the operand was originally clean at (down to leaf.CleanAbove,
		// which is at-or-shallower-than targetScope). Preserving CleanAbove
		// matters for OR: keeping a shallower CleanAbove here lets the OR
		// of a deep operand with a shallow CP=true operand retain its
		// higher cleanness reach.
		rich = leaf.Bitmap
		support = leaf.Bitmap.Clone().And(targetAnchor)
		cleanScope = leaf.CleanAbove
	} else {
		// Real lift: targetScope is above the operand's clean range. Only
		// the targetScope-level bits get rebuilt; everything above remains
		// ghost-prone. The lifted result can only claim cleanness at
		// targetScope.
		support, _ = idx.ops.LiftToAncestor(leaf.Witnesses, targetAnchor)
		rich = leaf.Bitmap.Clone().AndNot(targetAnchor).Or(support)
		cleanScope = targetScope
	}
	return &fastPathResult{
		Scope:      targetScope,
		Bitmap:     rich,
		Witnesses:  support,
		CleanAbove: cleanScope,
		CleanBelow: leaf.CleanBelow,
	}
}

// orLeaves performs an OR merge of two leaf results. See the OR merge
// dispatch matrix near the top of this file for the full table of cases.
// The implementation is a single rule: lift both operands to their LCA
// (no-op when already there; cheap anchor-intersect when target is within
// the operand's clean range; full LiftToAncestor otherwise) and union
// them. The clean-range shortcut is entirely encoded inside liftToScope.
//
// result.CleanAbove = deepest of the two lifted operands' CleanScopes.
// Because the cheap-lift path preserves the operand's original (higher)
// CleanAbove, an OR of two CP=true-equivalent operands keeps cleanness
// up to the property root — the union of authentic bits at every scope
// is itself authentic. An OR where one operand has CleanAbove below LCA
// (e.g. the result of a deeper AND) gets the deeper of the two as its
// own ceiling.
func orLeaves(idx *fastPathIndex, left, right *fastPathResult) *fastPathResult {
	lca := commonScope(left.Scope, right.Scope)
	l := liftToScope(idx, left, lca)
	r := liftToScope(idx, right, lca)
	return &fastPathResult{
		Scope:      lca,
		Bitmap:     l.Bitmap.Clone().Or(r.Bitmap),
		Witnesses:  l.Witnesses.Clone().Or(r.Witnesses),
		CleanAbove: deepestPath(l.CleanAbove, r.CleanAbove),
		CleanBelow: l.CleanBelow && r.CleanBelow,
	}
}

// andN folds N operands via andLeaves, sorted by Scope depth
// (deepest first) with CP=true breaking ties before CP=false within
// the same scope. The pairwise dispatch is order-invariant for
// correctness; the sort is purely to minimise intermediate work: //
//
//   - Deepest-first keeps consecutive same-TS operands adjacent at the
//     front so the same-TS branch of andLeaves folds them with no
//     lifts. The running result stays at the deepest TS as long as
//     subsequent shallower operands have CP=true (Row 1 of the
//     dispatch matrix — cheap no-lift merge at child.TS).
//
//   - CP=true-first within a same-TS run lets the running accumulator
//     stay CP=true for as long as possible across subsequent
//     ancestor-child merges, again favouring Row 1.
//
// Panics on an empty input. A single-operand call returns the operand
// unchanged.
//
// TODO aliszka:nested_filtering — add a grouping pass for CP=false
// operands that share a Scope. The current strategy folds left-
// deep, which is optimal when CP=true operands can be absorbed
// individually via the ancestor-child direct-merge shortcut. It is
// sub-optimal when many CP=false operands share a TS and the running
// accumulator needs a real LiftToAncestor at every sibling boundary.
//
// Sketch: bucket operands by Scope; within each bucket fold
// CP=false operands together first (no lift, just bitmap intersection
// at the shared TS). The per-bucket result is then lifted ONCE to the
// LCA of the buckets, instead of one real lift per operand. CP=true
// operands stay outside the buckets and feed into the sequential fold
// as today so they keep hitting Row 2's no-lift path. With this split
// the total lift count is bounded by the number of CP=false buckets
// rather than the number of CP=false operands.
func andN(idx *fastPathIndex, operands ...*fastPathResult) *fastPathResult {
	if len(operands) == 0 {
		panic("andN requires at least one operand")
	}
	if len(operands) == 1 {
		return operands[0]
	}
	sorted := make([]*fastPathResult, len(operands))
	copy(sorted, operands)
	sort.SliceStable(sorted, func(i, j int) bool {
		di := pathDepth(sorted[i].Scope)
		dj := pathDepth(sorted[j].Scope)
		if di != dj {
			return di > dj
		}
		// Within a same-TS run, prefer operands whose Bitmap is clean at more
		// scopes above TS (shallower CleanAbove). Mirrors the old
		// CP=true-first tiebreaker: cleaner operands stay at the front
		// so the running accumulator keeps its cleanest reach for as long
		// as possible across subsequent ancestor merges.
		return pathDepth(sorted[i].CleanAbove) < pathDepth(sorted[j].CleanAbove)
	})
	result := sorted[0]
	for i := 1; i < len(sorted); i++ {
		result = andLeaves(idx, result, sorted[i])
	}
	return result
}

// orN folds N operands via orLeaves, sorted by Scope depth
// (deepest first) with CP=true breaking ties before CP=false within
// the same scope. The pairwise dispatch is order-invariant for
// correctness; the sort is purely to minimise intermediate work.
//
// Unlike andN, OR always lands at the LCA of all operands — there is
// no Row 1 "stay at the deepest TS" shortcut to exploit, because
// merging below the LCA would lose any operand's "container exists"
// contribution at the higher scope. The sort still helps in two
// secondary ways: //
//
//   - Deepest-first clusters same-TS operands at the front. Same-TS
//     OR folds are pure unions with no lift, regardless of CP.
//
//   - CP=true-first within a same-TS run keeps the running accumulator
//     CP=true as long as possible. When subsequent shallower operands
//     promote the result to a higher LCA, each lift is a cheap
//     anchor-intersect rather than a full LiftToAncestor — but only if
//     the accumulator was CP=true at promotion time.
//
// Panics on an empty input. A single-operand call returns the operand
// unchanged.
//
// TODO aliszka:nested_filtering — add a grouping pass for CP=false
// operands that share a Scope. Same motivation as the andN TODO: // folding CP=false operands locally first (pure same-TS union, no
// lift) and lifting the per-bucket result ONCE to the LCA reduces the
// total LiftToAncestor count from O(operands) to O(buckets) when
// CP=false operands dominate. CP=true operands stay in the sequential
// fold so they keep using the cheap anchor-intersect lift path.
func orN(idx *fastPathIndex, operands ...*fastPathResult) *fastPathResult {
	if len(operands) == 0 {
		panic("orN requires at least one operand")
	}
	if len(operands) == 1 {
		return operands[0]
	}
	sorted := make([]*fastPathResult, len(operands))
	copy(sorted, operands)
	sort.SliceStable(sorted, func(i, j int) bool {
		di := pathDepth(sorted[i].Scope)
		dj := pathDepth(sorted[j].Scope)
		if di != dj {
			return di > dj
		}
		// Within a same-TS run, prefer operands whose Bitmap is clean at more
		// scopes above TS (shallower CleanAbove). Mirrors the old
		// CP=true-first tiebreaker: cleaner operands stay at the front
		// so the running accumulator keeps its cleanest reach for as long
		// as possible across subsequent ancestor merges.
		return pathDepth(sorted[i].CleanAbove) < pathDepth(sorted[j].CleanAbove)
	})
	result := sorted[0]
	for i := 1; i < len(sorted); i++ {
		result = orLeaves(idx, result, sorted[i])
	}
	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parentPath(path string) string {
	if i := strings.LastIndex(path, "."); i >= 0 {
		return path[:i]
	}
	return ""
}

// pathDepth returns the number of segments in a dot-separated path.
// The empty string (root) has depth 0; "countries" has depth 1;
// "countries.garages.cars" has depth 3.
func pathDepth(path string) int {
	if path == "" {
		return 0
	}
	return strings.Count(path, ".") + 1
}

func cloneOrEmpty(bm *sroar.Bitmap) *sroar.Bitmap {
	if bm == nil {
		return sroar.NewBitmap()
	}
	return bm.Clone()
}

// docIDs runs Witnesses through the real BitmapOps.MaskRootLeaf so the
// projection path itself is under test.
func (i *fastPathIndex) docIDs(r *fastPathResult) []uint64 {
	if r.Witnesses == nil || r.Witnesses.IsEmpty() {
		return nil
	}
	doc, _ := i.ops.MaskRootLeaf(r.Witnesses)
	return doc.ToArray()
}

// ---------------------------------------------------------------------------
// L2 schema and fixtures (countries.garages.cars)
// ---------------------------------------------------------------------------

func l2Schema() *models.Property {
	tx := func(name string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeText)}}
	}
	in := func(name string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeInt)}}
	}
	txArr := func(name string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeTextArray)}}
	}
	objArr := func(name string, nested ...*models.NestedProperty) *models.NestedProperty {
		return &models.NestedProperty{
			Name:             name,
			DataType:         []string{string(schema.DataTypeObjectArray)},
			NestedProperties: nested,
		}
	}
	return &models.Property{
		Name:     "countries",
		DataType: []string{string(schema.DataTypeObjectArray)},
		NestedProperties: []*models.NestedProperty{
			objArr("garages",
				tx("city"),
				objArr("cars",
					in("year"),
					tx("make"),
					tx("name"),
					txArr("colors"),
					objArr("accessories", tx("type")),
					objArr("tires", in("width")),
					objArr("doors", in("count")),
				),
			),
		},
	}
}

// l2Docs returns the L2 fixtures. 100–600 are the core single-leaf fixtures.
// 700 carries an empty-cars garage alongside a non-empty one — needed for
// owner-level `cars IS NULL true|false` on object[]. Docs 800/900 will be
// added when sibling-merge / ghost-recovery tests land.
func l2Docs() map[uint64]any {
	return map[uint64]any{
		100: []any{ // single country, single garage, three cars
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020, "make": "toyota", "colors": []any{"blue"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
					map[string]any{"year": 2018, "make": "honda", "colors": []any{"blue", "red"}, "accessories": []any{map[string]any{"type": "radio"}}, "tires": []any{map[string]any{"width": 195}}},
					map[string]any{"year": 2017, "name": "honda"},
				}},
			}},
		},
		200: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2015, "make": "honda", "colors": []any{"red"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
					map[string]any{"year": 2020, "make": "honda", "colors": []any{"blue"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
					map[string]any{"name": "ford"},
				}},
			}},
		},
		300: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2011, "make": "ford", "colors": []any{"green"}},
					map[string]any{"make": "honda", "colors": []any{"red"}}, // cars[1]: no year
				}},
			}},
		},
		400: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2012, "make": "ford", "colors": []any{"yellow"}},
				}},
			}},
		},
		500: []any{ // single country, two garages
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2018, "make": "mazda"},
					map[string]any{"year": 2019, "make": "ford"},
				}},
				map[string]any{"cars": []any{
					map[string]any{"year": 2022, "make": "bmw"},
					map[string]any{"year": 2016, "name": "honda"},
					map[string]any{"name": "honda"},
				}},
			}},
		},
		600: []any{ // two countries, one garage each
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2010, "make": "ford"},
					map[string]any{"year": 2018, "make": "mazda"},
					map[string]any{"name": "ford"},
				}},
			}},
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2011, "make": "bmw"},
					map[string]any{"year": 2020, "make": "toyota"},
					map[string]any{"name": "honda"},
				}},
			}},
		},
		700: []any{ // single country, two garages: one empty cars, one non-empty
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
				map[string]any{"cars": []any{
					map[string]any{"year": 2014, "make": "ford"},
				}},
			}},
		},
		// 810: every car matches `year=2020`. Forces FAIL paths for NOT and
		// IS NULL true to actually fire (without this, every fixture had at
		// least one car with year != 2020 or missing year, so the unpinned
		// negation helpers would pass even if the positive subtraction were
		// a no-op).
		810: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020, "make": "honda"},
					map[string]any{"year": 2020, "make": "toyota"},
				}},
			}},
		},
		// 820: two countries each with cars[1].year=2020. Forces NOT
		// cars[1].year=2020 to FAIL at doc level (every root has the pinned
		// match) — exercises the multi-root all-match case that doc 600
		// only half-covered (in doc 600 only one country matches).
		820: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2018, "make": "ford"},
					map[string]any{"year": 2020, "make": "honda"},
				}},
			}},
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2019, "make": "kia"},
					map[string]any{"year": 2020, "make": "mazda"},
				}},
			}},
		},
		// 830: car with multiple accessories — spoiler at index 1, not 0.
		// Verifies walker positions for non-first array elements at depth 4.
		// Car has no year/colors/make so its contribution to year/color tests
		// is limited to "missing scalar" witnesses (E, F, G, H, (5b)).
		830: []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"accessories": []any{
						map[string]any{"type": "radio"},
						map[string]any{"type": "spoiler"},
					}},
				}},
			}},
		},
		// 800: positive fixture for ghost recovery. One garage (warsaw)
		// contains a single car that satisfies spoiler+205+door=4. The
		// other garage (krakow) splits spoiler and 205 across two cars —
		// produces a ghost ancestor at the krakow garage when (spoiler AND
		// 205) is merged at cars. An ancestor constraint city=warsaw filters
		// the krakow ghost; a same-scope sibling like doors.count=4 doesn't.
		800: []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "warsaw", "cars": []any{
					map[string]any{
						"accessories": []any{map[string]any{"type": "spoiler"}},
						"tires":       []any{map[string]any{"width": 205}},
						"doors":       []any{map[string]any{"count": 4}},
					},
				}},
				map[string]any{"city": "krakow", "cars": []any{
					map[string]any{
						"accessories": []any{map[string]any{"type": "spoiler"}},
						"doors":       []any{map[string]any{"count": 4}},
					},
					map[string]any{
						"tires": []any{map[string]any{"width": 205}},
						"doors": []any{map[string]any{"count": 4}},
					},
				}},
			}},
		},
		// 900: negative fixture for ghost recovery. Same krakow split as
		// 800, but the warsaw garage has no spoiler+205 witness —
		// only doors. With city=warsaw the result is empty (ghost in krakow
		// is filtered, no witness in warsaw). With doors.count=4 it stays
		// empty (sibling matches in krakow too, ghost survives but no real
		// car witness anywhere).
		900: []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "warsaw", "cars": []any{
					map[string]any{
						"doors": []any{map[string]any{"count": 4}},
					},
				}},
				map[string]any{"city": "krakow", "cars": []any{
					map[string]any{
						"accessories": []any{map[string]any{"type": "spoiler"}},
						"doors":       []any{map[string]any{"count": 4}},
					},
					map[string]any{
						"tires": []any{map[string]any{"width": 205}},
						"doors": []any{map[string]any{"count": 4}},
					},
				}},
			}},
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// buildL2 ingests every L2 fixture into a fresh index.
func buildL2(t *testing.T) *fastPathIndex {
	t.Helper()
	prop := l2Schema()
	idx := newFastPathIndex("countries")
	for docID, value := range l2Docs() {
		idx.addDoc(t, prop, docID, value)
	}
	return idx
}

// fastPathTestCase is the table row used by both single-leaf and merge tests.
// build constructs the result lazily so each subtest captures its own
// expression — easier to scan than a slice of *fastPathResult.
//
// wantCleanAbove names the SHALLOWEST scope at which the result's Bitmap is
// still ghost-free (Bitmap ∩ _anchor(S) yields exactly wantDocs after
// MaskRootLeaf, for every S between Scope and CleanAbove inclusive).
// Above CleanAbove, Bitmap may carry owner-level ghosts — no assertion is
// made there.
type fastPathTestCase struct {
	name           string
	build          func(*fastPathIndex) *fastPathResult
	wantScope      string
	wantCleanAbove string
	wantDocs       []uint64 // after the real MaskRootLeaf projection
}

// runFastPathCases drives the standard assertion suite for any
// fastPathTestCase table: scope/CleanAbove equality, ES non-empty iff
// wantDocs non-empty, trustworthy-scope invariant, doc-id equality, and
// the no-ghost contract over [Scope, CleanAbove].
func runFastPathCases(t *testing.T, idx *fastPathIndex, cases []fastPathTestCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.build(idx)
			assert.Equal(t, tc.wantScope, r.Scope)
			assert.Equal(t, tc.wantCleanAbove, r.CleanAbove)
			// Witnesses mirrors wantDocs: non-empty iff wantDocs non-empty.
			// Bitmap is non-empty in the populated case; for empty-result we
			// skip the Bitmap check because some negation shapes can leave
			// ancestor chain bits in Bitmap even when Witnesses is empty.
			if len(tc.wantDocs) == 0 {
				assert.True(t, r.Witnesses.IsEmpty(), "Witnesses must be empty when wantDocs is empty")
			} else {
				assert.False(t, r.Bitmap.IsEmpty(), "Bitmap must not be empty")
				assert.False(t, r.Witnesses.IsEmpty(), "Witnesses must not be empty")
			}
			// Trustworthy-scope invariant: Bitmap ∩ _anchor(Scope) must
			// equal Witnesses. Holds regardless of CleanAbove — TS is
			// always inside the clean range and the equality is a structural
			// property of how Witnesses is built from Bitmap.
			richAtTruth := r.Bitmap.Clone().And(idx.anchor[r.Scope])
			assert.Equal(t, r.Witnesses.ToArray(), richAtTruth.ToArray(),
				"Bitmap ∩ _anchor(Scope) must equal Witnesses")
			assert.ElementsMatch(t, tc.wantDocs, idx.docIDs(r))

			// Ghost-free contract for the entire clean range: walk from
			// Scope up to and including CleanAbove. At each scope,
			// projecting Bitmap via anchor and masking to docIDs must yield
			// exactly wantDocs. The loop stops before going above
			// CleanAbove — that's where owner-level ghosts are allowed to
			// live, and the result explicitly disclaims cleanness there.
			//
			// Loop exits when scope would become parentPath(CleanAbove) —
			// i.e., one step above CleanAbove — which naturally handles the
			// "CleanAbove = propName" case (parentPath(propName) = "" and we
			// never read idx.anchor[""]).
			stop := parentPath(r.CleanAbove)
			for scope := r.Scope; scope != stop; scope = parentPath(scope) {
				richAtScope := r.Bitmap.Clone().And(idx.anchor[scope])
				docs, _ := idx.ops.MaskRootLeaf(richAtScope)
				assert.ElementsMatch(t, tc.wantDocs, docs.ToArray(),
					"clean range [TS, CleanAbove]: Bitmap ∩ _anchor(%q) must yield wantDocs", scope)
			}
		})
	}
}

// TestFastPathL2_SingleLeaf exercises the single-leaf shapes against the L2
// schema. Each subtest verifies the full result quadruple (Scope, Bitmap,
// Witnesses, CanProjectParentByAnchor) plus the masked doc IDs from
// BitmapOps.MaskRootLeaf.
func TestFastPathL2_SingleLeaf(t *testing.T) {
	idx := buildL2(t)

	cases := []fastPathTestCase{
		{
			name: "cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.year", 2020)
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{100, 200, 600, 810, 820},
		},
		{
			name: "cars[1].year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.year",
					2020,
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			wantDocs:       []uint64{200, 600, 810, 820},
		},
		{
			name: "cars.year IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars.year")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820},
		},
		{
			name: "cars[1].year IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.year",
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			wantDocs:       []uint64{100, 200, 500, 600, 810, 820},
		},
		{
			name: "cars.year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "countries.garages.cars.year")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			wantDocs:       []uint64{200, 300, 500, 600, 800, 830, 900},
		},
		{
			name: "cars[1].year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.year",
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			wantDocs:       []uint64{300, 400, 700, 800, 830, 900},
		},
		{
			name: "NOT cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.year", 2020)
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// 810 FAILS: every car has year=2020. 820 PASSES: each country
			// has cars[0] with year != 2020 (a per-element witness). 830
			// PASSES: car has no year, which counts as "not 2020". 800/900
			// PASS: cars in those docs have no year.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 820, 830, 900},
		},
		{
			name: "NOT cars[1].year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.year",
					2020,
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// 810 FAILS: cars[1].year=2020 in the only garage. 820 FAILS: // every country's cars[1] has year=2020 — no garage witness anywhere.
			// 830 PASSES: no cars[1] → missing pinned slot satisfies NOT.
			// 800/900 PASS: cars[1] missing or has no year.
			wantDocs: []uint64{100, 300, 400, 500, 600, 700, 800, 830, 900},
		},
		{
			name: "cars IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars")
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "cars IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "countries.garages.cars")
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			wantDocs:       []uint64{700},
		},
		// Scalar array (text[]) — verifies chain propagation through
		// walkScalarArray and that leafPositive / leafNot work over
		// per-element value/anchor entries without modification.
		{
			name: "cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.colors", "red")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{100, 200, 300},
		},
		{
			name: "NOT cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.colors", "red")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Every doc has at least one car lacking "red" (most fixtures
			// have no colors property at all — cars stay in _anchor(cars)
			// but contribute no posExact). All docs pass.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Depth-4 nested scalar (cars → accessories → type) — verifies chain
		// propagation through an extra level of walkNestedArray.
		{
			name: "cars.accessories.type=spoiler",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler")
			},
			wantScope:      "countries.garages.cars.accessories",
			wantCleanAbove: "countries",
			// 830 PASSES via accessories[1] — verifies walker positions for
			// non-first array elements at depth 4. 800 PASSES via spoiler
			// accessories in both g[0].cars[0] and g[1].cars[0]; 900 PASSES
			// via spoiler in g[1].cars[0].
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		{
			name: "NOT cars.accessories.type=spoiler",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.accessories.type", "spoiler")
			},
			wantScope:      "countries.garages.cars.accessories",
			wantCleanAbove: "countries.garages.cars.accessories",
			// doc 100: cars[1]'s radio is non-spoiler witness.
			// doc 200: all spoilers → no witness → FAIL.
			// 300-820: no accessories → empty scope, no witness possible → FAIL.
			// 830: accessories[0] is radio → witness.
			wantDocs: []uint64{100, 830},
		},
		// Empty-result edge case — value present in the schema but no doc
		// has it. Exercises the no-match path through every helper
		// invariant (Bitmap/Witnesses empty, MaskRootLeaf on empty input).
		{
			name: "cars.year=9999 (empty result)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.year", 9999)
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			wantDocs:       nil,
		},
		// Multi-pin path — two pins (garages[1] + cars[2]). Verifies the
		// narrow+lift chain across multiple array levels. Single-pin
		// equivalent `cars[2].name=honda` would also match docs 100 and 600;
		// the garages[1] pin filters them out, leaving only doc 500 (the
		// only fixture with garages[1].cars[2].name=honda).
		{
			name: "garages[1].cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.name",
					"honda",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{500},
		},
		{
			name: "garages[1].cars[2].name IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.name",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Only doc 500 has the full pinned path with a name field.
			wantDocs: []uint64{500},
		},
		{
			name: "garages[1].cars[2].name IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.name",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// All countries pass except doc 500's (which has the full
			// pinned path with a name). Missing-pin cases (no garages[1] or
			// no cars[2]) all count as witnesses — a missing pinned slot
			// satisfies IS NULL true.
			// 800/900 PASS: g[1] exists but cars[2] missing.
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT garages[1].cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.name",
					"honda",
					[]pinSpec{
						{"countries.garages", 1},
						{"countries.garages.cars", 2},
					})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Same set as the IS NULL true case — the only doc where the
			// full pinned path has name=honda is doc 500. Every other doc
			// passes either via missing pin or via name != honda.
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 800, 810, 820, 830, 900},
		},
		// Intermediate-pin: pin at garages[1] with cars unpinned. Discriminates
		// from single-pin `cars.make=bmw` (which would also include doc 600).
		// Only doc 500 has garages[1] containing a car with make=bmw.
		// NOT and IS NULL true variants use a per-element formula because
		// the pin chain has a gap (cars is unpinned between TS=countries
		// and ElementScope=cars). leafPinnedNot and leafPinnedIsNullTrue
		// dispatch internally on hasPinGap and call perElementNotFromSubtractand.
		{
			name: "garages[1].cars.make=bmw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.make",
					"bmw",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{500},
		},
		{
			name: "garages[1].cars.make IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// doc 500: g[1].cars[0].make=bmw. doc 700: g[1].cars[0].make=ford.
			// All other docs lack garages[1] or have no make on any car in g[1].
			wantDocs: []uint64{500, 700},
		},
		// Per-element NOT on intermediate-pin path. Every doc passes
		// either via a per-element witness (some car under g[1] with
		// make!=bmw, or with no make field) or via missing-pin success
		// (the country has no g[1] slot at all).
		{
			name: "NOT garages[1].cars.make=bmw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.make",
					"bmw",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Missing-pin: docs 100-400, 600 (both countries lack g[1]),
			// 810, 820 (both countries lack g[1]), 830 — all pass via
			// tsMissingPin.
			// Per-element witnesses: // - doc 500 g[1]: cars[0]=bmw is positive but cars[1]/[2]
			//   have name=honda with no make field — both are non-bmw
			//   witnesses.
			// - doc 700 g[1].cars[0]=ford — non-bmw witness.
			// - docs 800/900 g[1] cars have no make field at all — every
			//   car is a non-bmw witness.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Per-element IS NULL true on intermediate-pin path. Passes any
		// doc with at least one car under g[1] missing the make field,
		// OR with no g[1] at all. Owner-level would have excluded doc
		// 500 (g[1] has cars[0] with a make field); per-element correctly
		// includes 500 via cars[1] and cars[2].
		{
			name: "garages[1].cars.make IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Missing-pin: docs 100-400, 600, 810, 820, 830 (no g[1]).
			// Per-element witnesses: // - doc 500: g[1].cars[1] and cars[2] have name=honda but no
			//   make field — both are IS NULL true witnesses.
			// - docs 800/900: g[1] cars have no make at all — every car
			//   is a witness.
			// doc 700 FAILS: g[1].cars[0]=ford has a make field, and it's
			// the only car under g[1]. No per-element witness; pin is
			// present so no missing-pin contribution either.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
	}

	runFastPathCases(t, idx, cases)
}

// TestFastPathL2_Merge exercises compound (AND/OR) shapes against the L2
// schema, building on the single-leaf helpers. Each case constructs a
// compound via merge builders (andLeaves, …) operating on leaf results.
func TestFastPathL2_Merge(t *testing.T) {
	idx := buildL2(t)

	cases := []fastPathTestCase{
		// Positive AND at the same scope. Same-element semantics fall out
		// from intersecting chain-bearing Riches: only cars that satisfy
		// BOTH predicates simultaneously survive at the car self level.
		// CanProject=false: Bitmap at parent (garages) keeps chain bits for
		// docs where different cars in the same garage satisfy each leaf
		// (e.g. doc 100 cars[0]=2020/toyota + cars[1]=2018/honda) — that
		// would falsely project to garage level.
		{
			name: "cars.make=honda AND cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// doc 200: cars[1] has both year=2020 and make=honda.
			// doc 810: cars[0] has both year=2020 and make=honda.
			// doc 820 country 0: cars[1] has both. (Country 1 has neither in
			// the same car — but doc passes existentially via country 0.)
			// All other docs have year=2020 and make=honda in different cars
			// or not at all.
			wantDocs: []uint64{200, 810, 820},
		},
		// Positive OR at the same scope. Each leaf contributes its own
		// matching cars; the merged result is "exists a car matching either
		// predicate". CanProject=true is preserved because Bitmap chain bits at
		// parent legitimately indicate "garages with at least one matching
		// car for either leaf" — the existential OR semantic at parent.
		{
			name: "cars.make=honda OR cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			// 100: cars[0].year=2020. 200: cars[0].make=honda. 300: // cars[1].make=honda. 600 country 1: cars[1].year=2020.
			// 810: cars[0] year=2020 (and make=honda). 820 country 0: // cars[1] year=2020 (and make=honda). 400/500/700/830 lack both.
			wantDocs: []uint64{100, 200, 300, 600, 810, 820},
		},
		// Same property, different values — AND requires the same car's
		// scalar-array (colors) to contain BOTH values. Same shape as the
		// scalar-AND case above but on text[]: each colors element emits at
		// its own leaf position while sharing the car's chain. Intersection
		// at car self level survives only when a single car has both colors.
		{
			name: "cars.colors=blue AND cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.colors", "blue"),
					leafPositive(idx, "countries.garages.cars.colors", "red"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// doc 100: cars[1].colors=["blue","red"]. Other docs either lack
			// colors entirely (500/600/700/810/820/830), have only one of the
			// values (200 has blue and red split across cars[0]/cars[1]; 300
			// has only red; 400 has only "yellow"), or have neither.
			wantDocs: []uint64{100},
		},
		// Sibling branches under a shared owner. Each leaf's natural
		// Scope is its own array element (accessories vs tires); the
		// merge must land at the common ancestor (cars). andLeaves detects
		// the sibling relationship and lifts both operands to the LCA before
		// merging.
		{
			name: "cars.accessories.type=spoiler AND cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// doc 100: cars[0] has both spoiler-accessory and tires.width=205.
			// doc 200: cars[0] and cars[1] both have both. doc 800 g[0].cars[0]
			// has both. Other docs lack either accessories or tires (or both);
			// doc 830 has a spoiler accessory on cars[0] but no tires.
			// doc 900 has spoiler in g[1].cars[0] and tires in g[1].cars[1] —
			// no single car has both, FAIL.
			wantDocs: []uint64{100, 200, 800},
		},
		// Pinned+pinned merge at owner. Both leaves naturally have
		// Scope=garages after pin consumption, so andLeaves works
		// without lift. Same-garage semantic: a single garage must satisfy
		// both NOT cars[1].year=2020 (cars[1] missing or year ≠ 2020) AND
		// cars[2].name=honda. doc 600 deliberately FAILs because its two
		// countries each satisfy one half but no garage satisfies both —
		// this test pins the no-cross-country-mixing property.
		{
			name: "NOT cars[1].year=2020 AND cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPinnedPositive(idx,
						"countries.garages.cars.name", "honda",
						[]pinSpec{{"countries.garages.cars", 2}}))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// doc 100: g[0] satisfies both. doc 500: g[1] satisfies both.
			// doc 600: country 0 g[0] satisfies NOT but cars[2].name=ford;
			// country 1 g[0] cars[1].year=2020 fails NOT. No same-garage
			// witness anywhere.
			wantDocs: []uint64{100, 500},
		},
		// Mixed CanProject AND: pinned negation (TS=garages, CP=false)
		// ANDed with unpinned positive (TS=cars, CP=true). Ancestor.CP=false
		// + child.CP=true → direct merge at ancestor.TS=garages (child's
		// chain bits at garages are already clean since child.CP=true).
		// Result CP=false (ancestor's ghosts above garages remain).
		{
			name: "NOT cars[1].year=2020 AND cars.make=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPositive(idx, "countries.garages.cars.make", "honda"))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// doc 100: g[0] cars[1]=2018 satisfies NOT, cars[1].make=honda.
			// doc 300: g[0] cars[1] missing year satisfies NOT (missing
			// pinned slot is a witness), cars[1].make=honda.
			// 200/810/820: cars[1].year=2020 fails NOT. 400/500/600/700/830: // no make=honda or no satisfying garage.
			wantDocs: []uint64{100, 300},
		},
		// Ghost mitigation via ancestor constraint. The inner sibling-AND
		// at cars has CP=false because chain bits at garage level can
		// survive even when no single car satisfies both predicates (a
		// "ghost ancestor"). AND-ing with garages.city=warsaw — a positive
		// ancestor at garages, CP=true — masks ghosts that don't overlap
		// with warsaw garages, but cannot mask ghosts that coincide with
		// warsaw's authentic positions. Result CP=child.CP=false: ES at
		// cars is correct, but parent projection stays unsafe in general.
		//
		// For these specific fixtures the merge happens to produce a clean
		// result at parent — doc 800 g[1] (krakow) ghost is filtered out
		// because krakow ≠ warsaw; doc 900 has no inner witness anywhere.
		// But that's data-dependent; the CP flag stays false because a doc
		// with a warsaw garage carrying its own split-spoiler/tires ghost
		// would survive the filter and leak into a parent projection.
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) AND garages.city=warsaw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
						leafPositive(idx, "countries.garages.cars.tires.width", 205)),
					leafPositive(idx, "countries.garages.city", "warsaw"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			wantDocs:       []uint64{800},
		},
		// Ancestor+child AND with both CP=true. Ancestor garages.city=warsaw
		// (TS=garages) AND child cars.doors.count=4 (TS=doors). Merge at
		// child.TS=doors with CP=child.CP=true. Ancestor.Bitmap broadcasts to
		// descendants via the chain encoding, so doors-scope intersection
		// keeps only doors whose garage chain includes a warsaw garage.
		{
			name: "garages.city=warsaw AND cars.doors.count=4",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope: "countries.garages.cars.doors",
			// CleanAbove = ancestor.TS (garages). The dispatch matrix's
			// broadcasting-and-clean-down-to-A.Scope branch fires here: // per-element correlation holds at scopes [cars.doors, cars,
			// garages]; strictly above garages (countries) the chain bits
			// are shared and ghosts can survive.
			wantCleanAbove: "countries.garages",
			// 800 g[0] (warsaw): cars[0].doors=4 → MATCH. 800 g[1] (krakow): // doors=4 but chain is krakow → no warsaw overlap.
			// 900 g[0] (warsaw): cars[0].doors=4 → MATCH. 900 g[1] (krakow): // doors=4 but krakow.
			// No other fixture sets city or doors.
			wantDocs: []uint64{800, 900},
		},
		// Ancestor+child AND with both CP=false. Ancestor pinned NOT
		// (TS=garages) AND child same-scope AND at cars (TS=cars). Both
		// inputs carry ghosts, so child gets lifted to ancestor.TS=garages
		// first (LiftToAncestor rebuilds child.Bitmap at garages from
		// authentic Witnesses, wiping ghost bits), then same-scope AND
		// at garages with CP=false.
		{
			name: "NOT cars[1].year=2020 AND (cars.colors=red AND cars.colors=blue)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.colors", "red"),
						leafPositive(idx, "countries.garages.cars.colors", "blue")))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// Inner AND matches only doc 100 cars[1] — the only car with
			// both red and blue colors in any fixture. Outer NOT witness
			// covers doc 100 g[0] (cars[1].year=2018 ≠ 2020). Lifted child
			// at garages = {doc 100 g[0]}; ancestor ES at garages includes
			// that garage. Intersection → doc 100.
			wantDocs: []uint64{100},
		},
		// Contrast to the ancestor-constrained ghost-mitigation case above: // adding a same-scope sibling at cars (doors.count=4) does NOT clean
		// the ghost at the parent garage. The inner sibling-AND
		// (spoiler AND 205) carries chain bits at BOTH the warsaw garage
		// (real witness via cars[0]) and the krakow garage (ghost from
		// cars[0]=spoiler / cars[1]=205 split). doors.count=4 fires in cars
		// under BOTH garages of doc 800, so its chain bits overlap with the
		// ghost at krakow — the same-scope intersection preserves both
		// garages in Bitmap. Witnesses at cars is still clean (only the
		// warsaw car satisfies all three at car-self level), so wantDocs is
		// {800}. The point of the test is the structural property that
		// CanProject stays false after another same-scope AND: same-scope
		// siblings cannot recover from a ghost-bearing input, while an
		// ancestor constraint at a different scope sometimes can (data
		// permitting).
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) AND cars.doors.count=4",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
						leafPositive(idx, "countries.garages.cars.tires.width", 205)),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// doc 800: warsaw cars[0] has spoiler+205+doors=4 → MATCH.
			// krakow cars split spoiler/205 across cars, both have doors=4
			// → no single car satisfies all three at car-self → ghost only.
			// doc 900: warsaw cars[0] has only doors → no inner AND witness.
			// krakow cars split spoiler/205 like in 800 → ghost only.
			// Net witness across all garages: only doc 800 warsaw car.
			wantDocs: []uint64{800},
		},
		// Sibling OR with both operands CP=true. accessories and tires are
		// sibling sub-objects under cars; their LCA is cars. Both lifts
		// take the cheap anchor-intersect path because each operand's Bitmap
		// is already authentic at the LCA (chain bits propagate up via the
		// chain+self+descendantSelves encoding). Result CP=true: union
		// preserves cleanness when both inputs are clean.
		{
			name: "cars.accessories.type=spoiler OR cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope: "countries.garages.cars",
			// CleanAbove = propName. Both operands are positive leaves
			// (CS=countries). Lifting from accessories/tires to LCA=cars is
			// cheap (target=cars is at-or-below CS=countries), so Bitmap is
			// reused unchanged and the cleanness claim above LCA is
			// preserved through the lift. OR of two clean-at-countries
			// operands stays clean at countries.
			wantCleanAbove: "countries",
			// spoiler-accessory contributors: doc 100 cars[0], doc 200
			// cars[0], doc 800 g[0] cars[0] + g[1] cars[0], doc 830 cars[0]
			// (accessories[1]), doc 900 g[1] cars[0] — docs {100, 200, 800,
			// 830, 900}.
			// 205-tire contributors: doc 100 cars[0], doc 200 cars[0], doc
			// 800 g[0] cars[0] + g[1] cars[1], doc 900 g[1] cars[1] — docs
			// {100, 200, 800, 900}.
			// Union: {100, 200, 800, 830, 900}. doc 830 only via
			// accessories (no tires); all others appear in both.
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		// Ancestor+child OR with both operands CP=true. Pins the
		// "container exists" semantic: docs 800 and 900 have warsaw
		// garages but no ford cars, and must still pass via the warsaw
		// branch alone. A bug that merged at C.Scope=cars (instead of LCA
		// = A.Scope = garages) would drop them, because A's value bits at
		// cars-self exist only for cars under warsaw garages — empty for
		// warsaw garages with cars that don't satisfy B. The correct
		// merge at A.Scope keeps "garages where A or B fires" without
		// requiring B to fire under A.
		//
		// A.Scope=garages is already the LCA, so A's lift is a no-op.
		// B (TS=cars, CP=true) gets the cheap anchor-intersect lift.
		// Result CP=true: both inputs clean above their TS, union clean
		// above garages.
		{
			name: "garages.city=warsaw OR cars.make=ford",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.make", "ford"))
			},
			wantScope: "countries.garages",
			// CleanAbove = propName. warsaw is already at LCA=garages, so
			// its lift is the early-return no-op (Bitmap and CS untouched).
			// ford takes the cheap lift to garages: target=garages is
			// at-or-below CS=countries, Bitmap is reused and CS preserved.
			// The OR ends up clean at countries.
			wantCleanAbove: "countries",
			// warsaw garages: doc 800 g[0], doc 900 g[0] — docs {800, 900}.
			// ford-cars (make=ford): doc 300, 400, 500 (g[0] cars[1]),
			// 600 (country 0 g[0] cars[0]), 700 (g[1] cars[0]), 820
			// (country 0 g[0] cars[0]) — docs {300, 400, 500, 600, 700,
			// 820}. doc 200 has name=ford (not make=ford) → not a match.
			// Union: docs where some garage has city=warsaw OR some car
			// in any garage has make=ford. Docs 800 and 900 pass via
			// warsaw garage despite having no ford cars at all.
			wantDocs: []uint64{300, 400, 500, 600, 700, 800, 820, 900},
		},
		// Ancestor+child OR with C.CP=false. A=warsaw (TS=garages,
		// CP=true) is already at the LCA. C=inner AND of colors (TS=cars,
		// CP=false) carries chain ghosts at garages — different cars in
		// the same garage satisfying each colors leaf could leave a garage
		// chain bit even with no single car matching both. The lift uses
		// LiftToAncestor on C.ES to rebuild lifted_C.Bitmap at garages from
		// authentic Witnesses, wiping those ghost bits before the
		// union. Result CP=false because A.CP && C.CP = false.
		{
			name: "garages.city=warsaw OR (cars.colors=red AND cars.colors=blue)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.colors", "red"),
						leafPositive(idx, "countries.garages.cars.colors", "blue")))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// Inner AND matches only doc 100 cars[1] — the only car with
			// both red and blue colors in any fixture. Lifted to garages
			// = doc 100 g[0]. warsaw garages = doc 800 g[0], doc 900 g[0].
			// Union → {100, 800, 900}.
			wantDocs: []uint64{100, 800, 900},
		},
		// Sibling OR with asymmetric CP. A=NOT spoiler (TS=accessories,
		// CP=false) requires the real LiftToAncestor + Bitmap reconstruction
		// path because A.Bitmap at cars carries chain bits for every car
		// that owns any accessory — including cars whose accessories are
		// all spoiler (no non-spoiler witness). LiftToAncestor on A.ES
		// gives only cars with at least one non-spoiler accessory.
		// B=205-tires (TS=tires, CP=true) takes the cheap anchor-intersect
		// lift; B.Bitmap at cars is already authentic. Result CP=false
		// because A.CP && B.CP = false.
		//
		// Note: wantDocs at the doc level coincides with the simpler
		// spoiler-OR-205 case above because MaskRootLeaf collapses to
		// docID — the lift's per-car correction (excluding all-spoiler
		// cars from A's contribution at cars-self) doesn't surface as a
		// different doc set. The harness still verifies the trustworthy-
		// scope invariant (Bitmap ∩ _anchor(cars) == ES), which would fail
		// if A were merged without lifting.
		{
			name: "NOT cars.accessories.type=spoiler OR cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafNot(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// NOT spoiler witness cars (per-car non-spoiler accessory): // doc 100 cars[1] (radio), doc 830 cars[0] (radio at idx 0).
			// 205-tire cars: doc 100 cars[0], doc 200 cars[0], doc 800
			// warsaw cars[0], doc 800 krakow cars[1], doc 900 krakow
			// cars[1].
			// Union at cars-self covers docs {100, 200, 800, 830, 900}.
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		// Same-TS OR with mixed CP: inner AND (TS=cars, CP=false) ORed
		// with a leaf at cars (CP=true). Both operands at the same TS, so
		// no lifts; result CP = false && true = false. Verifies the
		// CP-propagation rule for same-TS OR with mixed input — the
		// existing same-TS OR test only covers (true, true).
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) OR cars.make=ford",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
						leafPositive(idx, "countries.garages.cars.tires.width", 205)),
					leafPositive(idx, "countries.garages.cars.make", "ford"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Inner AND (spoiler+205 same car): docs {100, 200, 800}.
			// ford-cars: docs {300, 400, 500, 600, 700, 820}.
			// Union: {100, 200, 300, 400, 500, 600, 700, 800, 820}.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 820},
		},
		// AND consuming an OR result. Inner OR at cars (CP=true, since
		// both inputs are clean positives at the same TS); outer leaf at
		// cars (CP=true). Same-TS AND, result CP=false structurally.
		// Verifies that andLeaves correctly dispatches on an OR-produced
		// input — the OR's clean output gets ANDed without surprises.
		{
			name: "(cars.make=honda OR cars.make=ford) AND cars.year=2018",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					orLeaves(idx,
						leafPositive(idx, "countries.garages.cars.make", "honda"),
						leafPositive(idx, "countries.garages.cars.make", "ford")),
					leafPositive(idx, "countries.garages.cars.year", 2018))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Per-car: (make=honda OR make=ford) AND year=2018 in the
			// same car.
			// doc 100: cars[1] make=honda year=2018 ✓ → PASS.
			// doc 820 country 0: cars[0] make=ford year=2018 ✓ → PASS.
			// All other docs: no single car matches both predicates
			// (e.g. doc 500 cars[1] is ford/2019, doc 600 country 0
			// cars[0] is ford/2010, doc 300 cars[0] is ford/2011 etc).
			wantDocs: []uint64{100, 820},
		},
		// Ancestor+child OR with both operands CP=false. A=pinned NOT
		// (TS=garages, CP=false) is already at the LCA. C=inner same-TS
		// AND at cars (TS=cars, CP=false) carries chain ghosts at
		// garages — different cars in the same garage matching each leaf
		// would leave a garage bit even when no single car matches both.
		// C goes through LiftToAncestor on its Witnesses, rebuilding
		// lifted_C.Bitmap at garages from authentic ES (wiping the ghost
		// bits before the union). Result CP = false && false = false.
		// Verifies the (A.CP=false, C.CP=false) cell of ancestor+child
		// OR, which the prior tests didn't cover.
		{
			name: "NOT cars[1].year=2020 OR (cars.make=honda AND cars.year=2020)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					andLeaves(idx,
						leafPositive(idx, "countries.garages.cars.make", "honda"),
						leafPositive(idx, "countries.garages.cars.year", 2020)))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// A (NOT cars[1].year=2020 at garages): garages where cars[1]
			// missing or year≠2020 → docs {100, 300, 400, 500, 600, 700,
			// 800, 830, 900}.
			// Inner AND (same car has make=honda AND year=2020): doc 200
			// cars[1], doc 810 cars[0], doc 820 country 0 cars[1] → docs
			// {200, 810, 820}. Lifted to garages: doc 200 g[0], doc 810
			// g[0], doc 820 country 0 g[0].
			// Union covers every fixture: {100, 200, 300, 400, 500, 600,
			// 700, 800, 810, 820, 830, 900}. C meaningfully contributes
			// 200/810/820 — none are in A.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Owner-level IS NULL true combined with a positive child via OR.
		// A=cars IS NULL true at garages (CP=false; the IS-NULL-true
		// shape carries chain ghosts at parent garages by construction).
		// C=make=honda at cars (CP=true). Ancestor+child OR, C.CP=true →
		// cheap anchor-intersect lift for C; A is already at LCA. Result
		// CP = false && true = false. Verifies that owner-level IS NULL
		// composes correctly as an operand — no prior merge test uses it.
		{
			name: "cars IS NULL true OR cars.make=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafIsNullTrue(idx, "countries.garages.cars"),
					leafPositive(idx, "countries.garages.cars.make", "honda"))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// cars IS NULL true at garages: only doc 700 g[0] (the empty
			// cars garage). All other garages have at least one car so
			// IS NULL true fails.
			// make=honda at cars: docs with a car whose make field is
			// "honda" — {100, 200, 300, 810, 820}. Docs 500/600 only
			// have name=honda (not make), so they don't qualify.
			// Union → {100, 200, 300, 700, 810, 820}.
			wantDocs: []uint64{100, 200, 300, 700, 810, 820},
		},
		// NOT applied to a compound (AND) result. The algorithm exposes
		// negate as a generic universe-subtract at the compound's
		// Scope: NOT-ES = _anchor(TS) ANDNOT compound.ES. That gives
		// per-element-existential semantics at the compound's TS — "some
		// element at TS does NOT satisfy the compound." A doc passes if
		// any TS-element falls outside the compound's ES.
		//
		// Note: NOT-of-compound is a deferred design area (per-element vs
		// owner-level semantics differ for non-trivial doc shapes). This
		// test pins the algorithmic behavior of negate(), not a chosen
		// user-facing semantic.
		{
			name: "NOT (NOT cars[1].year=2020 AND cars[2].name=honda)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPinnedPositive(idx,
						"countries.garages.cars.name", "honda",
						[]pinSpec{{"countries.garages.cars", 2}})))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// Inner AND ES at garages = {doc 100 g[0], doc 500 g[1]}.
			// negate at garages = _anchor(garages) ANDNOT inner.ES = every
			// garage except those two.
			// doc 100: only g[0], which IS in inner → NOT has zero garages
			// → doc 100 fails.
			// doc 500: g[0] not in inner, g[1] in inner → NOT has g[0] →
			// doc 500 passes.
			// Every other doc: no garages in inner → all garages survive
			// the subtract → docs pass.
			wantDocs: []uint64{200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// andN orchestration: sorts the 3 operands by TS depth (deepest
		// first, CP=true breaking ties) before sequential folding via
		// andLeaves. cars.tires.width=205 (TS=cars.tires, depth 4) lands
		// first; cars.make=honda and cars.year=2020 (TS=cars, depth 3,
		// both CP=true) fold after. The dispatch keeps the running
		// result at the deepest TS (cars.tires) for both subsequent
		// merges because each shallower ancestor is CP=true (Row 1 of
		// the AND dispatch: merge at child.TS, CP=child.CP).
		//
		// A naive left-to-right nested call (((make AND year) AND
		// tires)) lands at cars / CP=false because the first same-TS
		// AND already collapses to CP=false. andN gets the same wantDocs
		// but at a strictly more informative Scope.
		{
			name: "andN(cars.make=honda, cars.year=2020, cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope: "countries.garages.cars.tires",
			// CleanAbove = cars (= ancestor.TS for the ancestor+child step).
			// Step 1 (tires AND honda): ancestor=honda(cars), child=tires.
			// Ceiling = honda.TS = cars. result.CS = cars. Step 2 (... AND
			// year): year.TS=cars is ancestor; ceiling still cars. Final
			// CleanAbove = cars. Strictly above cars (garages, countries)
			// the chain bits are shared and ghosts can survive — this is
			// the bug the old CP=true bool hid.
			wantCleanAbove: "countries.garages.cars",
			// Only doc 200 cars[1] satisfies all three (make=honda,
			// year=2020, tires.width=205) in the same car. Other
			// candidates fail one of the conjuncts: // - doc 100 cars[0]: toyota/2020/205 → fails honda.
			// - doc 100 cars[1]: honda/2018/195 → fails year and tires.
			// - doc 200 cars[0]: honda/2015/205 → fails year.
			// - doc 810 cars[0]: honda/2020/no-tires → fails tires.
			// - doc 820 cars[1]: honda/2020/no-tires → fails tires.
			wantDocs: []uint64{200},
		},
		// andN equivalent of the ghost-mitigation case above. Inputs: // accessories (depth 4, CP=true), tires (depth 4, CP=true),
		// garages.city (depth 2, CP=true). Sort places accessories and
		// tires first (siblings under cars), then warsaw. Fold: //   - accessories AND tires → sibling lift to LCA=cars, same-TS,
		//     CP=false, TS=cars.
		//   - result AND warsaw → ancestor=warsaw(garages, CP=true),
		//     child=result(cars, CP=false). Row 1: merge at cars,
		//     CP=child.CP=false.
		// Same wantDocs / TS / CP as the pairwise version.
		{
			name: "andN(spoiler, tires.width=205, garages.city=warsaw)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.city", "warsaw"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// doc 800 warsaw cars[0] has spoiler+205 under a warsaw
			// garage. krakow split spoiler/205 ghost gets filtered by
			// the warsaw constraint at garages.
			wantDocs: []uint64{800},
		},
		// andN over three same-depth siblings (accessories, tires,
		// doors — all sub-objects of cars, depth 4, CP=true). Sort
		// leaves the input order intact (all same depth+CP). Fold: //   - accessories AND tires → sibling at LCA=cars, CP=false.
		//   - result(cars, CP=false) AND doors(cars.doors, CP=true) →
		//     Row 2 (A.CP=false, C.CP=true): merge at A.Scope=cars,
		//     CP=false.
		// Same result as the pairwise contrast test above. Useful as
		// a regression check that the N-ary dispatch handles a uniform
		// sibling triple without surprises.
		{
			name: "andN(spoiler, tires.width=205, doors.count=4)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Only doc 800 warsaw cars[0] has all three in a single car.
			wantDocs: []uint64{800},
		},
		// andN over four leaves spanning two TS levels (two siblings at
		// depth 4, two same-TS leaves at depth 3). Inputs: //   - cars.accessories.type=spoiler (TS=accessories, depth 4)
		//   - cars.tires.width=205          (TS=tires,       depth 4)
		//   - cars.make=honda               (TS=cars,        depth 3)
		//   - cars.year=2020                (TS=cars,        depth 3)
		// All CP=true. Sort yields depth-4 pair first, then the depth-3
		// pair. Fold: //   - accessories AND tires → sibling lift to cars, same-TS at
		//     cars, CP=false.
		//   - result(cars, CP=false) AND make(cars, CP=true) → same-TS,
		//     CP=false.
		//   - result(cars, CP=false) AND year(cars, CP=true) → same-TS,
		//     CP=false.
		// Per-car semantics: car with spoiler accessory AND 205 tire AND
		// make=honda AND year=2020 simultaneously.
		{
			name: "andN(spoiler, tires.width=205, make=honda, year=2020)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Only doc 200 cars[1] has all four (spoiler, 205, honda,
			// 2020) in the same car. Other candidates fail at least one: // - doc 100 cars[0]: spoiler+205+toyota+2020 → fails make.
			// - doc 200 cars[0]: spoiler+205+honda+2015 → fails year.
			// - doc 800 warsaw cars[0]: spoiler+205+no make → fails make.
			wantDocs: []uint64{200},
		},
		// andN with three leaves spanning two TS levels and mixed CP.
		// Inputs: //   - NOT cars[1].year=2020 (TS=garages, depth 2, CP=false)
		//   - cars.make=honda          (TS=cars,   depth 3, CP=true)
		//   - cars.year=2018           (TS=cars,   depth 3, CP=true)
		// Sort places the two depth-3 CP=true leaves first, then the
		// depth-2 CP=false one. Fold: //   - make AND year → same-TS at cars, CP=false.
		//   - result(cars, CP=false) AND NOT(garages, CP=false) → Row 3
		//     (both CP=false): lift result to garages, same-TS at
		//     garages, CP=false.
		// Per-garage semantics: garage where some car has make=honda
		// AND year=2018 simultaneously, AND cars[1] is not year=2020.
		{
			name: "andN(NOT cars[1].year=2020, cars.make=honda, cars.year=2018)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPinnedNot(idx,
						"countries.garages.cars.year", 2020,
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2018))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// doc 100 g[0]: cars[1] is honda/2018 — same car has both.
			// cars[1].year=2018 ≠ 2020 → NOT also satisfied → PASS.
			// All other docs fail at least one conjunct in same-car or
			// same-garage form.
			wantDocs: []uint64{100},
		},
		// orN over three same-TS positive leaves (all at cars, all
		// CP=true). Sort is a no-op (input order preserved). Fold: //   - make OR year → same-TS at cars, no lifts. CP=true.
		//   - result OR colors → same-TS at cars, no lifts. CP=true.
		// Verifies the orchestration on the simplest path (no scope
		// transitions, no lifts).
		{
			name: "orN(cars.make=honda, cars.year=2018, cars.colors=blue)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2018),
					leafPositive(idx, "countries.garages.cars.colors", "blue"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			// make=honda: {100, 200, 300, 810, 820}.
			// year=2018: {100, 500, 600, 820} (cars with year=2018).
			// colors=blue: {100, 200} (cars with blue in their colors).
			// Union: {100, 200, 300, 500, 600, 810, 820}.
			wantDocs: []uint64{100, 200, 300, 500, 600, 810, 820},
		},
		// orN over three sibling sub-object leaves under cars (all CP=
		// true, all depth 4). Sort is a no-op. Fold: //   - spoiler OR 205 → sibling lift to LCA=cars (both cheap
		//     since CP=true), same-TS at cars. CP=true.
		//   - result(cars, CP=true) OR doors(cars.doors, CP=true) →
		//     LCA=cars. result no-op (already at LCA). doors cheap
		//     lift to cars. Union. CP=true.
		// Verifies the matrix's "OR always lands at LCA" rule across a
		// 3-way sibling union with full CP preservation.
		{
			name: "orN(spoiler, tires.width=205, doors.count=4)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantScope: "countries.garages.cars",
			// CleanAbove = propName. Three sibling positive leaves under
			// cars; each lifts to LCA=cars via the cheap path (Bitmap and CS
			// preserved). OR result keeps the chain-up cleanness all the
			// way to the property root.
			wantCleanAbove: "countries",
			// spoiler: {100, 200, 800, 830, 900}.
			// 205-tires: {100, 200, 800, 900}.
			// doors=4: {800, 900}.
			// Union: {100, 200, 800, 830, 900}. doc 830 only via spoiler.
			wantDocs: []uint64{100, 200, 800, 830, 900},
		},
		// orN spanning three TS levels (garages, cars, cars.tires), all
		// CP=true. Sort places tires (depth 4), then cars-level honda
		// (depth 3), then warsaw (depth 2). Fold: //   - tires OR honda → LCA=cars. tires cheap lift to cars
		//     (CP=true). honda no-op. CP=true.
		//   - result(cars, CP=true) OR warsaw(garages, CP=true) →
		//     LCA=garages. result cheap lift to garages (CP=true).
		//     warsaw no-op. Union at garages. CP=true.
		// Verifies the container-exists semantic in a 3-way OR: doc 800
		// passes via warsaw alone, doc 810/820 via honda alone, etc.
		{
			name: "orN(garages.city=warsaw, cars.make=honda, cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope: "countries.garages",
			// CleanAbove = propName. All three are positive leaves
			// (CS=countries); each lift to LCA=garages is cheap and
			// preserves the operand's CleanAbove.
			wantCleanAbove: "countries",
			// warsaw garages: {800, 900}.
			// make=honda: {100, 200, 300, 810, 820}.
			// 205-tires: {100, 200, 800, 900}.
			// Union: {100, 200, 300, 800, 810, 820, 900}.
			wantDocs: []uint64{100, 200, 300, 800, 810, 820, 900},
		},
		// NOT applied to an AND of ancestor + descendant. Inner AND: // warsaw(TS=garages, CP=true) AND doors=4(TS=cars.doors,
		// CP=true). Row 1 dispatch merges at child.TS=cars.doors with
		// CP=true; inner.ES = doors under warsaw garages with count=4.
		// negate at cars.doors: doors NOT in inner.ES. The result is
		// at the compound's TS (cars.doors), per-element-existential.
		{
			name: "NOT (garages.city=warsaw AND cars.doors.count=4)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.doors.count", 4)))
			},
			wantScope:      "countries.garages.cars.doors",
			wantCleanAbove: "countries.garages.cars.doors",
			// Inner ES at cars.doors: doors under warsaw garages with
			// count=4 = doc 800 warsaw cars[0].doors[0], doc 900 warsaw
			// cars[0].doors[0].
			// NOT at cars.doors: every other doors-self position.
			// MaskRootLeaf: docs with some door NOT in inner.
			// - docs 100-700, 810-830: no doors-self bits at all → no
			//   witness possible → fail.
			// - doc 800: krakow cars[0/1].doors[0]=4 NOT in inner →
			//   witnesses → PASS.
			// - doc 900: krakow cars[0/1].doors[0]=4 NOT in inner →
			//   witnesses → PASS.
			wantDocs: []uint64{800, 900},
		},
		// NOT applied to an AND of siblings under cars. Inner AND: // spoiler(TS=accessories) AND 205(TS=tires) → sibling lift to
		// LCA=cars, same-TS AND at cars, CP=false. inner.ES = cars
		// with both spoiler accessory and 205 tire in the same car.
		// negate at cars: cars NOT in inner.ES.
		//
		// Result is trivially true at the doc level for these fixtures
		// because every doc has at least one car that fails one of the
		// conjuncts. The test still pins the algorithm's behavior: // negate at the sibling-AND TS (cars) and per-element-
		// existential semantics.
		{
			name: "NOT (cars.accessories.type=spoiler AND cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205)))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Inner ES at cars: cars with spoiler+205 in the same car =
			// doc 100 cars[0], doc 200 cars[0], doc 200 cars[1], doc 800
			// warsaw cars[0]. Every doc has at least one other car that
			// is not in inner.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// NOT applied to an OR of same-scope leaves. Inner OR: // make=honda OR make=ford at cars (both CP=true). Union ES at
		// cars = cars whose make is honda or ford. negate at cars: // cars without honda/ford make. A doc fails only when every
		// car has make in {honda, ford}.
		{
			name: "NOT (cars.make=honda OR cars.make=ford)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.make", "ford")))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Docs where every car has make in {honda, ford}: // - doc 300: cars[0]=ford, cars[1]=honda → all in → FAIL.
			// - doc 400: cars[0]=ford → all in → FAIL.
			// - doc 700: g[1] cars[0]=ford → all (one) in → FAIL.
			// Other docs have at least one car with a different make
			// (or no make at all → also not honda/ford).
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},
		// NOT applied to an OR of ancestor + descendant. Inner OR: // warsaw(TS=garages, CP=true) OR honda(TS=cars, CP=true) → OR
		// always lands at LCA=garages. honda gets cheap lift to garages.
		// inner.ES at garages = warsaw garages ∪ garages-with-honda-
		// cars. negate at garages: garages with neither.
		{
			name: "NOT (garages.city=warsaw OR cars.make=honda)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.make", "honda")))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// Inner ES at garages: warsaw garages {800 g[0], 900 g[0]}
			// ∪ honda-car garages {100 g[0], 200 g[0], 300 g[0],
			// 810 g[0], 820 country 0 g[0]}.
			// NOT at garages: garages with neither. Docs that pass: // - 400, 500, 600, 700: no warsaw, no honda → garages pass.
			// - 800: g[1] krakow has no honda → witness.
			// - 820: country 1 g[0] has no warsaw no honda → witness.
			// - 830: g[0] no warsaw, only car has no make → witness.
			// - 900: g[1] krakow no warsaw no honda → witness.
			// Docs that fail (every garage has warsaw or honda): // - 100, 200, 300, 810: only garage has honda.
			wantDocs: []uint64{400, 500, 600, 700, 800, 820, 830, 900},
		},
		// NOT applied to an OR of siblings under cars. Inner OR: // spoiler(TS=accessories) OR 205(TS=tires) → sibling lift to
		// LCA=cars. Each operand's CP=true so both lifts are cheap.
		// inner.ES at cars = cars with spoiler accessory or 205 tire.
		// negate at cars: cars without either.
		{
			name: "NOT (cars.accessories.type=spoiler OR cars.tires.width=205)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafPositive(idx, "countries.garages.cars.tires.width", 205)))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Docs that fail (every car has spoiler or 205): // - 800: warsaw cars[0] (spoiler+205), krakow cars[0]
			//   (spoiler), krakow cars[1] (205) — all in inner → FAIL.
			// - 830: only car has spoiler → FAIL.
			// All other docs have at least one car without either.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820, 900},
		},
		// ---------------------------------------------------------------
		// De Morgan transformations of the three NOT(OR) tests above.
		// These manually express the rewrite NOT(A OR B) → NOT(A) AND
		// NOT(B). For same-scope clauses De Morgan is an equivalence and
		// the result coincides with the NOT(OR) form. For cross-scope
		// clauses (ancestor-child or siblings) the transformation diverges
		// — each NOT keeps its natural TS, then the AND correlates
		// per-element at the deeper scope. This: //   - Includes docs where a non-warsaw garage contains a non-honda
		//     car alongside a honda car (lost by the NOT(OR) form because
		//     the garage gets flagged by the inner OR).
		//   - Excludes docs whose relevant scopes are empty (e.g. no
		//     accessories or no tires at all) since per-element AND
		//     requires an actual witness, not vacuous truth.
		// These tests pin the transformed semantics; they are not used by
		// the implementation today but document the expected behaviour
		// should a planner choose to apply the rewrite.

		// Same-scope De Morgan: should match its NOT(OR) counterpart docs
		// exactly. Acts as a sanity check that the transformation is a
		// no-op for same-TS clauses.
		{
			name: "NOT(cars.make=honda) AND NOT(cars.make=ford) [De Morgan of NOT(honda OR ford)]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "countries.garages.cars.make", "honda"),
					leafNot(idx, "countries.garages.cars.make", "ford"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Same wantDocs as NOT(honda OR ford) — De Morgan holds at
			// same scope. Docs that fail: 300 (cars[0]=ford, cars[1]=
			// honda → every car is honda or ford), 400 (only car is
			// ford), 700 (only car is ford).
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},
		// Cross-scope De Morgan: ancestor-child relation (garages vs cars).
		// Diverges from NOT(warsaw OR honda) which excludes 100/200/300/
		// 810 because those garages contain a honda car. The transformed
		// form keeps per-element NOT(honda) at cars, then ANDs with the
		// per-element NOT(warsaw) at garages — every doc has at least one
		// non-warsaw garage with a non-honda car (or a car without make),
		// so all 12 docs pass.
		{
			name: "NOT(garages.city=warsaw) AND NOT(cars.make=honda) [De Morgan of NOT(warsaw OR honda)]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "countries.garages.city", "warsaw"),
					leafNot(idx, "countries.garages.cars.make", "honda"))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// Per-garage: garage is not warsaw AND contains at least one
			// non-honda car descendant.
			// - 100 g[0]: no city set, cars[0]=toyota → passes.
			// - 200 g[0]: no city, cars[2]=name=ford (no make) → passes.
			// - 300 g[0]: cars[0]=ford → passes.
			// - 400-700: no warsaw, non-honda cars present → all pass.
			// - 800 g[1]=krakow, cars have no make field → passes.
			// - 810: no city, cars[1]=toyota → passes.
			// - 820: no city, cars[0]=ford → passes.
			// - 830: no city, cars[0] has no make → passes.
			// - 900 g[1]=krakow, cars have no make → passes.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		// Sibling De Morgan: accessories vs tires under cars. Diverges
		// from NOT(spoiler OR 205) which passes 10 docs (any car without
		// spoiler-or-205 attributes satisfies it vacuously). The
		// transformed form requires an actual non-spoiler accessory AND
		// a non-205 tire under the same car — empty-scope docs (no
		// accessories or no tires) fail.
		{
			name: "NOT(spoiler) AND NOT(205) [De Morgan of NOT(spoiler OR 205)]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "countries.garages.cars.accessories.type", "spoiler"),
					leafNot(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Per-car: car has at least one non-spoiler accessory AND at
			// least one non-205 tire.
			// - doc 100 cars[1]: radio (non-spoiler) + 195 (non-205) ✓.
			// - doc 200: cars[0] and cars[1] both spoiler+205; cars[2]
			//   has no accessories/tires → no witnesses anywhere → FAIL.
			// - doc 830 cars[0]: radio but no tires → no 205 witness → FAIL.
			// - All other docs lack either accessories or tires entirely
			//   → no per-element witness on at least one side.
			wantDocs: []uint64{100},
		},

		// ContainsAny over a scalar field at TS=cars. Equivalent to
		// orN of make=honda and make=ford with both at the same TS.
		// Same-TS OR: pure union with no lifts, CP=true preserved.
		{
			name: "cars.make ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "countries.garages.cars.make", "honda", "ford")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			// honda: {100, 200, 300, 810, 820}.
			// ford: {300, 400, 500, 600, 700, 820}.
			// Union: {100, 200, 300, 400, 500, 600, 700, 810, 820}.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820},
		},
		// ContainsAny with a rarer value set. Verifies orN over makes
		// not covered by the main fixtures (bmw, kia).
		{
			name: "cars.make ContainsAny [bmw, kia]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "countries.garages.cars.make", "bmw", "kia")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries",
			// bmw: doc 500 g[1] cars[0], doc 600 country 1 g[0] cars[0].
			// kia: doc 820 country 1 g[0] cars[0].
			// Union: {500, 600, 820}.
			wantDocs: []uint64{500, 600, 820},
		},
		// ContainsAll over a scalar-array field at TS=cars. Equivalent
		// to andN of colors=red and colors=blue, both at TS=cars. Same-
		// TS AND enforces per-car co-occurrence: only cars whose colors
		// array contains BOTH values survive.
		{
			name: "cars.colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAll(idx, "countries.garages.cars.colors", "red", "blue")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Only doc 100 cars[1].colors = ["blue", "red"] satisfies
			// both. Other docs either lack one of the values or have
			// them split across different cars (e.g. doc 200 cars[0]
			// has red, cars[1] has blue).
			wantDocs: []uint64{100},
		},
		// Pinned ContainsAny on cars[1].make. Narrows the value-union
		// (honda ∪ ford) with the cars[1] pin idx before lifting to
		// TS=garages.
		{
			name: "cars[1].make ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages.cars", 1}},
					"honda", "ford")
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// cars[1].make=honda: doc 100, 200, 300, 820 country 0.
			// cars[1].make=ford: doc 500 g[0].
			// Union: {100, 200, 300, 500, 820}.
			wantDocs: []uint64{100, 200, 300, 500, 820},
		},
		// Pinned ContainsAll on cars[1].colors. The value-intersection
		// (red ∩ blue) collapses to cars where the same cars[1].colors
		// array contains both, then the cars[1] pin narrows to garages
		// whose cars[1] qualifies.
		{
			name: "cars[1].colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx,
					"countries.garages.cars.colors",
					[]pinSpec{{"countries.garages.cars", 1}},
					"red", "blue")
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// Only doc 100 cars[1].colors = ["blue", "red"] satisfies
			// both at cars[1]. doc 200 cars[1] = [blue] (no red); doc
			// 300 cars[1] = [red] (no blue).
			wantDocs: []uint64{100},
		},
		// Multi-pin ContainsAll: garages[0].cars[1].colors ContainsAll
		// [red, blue]. Two pins narrow the value-intersection at every
		// level of the chain (garages[0] AND cars[1]). TS = parent of
		// outermost pin = countries.
		{
			name: "garages[0].cars[1].colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx,
					"countries.garages.cars.colors",
					[]pinSpec{
						{"countries.garages", 0},
						{"countries.garages.cars", 1},
					},
					"red", "blue")
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Only doc 100 g[0].cars[1].colors = ["blue", "red"]
			// satisfies both at the pinned slot. doc 200 g[0].cars[1]
			// has only blue; doc 300 g[0].cars[1] has only red.
			wantDocs: []uint64{100},
		},
		// Intermediate-pin ContainsAny: garages[1].cars.make ContainsAny
		// [honda, ford]. Pinned at garages level only; cars unpinned, so
		// any car under garages[1] can satisfy. TS = countries.
		{
			name: "garages[1].cars.make ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford")
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Need a doc with garages[1] whose cars include make=honda
			// or make=ford. Only doc 700 g[1].cars[0]=ford qualifies.
			// doc 500 g[1] cars have only "name" fields (no make).
			// doc 800/900 g[1] cars have no make at all.
			// Other docs have no g[1].
			wantDocs: []uint64{700},
		},
		// Multi-pin ContainsAny: garages[0].cars[2].name ContainsAny
		// [honda, ford]. Two pins narrow the value-union at every
		// chain level. TS = countries.
		{
			name: "garages[0].cars[2].name ContainsAny [honda, ford]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx,
					"countries.garages.cars.name",
					[]pinSpec{
						{"countries.garages", 0},
						{"countries.garages.cars", 2},
					},
					"honda", "ford")
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// doc 100 g[0].cars[2].name=honda ✓.
			// doc 200 g[0].cars[2].name=ford ✓.
			// doc 600 country 0 g[0].cars[2].name=ford ✓; country 1
			//   g[0].cars[2].name=honda ✓.
			// doc 500 g[0] has only 2 cars → no cars[2].
			// Other docs either lack cars[2] in g[0] or have no name
			// field there.
			wantDocs: []uint64{100, 200, 600},
		},
		// Intermediate-pin ContainsAll: garages[0].cars.colors ContainsAll
		// [red, blue]. Pinned at garages only; cars unpinned. Value-
		// intersection at cars (per-car co-occurrence) narrowed by
		// garages[0]; the same car under g[0] must contain both colors.
		// TS = countries.
		{
			name: "garages[0].cars.colors ContainsAll [red, blue]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx,
					"countries.garages.cars.colors",
					[]pinSpec{{"countries.garages", 0}},
					"red", "blue")
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// doc 100 g[0].cars[1].colors=[blue, red] satisfies both
			// at a single car. doc 200 has red and blue split across
			// cars[0] and cars[1] → no per-car co-occurrence. doc 300
			// has only red. No other doc has both colors anywhere.
			wantDocs: []uint64{100},
		},
		// ---------------------------------------------------------------
		// ContainsNone equivalence trios. Each trio runs the same query
		// in three syntactic forms — every form produces identical
		// wantDocs because ContainsNone is owner-level by design: //   (a) leafContainsNone / leafPinnedContainsNone — direct
		//       chained AndNot (pinned variant always owner-level,
		//       regardless of pin layout).
		//   (b) negate(leafContainsAny / leafPinnedContainsAny) —
		//       universe-subtract of positive ContainsAny.
		//   (c) negate(orLeaves(value-positive, value-positive)) — the
		//       canonical NOT-of-explicit-OR form.
		// Covers unpinned, fully-pinned single-pin, and intermediate-pin.
		// All three forms agree everywhere because the operator's
		// natural reading ("the array contains none of these values")
		// is universal over the array — the pinned variant doesn't
		// dispatch on hasPinGap. See
		// TestFastPathL2_IntermediatePinContainsNoneAgreement for a
		// discriminator fixture that confirms agreement on a mixed
		// matching/non-matching pattern.

		// --- Unpinned trio: cars.make ∉ {honda, ford} ------------------
		// Form (c) for the unpinned trio is already covered by the
		// existing "NOT (cars.make=honda OR cars.make=ford)" test
		// further up — same wantDocs proves the equivalence.
		{
			name: "cars.make ContainsNone [honda, ford] [direct]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsNone(idx, "countries.garages.cars.make", "honda", "ford")
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Per-car: a car whose make is not honda and not ford
			// (cars without a make field also satisfy). Excludes docs
			// where every car has make in {honda, ford}: // - doc 300: cars[0]=ford, cars[1]=honda → FAIL.
			// - doc 400: only car is ford → FAIL.
			// - doc 700: only car is ford → FAIL.
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (cars.make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafContainsAny(idx, "countries.garages.cars.make", "honda", "ford"))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// Same docs as the ContainsNone form — proves the direct
			// helper matches universe-subtract of ContainsAny.
			wantDocs: []uint64{100, 200, 500, 600, 800, 810, 820, 830, 900},
		},

		// --- Single-pin trio: cars[1].make ∉ {honda, ford} -------------
		// Fully-pinned (no gap) — leafPinnedContainsNone routes through
		// negate(ContainsAny). All three forms compute the owner-level
		// result, which coincides with per-element because cars[1] is
		// uniquely identified per garage.
		{
			name: "cars[1].make ContainsNone [honda, ford] [direct]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages.cars", 1}},
					"honda", "ford")
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			// cars[1].make=honda fires for docs 100/200/300 + 820 c0.
			// cars[1].make=ford fires for doc 500 g[0].
			// Per-garage: garage passes if its cars[1] is neither honda
			// nor ford (cars[1] missing or no make also counts).
			// - doc 100/200/300: only garage's cars[1] is honda → FAIL.
			// - doc 400: cars[1] missing → garage passes.
			// - doc 500: g[0] ford fails, g[1] cars[1]=name=honda (no
			//   make field) passes → doc passes.
			// - doc 820: country 0 honda fails, country 1 mazda passes.
			// - all other docs have a passing garage.
			wantDocs: []uint64{400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (cars[1].make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages.cars", 1}},
					"honda", "ford"))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			wantDocs:       []uint64{400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (cars[1].make=honda OR cars[1].make=ford) [via NOT-of-OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "countries.garages.cars.make", "honda",
						[]pinSpec{{"countries.garages.cars", 1}}),
					leafPinnedPositive(idx, "countries.garages.cars.make", "ford",
						[]pinSpec{{"countries.garages.cars", 1}})))
			},
			wantScope:      "countries.garages",
			wantCleanAbove: "countries.garages",
			wantDocs:       []uint64{400, 500, 600, 700, 800, 810, 820, 830, 900},
		},

		// --- Intermediate-pin trio: garages[1].cars.make ∉ {honda, ford}
		// Has a pin gap (cars unpinned between TS=countries and
		// ElementScope=cars). leafPinnedContainsNone uses the per-element
		// formula via perElementNotFromSubtractands; the negate forms
		// give owner-level. On these fixtures the three agree because
		// no doc has g[1] containing both matching and non-matching cars.
		{
			name: "garages[1].cars.make ContainsNone [honda, ford] [direct, per-element]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford")
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// Docs without g[1] pass via tsMissingPin (100-400, 600,
			// 810, 820, 830). Docs with g[1] cars whose makes are not
			// in {honda, ford} pass via per-element witnesses: 500
			// (bmw + name-only cars), 800/900 (no make under g[1]).
			// Doc 700 fails: g[1].cars[0]=ford is the only car, no
			// witness; pin present so no missing-pin contribution.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (garages[1].cars.make ContainsAny [honda, ford]) [via negate, owner-level]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford"))
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			// ContainsAny.ES = {country 0 of doc 700} (only country
			// with honda-or-ford under g[1]). negate at countries →
			// every country except that one. Doc 700 fails.
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
		{
			name: "NOT (garages[1].cars.make=honda OR garages[1].cars.make=ford) [via NOT-of-OR, owner-level]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "countries.garages.cars.make", "honda",
						[]pinSpec{{"countries.garages", 1}}),
					leafPinnedPositive(idx, "countries.garages.cars.make", "ford",
						[]pinSpec{{"countries.garages", 1}})))
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			wantDocs:       []uint64{100, 200, 300, 400, 500, 600, 800, 810, 820, 830, 900},
		},
		// Sibling AND with mixed CP: A=radio at accessories (CP=true) AND
		// B=NOT 205 at tires (CP=false). Both lifts land at LCA=cars: A
		// via cheap anchor-intersect (CP=true), B via real LiftToAncestor
		// on its Witnesses (CP=false). Per-car AND requires the same
		// car to have a radio accessory AND a non-205 tire.
		{
			name: "cars.accessories.type=radio AND NOT cars.tires.width=205",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.cars.accessories.type", "radio"),
					leafNot(idx, "countries.garages.cars.tires.width", 205))
			},
			wantScope:      "countries.garages.cars",
			wantCleanAbove: "countries.garages.cars",
			// radio cars: doc 100 cars[1] (accessories=[radio]) and
			// doc 830 cars[0] (accessories=[radio, spoiler]). doc 200's
			// cars all have spoiler, not radio.
			// non-205 tire cars (per-element witness): doc 100 cars[1]
			// (tire=195), doc 200 cars[1] (tire=195). Other tire-bearing
			// cars have only 205. Cars without tires have no per-element
			// witness (empty scope).
			// Intersection at cars-self: doc 100 cars[1]. doc 830 cars[0]
			// has radio but no tires → no non-205 witness.
			wantDocs: []uint64{100},
		},
	}

	runFastPathCases(t, idx, cases)
}

// TestFastPathL2_IntermediatePinContainsNoneAgreement uses a fixture
// pattern the shared L2 set doesn't contain — garages[1] holding both
// a matching car (honda) and a non-matching car (toyota) — to confirm
// that all three ContainsNone forms agree even on this discriminator: //
//   - leafPinnedContainsNone (always owner-level, no hasPinGap dispatch).
//   - negate(leafPinnedContainsAny).
//   - negate(orLeaves(positive, positive)).
//
// All three exclude doc 1000 because garages[1] contains a honda car,
// which is the natural reading of "ContainsNone [honda, ford]": the
// array must contain *none* of those values, and one honda is enough
// to fail the predicate.
//
// Historical note: an earlier version of leafPinnedContainsNone used
// per-element semantics via hasPinGap dispatch (matching the
// leafPinnedNot family), which produced wantDocs={1000} here — a
// non-matching car (toyota) would witness the predicate even when
// matching cars were present in the same garage. That contradicted
// the operator's English meaning and was reverted in favour of
// uniform owner-level semantics for ContainsNone.
func TestFastPathL2_IntermediatePinContainsNoneAgreement(t *testing.T) {
	prop := l2Schema()
	idx := newFastPathIndex("countries")
	// Discriminator doc: garages[1] contains a honda car AND a toyota
	// car. honda matches the ContainsNone subtractand set; toyota does
	// not. Per-element would let toyota witness the predicate; owner-
	// level rejects the country because the garage contains a honda car.
	// All three forms below use owner-level → all three reject.
	idx.addDoc(t, prop, 1000, []any{
		map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
			map[string]any{"cars": []any{
				map[string]any{"make": "honda"},
				map[string]any{"make": "toyota"},
			}},
		}},
	})

	cases := []fastPathTestCase{
		// Direct helper. After the owner-level change this goes through
		// negate(leafPinnedContainsAny) internally. honda is lifted to
		// country 0 via the positive ContainsAny; negate excludes it,
		// result empty.
		{
			name: "garages[1].cars.make ContainsNone [honda, ford] [direct, owner-level]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford")
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			wantDocs:       nil,
		},
		// Owner-level via explicit negate(ContainsAny).
		{
			name: "NOT (garages[1].cars.make ContainsAny [honda, ford]) [via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}},
					"honda", "ford"))
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			wantDocs:       nil,
		},
		// Owner-level via canonical NOT-of-OR. positive(honda) fires on
		// country 0, OR includes it, negate excludes it, result empty.
		{
			name: "NOT (garages[1].cars.make=honda OR garages[1].cars.make=ford) [via NOT-of-OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "countries.garages.cars.make", "honda",
						[]pinSpec{{"countries.garages", 1}}),
					leafPinnedPositive(idx, "countries.garages.cars.make", "ford",
						[]pinSpec{{"countries.garages", 1}})))
			},
			wantScope:      "countries",
			wantCleanAbove: "countries",
			wantDocs:       nil,
		},
	}

	runFastPathCases(t, idx, cases)
}

// TestFastPathL2_AncestorChildAND_CleanRange pins the cleanness ceiling
// for ancestor+child AND, demonstrating both the bug the old CP=true bool
// missed and the precise contract the new CleanAbove encodes.
//
// Setup: 2-leaf ancestor+child AND `garages.city=warsaw AND
// cars.doors.count=4`. A.Scope=garages, C.Scope=cars.doors. Under the old
// dispatch this carried CanProjectParentByAnchor=true, which was naturally
// read as "Bitmap is clean at every ancestor scope above TS" but actually
// only proved cleanness at the immediate parent of TS. The dispatch
// over-claimed.
//
// Self-contained mini-index with two docs: //   - doc 1: warsaw garage with a car having doors.count=4 → genuine
//
//	  same-garage match.
//	- doc 2: warsaw garage with NO doors.count=4 in its cars, plus a
//	  krakow garage with doors.count=4. No same-garage match: the
//	  intersection at cars.doors-level (and at cars-level, and at
//	  garages-level) is empty, but at countries-level both operands'
//	  country chain bits survive — owner-level ghost.
//
// The new model encodes the structural ceiling exactly: // CleanAbove=garages (= ancestor.TS), so [cars.doors, cars, garages] is
// asserted clean and countries is explicitly disclaimed. This test
// verifies both halves: (a) projections at cars.doors / cars / garages
// correctly yield wantDocs=[1], and (b) the projection at countries
// correctly produces the ghost — proving doc 2 leaks above CleanAbove
// and is rightly outside the cleanness range.
func TestFastPathL2_AncestorChildAND_CleanRange(t *testing.T) {
	prop := l2Schema()
	idx := newFastPathIndex("countries")

	// doc 1: genuine same-garage match (warsaw + doors=4 under same garage).
	idx.addDoc(t, prop, 1, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})
	// doc 2: split — warsaw garage has no doors=4; krakow garage does.
	// Owner-level "country has warsaw garage AND country has doors=4
	// somewhere" is true, but no SAME garage satisfies both.
	idx.addDoc(t, prop, 2, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 99}}},
			}},
			map[string]any{"city": "krakow", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})

	r := andLeaves(idx,
		leafPositive(idx, "countries.garages.city", "warsaw"),
		leafPositive(idx, "countries.garages.cars.doors.count", 4))

	require.Equal(t, "countries.garages.cars.doors", r.Scope)
	// CleanAbove = ancestor.TS, the structural ceiling for ancestor+child
	// AND. Strictly above this scope (= "countries"), owner-level ghosts
	// can survive — and we explicitly disclaim cleanness there.
	require.Equal(t, "countries.garages", r.CleanAbove)

	wantDocs := []uint64{1}

	// Baseline: Witnesses at TS yields the correct docs.
	assert.ElementsMatch(t, wantDocs, idx.docIDs(r),
		"Witnesses at TS must yield the same-garage match")

	// Inside the clean range — projections must be authentic.
	for _, scope := range []string{
		"countries.garages.cars", // parent(TS), inside clean range
		"countries.garages",      // CleanAbove, top of the clean range
	} {
		richAtScope := r.Bitmap.Clone().And(idx.anchor[scope])
		docs, _ := idx.ops.MaskRootLeaf(richAtScope)
		assert.ElementsMatch(t, wantDocs, docs.ToArray(),
			"inside clean range: Bitmap ∩ _anchor(%q) must yield wantDocs", scope)
	}

	// Strictly above the clean range — projection at countries picks up
	// doc 2 as an owner-level ghost. This is the exact behaviour the old
	// CP=true bool over-claimed away; CleanAbove=garages correctly
	// disclaims it.
	richAtCountries := r.Bitmap.Clone().And(idx.anchor["countries"])
	docsAtCountries, _ := idx.ops.MaskRootLeaf(richAtCountries)
	assert.ElementsMatch(t, []uint64{1, 2}, docsAtCountries.ToArray(),
		"above CleanAbove: projection picks up the owner-level ghost (doc 2) — this is why countries is disclaimed")
}

// TestFastPathL2_CompoundANDLiftedAboveCleanScope exercises the one
// case where the old CP=true bool and the new CleanAbove model diverge
// at runtime: a compound result whose CleanAbove sits at an
// intermediate scope (e.g. ancestor.TS from an ancestor+child AND), then
// re-merged with another operand whose TS is ABOVE that CleanAbove.
//
// Setup: `(garages.city=warsaw AND cars.doors.count=4)` has CS=garages.
// Then AND it with `garages IS NULL false` (TS=countries) — which forces
// the merge scope up to countries. The merge target is shallower than
// the compound's CleanAbove, so the compound is NOT clean at the merge
// scope; without a real lift, doc 2's ghost country bit would survive
// the intersection and leak into the result.
//
// Discriminator: doc 2 has a warsaw garage and a separate krakow
// garage-with-doors=4 (no same-garage match). doc 2 should NOT appear
// in the result. doc 1 (same-garage match) and doc 3 (genuine same-
// garage match, plus IS NULL false fires) should appear.
//
// Under the old CP=true bool: the compound would carry CP=true (over-
// claim), `liftToScope` would take the cheap anchor-intersect path, the
// compound's ghost country bit would survive, and doc 2 would leak.
//
// Under CleanAbove: the compound's CS=garages signals "not clean at
// countries", `liftToScope` takes the full-lift path (rebuild Bitmap at
// countries from authentic Witnesses), the ghost is wiped, and doc 2
// is correctly excluded.
//
// This test also pins the andLeaves dispatch guard: when the child of
// ancestor+child AND is `aboveScopeClean` but its CleanAbove is DEEPER
// than ancestor.TS, the merge must lift child to ancestor.TS — child's
// bits at ancestor.TS aren't authentic.
func TestFastPathL2_CompoundANDLiftedAboveCleanScope(t *testing.T) {
	prop := l2Schema()
	idx := newFastPathIndex("countries")

	// doc 1: genuine same-garage match.
	idx.addDoc(t, prop, 1, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})
	// doc 2: split fixture. Country has a warsaw garage (no doors=4)
	// and a krakow garage (with doors=4). NO same-garage match — must
	// be excluded from `warsaw AND doors=4 AND garages IS NULL false`.
	idx.addDoc(t, prop, 2, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 99}}},
			}},
			map[string]any{"city": "krakow", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})
	// doc 3: another genuine same-garage match — extra positive evidence
	// that the result isn't accidentally empty for some unrelated reason.
	idx.addDoc(t, prop, 3, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "warsaw", "cars": []any{
				map[string]any{"doors": []any{map[string]any{"count": 4}}},
			}},
		}},
	})

	// Step 1: build the ancestor+child compound. CleanAbove = garages
	// per the broadcasting rule.
	compound := andLeaves(idx,
		leafPositive(idx, "countries.garages.city", "warsaw"),
		leafPositive(idx, "countries.garages.cars.doors.count", 4))
	require.Equal(t, "countries.garages.cars.doors", compound.Scope)
	require.Equal(t, "countries.garages", compound.CleanAbove)

	// Step 2: AND with `garages IS NULL false` (TS=countries=propName).
	// This forces the merge to land at countries — strictly above the
	// compound's CleanAbove=garages — exercising the lift-above-CS path.
	garagesExist := leafIsNullFalse(idx, "countries.garages")
	require.Equal(t, "countries", garagesExist.Scope)
	require.Equal(t, "countries", garagesExist.CleanAbove)

	r := andLeaves(idx, compound, garagesExist)

	// The result must yield only the same-garage matches. Doc 2 has no
	// same-garage witness for warsaw+doors=4 and must NOT leak.
	wantDocs := []uint64{1, 3}
	assert.ElementsMatch(t, wantDocs, idx.docIDs(r),
		"compound AND (CS=garages) merged with leaf at countries (above CS) must lift compound to countries — ghosts above CS get wiped; doc 2 must not leak")
}
