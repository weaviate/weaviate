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
// execution model. Tests exercise the (Rich, ExactSupport, TruthScope,
// CanProjectParentByAnchor) composition rules directly against
// AssignPositions output, skipping the parser / planner / shard write layers.

// ---------------------------------------------------------------------------
// AND merge dispatch matrix
// ---------------------------------------------------------------------------
//
// andLeaves dispatches on (a) the relationship between the two inputs'
// TruthScopes and (b) their CanProjectParentByAnchor (CP) flags. Three
// scope relations × four CP combinations = 12 cells; many collapse to a
// small number of distinct rules.
//
// Notation: A = ancestor (higher TS), C = child (lower TS).
//
//   scope relation       (A.CP, C.CP)   merge scope   result.CP   action
//   ─────────────────────────────────────────────────────────────────────
//   same TS              (any, any)     left.TS       false       andAtScope
//   ─────────────────────────────────────────────────────────────────────
//   ancestor + child     (true,  true)  C.TS          true        andAtScope
//   ancestor + child     (true,  false) C.TS          false       andAtScope
//   ancestor + child     (false, true)  A.TS          false       andAtScope (no lift)
//   ancestor + child     (false, false) A.TS          false       liftToScope(C, A.TS) then andAtScope
//   ─────────────────────────────────────────────────────────────────────
//   siblings (LCA above) (any, any)     LCA           false       lift both to LCA, recurse same-TS
//
// Why each rule:
//
//   - Same TS: chain bits at parent can survive even when different
//     elements satisfy each leaf, producing ghost ancestors → CP=false.
//
//   - Ancestor+child, A.CP=true: A.Rich is clean above A.TS and broadcasts
//     to descendants (via the chain+self+descendantSelves encoding from
//     assign.go). Intersection at C.TS is a correct same-element filter.
//     Intersection never invents bits, so result CP = C.CP — A cannot
//     mask C's ghosts that coincide with A's authentic positions.
//
//   - Ancestor+child, A.CP=false / C.CP=true: A.Rich has ghosts above
//     A.TS but is trustworthy at A.TS itself (Rich ∩ _anchor(A.TS) = ES).
//     C.Rich is authentic at every scope from C.TS up to root, including
//     A.TS — its chain bits at A.TS are already clean. Direct intersection
//     at A.TS works without lifting. Result CP=false because A's ghosts
//     above A.TS remain.
//
//   - Ancestor+child, A.CP=false / C.CP=false: C.Rich may carry ghosts at
//     A.TS that would coincide with A's authentic ES and leak into the
//     result. Lift C up first — LiftToAncestor reconstructs C.Rich at
//     A.TS from authentic ExactSupport, wiping ghost bits — then merge
//     same-scope.
//
//   - Siblings: same-scope AND at LCA, structurally CP=false (same
//     reason as same-TS).
//
// The core merge formula (rich = left.Rich ∩ right.Rich, support = rich ∩
// _anchor(mergeScope)) is identical in every branch; only the merge scope
// and CP value vary.

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
type fastPathResult struct {
	TruthScope               string
	Rich                     *sroar.Bitmap
	ExactSupport             *sroar.Bitmap
	CanProjectParentByAnchor bool
}

// ---------------------------------------------------------------------------
// Leaf builders
// ---------------------------------------------------------------------------

// leafPositive realizes a positive value leaf `path = value`:
//
//	TruthScope   = parent(path)
//	Rich         = value(path, value)
//	ExactSupport = Rich ∩ _anchor(TruthScope)
//	CanProjectParentByAnchor = true
func leafPositive(idx *fastPathIndex, path string, value any) *fastPathResult {
	truthScope := parentPath(path)
	rich := cloneOrEmpty(idx.values[path][value])
	support := rich.Clone().And(idx.anchor[truthScope])
	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             support,
		CanProjectParentByAnchor: true,
	}
}

// leafIsNullFalse realizes `path IS NULL false` (equivalently exists(path)).
// Mirrors leafPositive but reads from _exists instead of a value-keyed bucket
// so the leaf fires for every emission regardless of the recorded value.
//
//	TruthScope   = parent(path)
//	Rich         = _exists(path)
//	ExactSupport = Rich ∩ _anchor(TruthScope)
//	CanProjectParentByAnchor = true
func leafIsNullFalse(idx *fastPathIndex, path string) *fastPathResult {
	truthScope := parentPath(path)
	rich := cloneOrEmpty(idx.exists[path])
	support := rich.Clone().And(idx.anchor[truthScope])
	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             support,
		CanProjectParentByAnchor: true,
	}
}

// negate produces the support-first negation of any positive leaf result.
// The transformation is identical across every NOT-style leaf (unpinned NOT,
// IS NULL true, pinned variants): the positive helper's ExactSupport is
// always the canonical positive support at TruthScope, and the negation is
// `universe \ pos.ExactSupport` for both ExactSupport (over _anchor) and
// Rich (over _exists). CanProject flips to false because Rich keeps chain
// bits above TruthScope after subtract — projecting up by anchor would
// falsely include docs where the negation actually fails.
//
//	TruthScope   = pos.TruthScope
//	ExactSupport = _anchor(TruthScope) ANDNOT pos.ExactSupport
//	Rich         = _exists(TruthScope) ANDNOT pos.ExactSupport
//	CanProjectParentByAnchor = false
func negate(idx *fastPathIndex, pos *fastPathResult) *fastPathResult {
	truthAnchor := idx.anchor[pos.TruthScope]
	return &fastPathResult{
		TruthScope:               pos.TruthScope,
		ExactSupport:             cloneOrEmpty(truthAnchor).AndNot(pos.ExactSupport),
		Rich:                     cloneOrEmpty(idx.exists[pos.TruthScope]).AndNot(pos.ExactSupport),
		CanProjectParentByAnchor: false,
	}
}

// leafIsNullTrue realizes `path IS NULL true` — universe-subtract of
// leafIsNullFalse's positive support.
func leafIsNullTrue(idx *fastPathIndex, path string) *fastPathResult {
	return negate(idx, leafIsNullFalse(idx, path))
}

// leafNot realizes `NOT path = value` — universe-subtract of leafPositive's
// positive support. Empty array scope (no elements) does NOT count as a
// witness — that falls out because pos.ExactSupport already requires at
// least one TruthScope-element to exist.
func leafNot(idx *fastPathIndex, path string, value any) *fastPathResult {
	return negate(idx, leafPositive(idx, path, value))
}

// leafPinnedIsNullFalse realizes `<pinned path>.x IS NULL false` using the
// same unified pipeline as leafPinnedPositive, just reading from _exists
// instead of a value-keyed bucket. Works for any pin layout.
//
//	A              = _exists(valuePath) ∩ _idx(pin_0) ∩ … ∩ _idx(pin_N-1)
//	mPin           = A ∩ _anchor(parent(valuePath))
//	TruthScope     = parent(pins[0].path)
//	narrowedAnchor = _anchor(TruthScope) ∩ A
//	ExactSupport   = LiftToAncestor(mPin, narrowedAnchor)
//	Rich           = (A ANDNOT _anchor(TruthScope)) ∪ ExactSupport
//	CanProjectParentByAnchor = false
//
// TODO aliszka:nested_filtering — make the lift lazy when downstream only
// consumes doc-level output; MaskRootLeaf(mPin) suffices in that case.
func leafPinnedIsNullFalse(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("pinned IS NULL false needs at least one pin")
	}

	innerAnchor := idx.anchor[parentPath(valuePath)]
	truthScope := parentPath(pins[0].path)
	truthAnchor := idx.anchor[truthScope]

	a := cloneOrEmpty(idx.exists[valuePath])
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}

	mPin := a.Clone().And(innerAnchor)

	narrowedAnchor := cloneOrEmpty(truthAnchor).And(a)
	exactSupport, _ := idx.ops.LiftToAncestor(mPin, narrowedAnchor)

	rich := a.AndNot(truthAnchor).Or(exactSupport)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafPinnedIsNullTrue realizes `<pinned path>.x IS NULL true` —
// universe-subtract of leafPinnedIsNullFalse's positive support. Works for
// any pin count. Missing pinned slot counts as true: universe \
// pos.ExactSupport includes every owner where no pinned match existed,
// including those where any pinned slot itself is missing.
func leafPinnedIsNullTrue(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	return negate(idx, leafPinnedIsNullFalse(idx, valuePath, pins))
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
//	TruthScope     = parent(pins[0].path)
//	narrowedAnchor = _anchor(TruthScope) ∩ A                          // optimization
//	ExactSupport   = LiftToAncestor(mPin, narrowedAnchor)
//	Rich           = (A ANDNOT _anchor(TruthScope)) ∪ ExactSupport
//	CanProjectParentByAnchor = false
//
// A carries chain bits inside the pinned subtree. Some are authentic
// ancestors of real matches; others are ghost bits left over when value
// matches at a sibling slot of a pin (e.g., value match at cars[3] while pin
// requires cars[2] keeps the shared country/garage chain bits).
// mPin's `∩ _anchor(parent(valuePath))` drops every chain bit (both
// authentic and ghost), leaving only leaf-scope selves where every pin
// constraint holds at a single concrete element. That's the canonical
// ExactSupport once lifted to TruthScope.
//
// narrowedAnchor: every authentic TruthScope ancestor of an mPin element is
// already in A (chain bit of the same emission), so pre-intersecting the
// parent anchor with A doesn't drop any required predecessor — but it does
// shrink the scan to only buckets that survived the pin intersections.
// For sparse filters this is a significant cut to the LiftToAncestor cost.
//
// Rich preserves chain structure within the pinned subtree by stripping only
// TruthScope-level bits before OR-ing ExactSupport back. Intermediate-level
// ghosts (e.g., garages self bits in a sibling-mismatch case) can persist
// below TruthScope; CanProjectParentByAnchor=false documents that downstream
// composition above TruthScope via anchor is unsafe.
//
// Does not support a root-level pin (TruthScope would resolve to "" with no
// corresponding parent anchor in the index). Add separate handling if a
// 3-pin path including the root array is needed.
//
// TODO aliszka:nested_filtering — make the lift lazy when downstream only
// consumes doc-level output; MaskRootLeaf(mPin) suffices in that case.
func leafPinnedPositive(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}

	innerAnchor := idx.anchor[parentPath(valuePath)]
	truthScope := parentPath(pins[0].path)
	truthAnchor := idx.anchor[truthScope]

	a := cloneOrEmpty(idx.values[valuePath][value])
	for _, pin := range pins {
		a.And(idx.idx[pin.path][pin.index])
	}

	mPin := a.Clone().And(innerAnchor)

	narrowedAnchor := cloneOrEmpty(truthAnchor).And(a)
	exactSupport, _ := idx.ops.LiftToAncestor(mPin, narrowedAnchor)

	rich := a.AndNot(truthAnchor).Or(exactSupport)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafPinnedNot realizes `NOT <pinned path>.x = value` — universe-subtract
// of leafPinnedPositive's positive support. Works for any pin count (single,
// intermediate, multi) since leafPinnedPositive is unified.
//
// Missing pinned slot counts as success: both "value mismatch" and "no
// pinned slot" satisfy the NOT. This falls out of the universe-subtract —
// pos.ExactSupport is empty for owners whose pinned slot doesn't exist, and
// universe \ ∅ = universe, so those owners survive.
func leafPinnedNot(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	return negate(idx, leafPinnedPositive(idx, valuePath, value, pins))
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
	if left.TruthScope == right.TruthScope {
		return andAtScope(idx, left, right, left.TruthScope, false)
	}

	common := commonScope(left.TruthScope, right.TruthScope)

	// Siblings (LCA is above both) → lift both up, recurse as same-scope.
	if common != left.TruthScope && common != right.TruthScope {
		return andLeaves(idx,
			liftToScope(idx, left, common),
			liftToScope(idx, right, common))
	}

	// One is ancestor of the other.
	var ancestor, child *fastPathResult
	if common == left.TruthScope {
		ancestor, child = left, right
	} else {
		ancestor, child = right, left
	}

	if ancestor.CanProjectParentByAnchor {
		// A clean above A.TS, broadcasts to descendants → safe filter at
		// C.TS. Result CP propagates from child: A cannot mask C's ghosts
		// that coincide with A's authentic positions.
		return andAtScope(idx, child, ancestor, child.TruthScope, child.CanProjectParentByAnchor)
	}
	if child.CanProjectParentByAnchor {
		// C.Rich is authentic at every scope from C.TS up to root, so its
		// chain bits at A.TS are already clean. Intersect directly at A.TS
		// — no lift needed. A.Rich is trustworthy at A.TS by the
		// scope invariant, so support = (A.Rich ∩ C.Rich) ∩ _anchor(A.TS)
		// is correct. Result CP=false: A's ghosts above A.TS remain.
		return andAtScope(idx, ancestor, child, ancestor.TruthScope, false)
	}
	// Both carry ghosts; C's may reach A.TS. Lift C up so its A.TS bits
	// are rebuilt from authentic ExactSupport, then merge same-scope.
	lifted := liftToScope(idx, child, ancestor.TruthScope)
	return andAtScope(idx, ancestor, lifted, ancestor.TruthScope, false)
}

// andAtScope is the shared core merge formula used by every branch of
// andLeaves once the merge scope and CanProject value are known.
func andAtScope(idx *fastPathIndex, left, right *fastPathResult, scope string, canProject bool) *fastPathResult {
	rich := left.Rich.Clone().And(right.Rich)
	support := rich.Clone().And(idx.anchor[scope])
	return &fastPathResult{
		TruthScope:               scope,
		Rich:                     rich,
		ExactSupport:             support,
		CanProjectParentByAnchor: canProject,
	}
}

// liftToScope re-wraps a leaf result at a higher (ancestor) TruthScope.
// Useful when sibling leaves with different natural witness scopes share a
// common ancestor where the merge should land (e.g. `cars.accessories.type`
// at TruthScope=accessories and `cars.tires.width` at TruthScope=tires both
// merge at cars).
//
// The lift method depends on leaf.CanProjectParentByAnchor:
//
//   - true: anchor intersection is safe — no ghost ancestors above the
//     original TruthScope, so Rich ∩ _anchor(targetScope) equals the
//     correct lifted ExactSupport. Rich is reused unchanged.
//
//   - false: Rich carries ghosts at intermediate scopes. Use a real
//     predecessor lift (LiftToAncestor) on ExactSupport to project to
//     targetScope, then reconstruct Rich at the new scope by stripping
//     target-scope ghost bits and OR-ing back the authentic lifted ES —
//     mirroring how pinned positive helpers strip TS ghosts.
//
// targetScope must be an ancestor of leaf.TruthScope in the schema (or
// equal, in which case the input is returned unchanged). Caller is
// responsible for ensuring this — no schema lookup happens here.
//
// CanProject is preserved (lift doesn't change ghost characteristics above
// the original TruthScope).
func liftToScope(idx *fastPathIndex, leaf *fastPathResult, targetScope string) *fastPathResult {
	if leaf.TruthScope == targetScope {
		return leaf
	}
	targetAnchor := idx.anchor[targetScope]
	var rich, support *sroar.Bitmap
	if leaf.CanProjectParentByAnchor {
		rich = leaf.Rich
		support = leaf.Rich.Clone().And(targetAnchor)
	} else {
		support, _ = idx.ops.LiftToAncestor(leaf.ExactSupport, targetAnchor)
		rich = leaf.Rich.Clone().AndNot(targetAnchor).Or(support)
	}
	return &fastPathResult{
		TruthScope:               targetScope,
		Rich:                     rich,
		ExactSupport:             support,
		CanProjectParentByAnchor: leaf.CanProjectParentByAnchor,
	}
}

// orLeaves performs a same-scope OR merge of two leaf results. Both inputs
// must share TruthScope.
//
//	Rich         = left.Rich ∪ right.Rich
//	ExactSupport = left.ExactSupport ∪ right.ExactSupport   (cheaper than rich ∩ anchor)
//	TruthScope   = left.TruthScope
//	CanProjectParentByAnchor = left && right
//
// Unlike same-scope AND, OR preserves CanProject when both inputs do:
// chain bits at parent scope in Rich legitimately indicate "some matching
// element under this parent" — which is exactly the existential OR semantic.
// No ghost ancestors are introduced.
func orLeaves(idx *fastPathIndex, left, right *fastPathResult) *fastPathResult {
	if left.TruthScope != right.TruthScope {
		panic("orLeaves requires same TruthScope")
	}
	rich := left.Rich.Clone().Or(right.Rich)
	support := left.ExactSupport.Clone().Or(right.ExactSupport)
	return &fastPathResult{
		TruthScope:               left.TruthScope,
		Rich:                     rich,
		ExactSupport:             support,
		CanProjectParentByAnchor: left.CanProjectParentByAnchor && right.CanProjectParentByAnchor,
	}
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

func cloneOrEmpty(bm *sroar.Bitmap) *sroar.Bitmap {
	if bm == nil {
		return sroar.NewBitmap()
	}
	return bm.Clone()
}

// docIDs runs ExactSupport through the real BitmapOps.MaskRootLeaf so the
// projection path itself is under test.
func (i *fastPathIndex) docIDs(r *fastPathResult) []uint64 {
	if r.ExactSupport == nil || r.ExactSupport.IsEmpty() {
		return nil
	}
	doc, _ := i.ops.MaskRootLeaf(r.ExactSupport)
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
type fastPathTestCase struct {
	name           string
	build          func(*fastPathIndex) *fastPathResult
	wantTruthScope string
	wantCanProject bool
	wantDocs       []uint64 // after the real MaskRootLeaf projection
}

// runFastPathCases drives the standard assertion suite for any
// fastPathTestCase table: scope/CanProject equality, ES non-empty iff wantDocs
// non-empty, trustworthy-scope invariant, doc-id equality, and (when
// CanProject=true) the no-ghost-at-parent contract.
func runFastPathCases(t *testing.T, idx *fastPathIndex, cases []fastPathTestCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.build(idx)
			assert.Equal(t, tc.wantTruthScope, r.TruthScope)
			assert.Equal(t, tc.wantCanProject, r.CanProjectParentByAnchor)
			// ExactSupport mirrors wantDocs: non-empty iff wantDocs non-empty.
			// Rich is non-empty in the populated case; for empty-result we
			// skip the Rich check because some negation shapes can leave
			// ancestor chain bits in Rich even when ExactSupport is empty.
			if len(tc.wantDocs) == 0 {
				assert.True(t, r.ExactSupport.IsEmpty(), "ExactSupport must be empty when wantDocs is empty")
			} else {
				assert.False(t, r.Rich.IsEmpty(), "Rich must not be empty")
				assert.False(t, r.ExactSupport.IsEmpty(), "ExactSupport must not be empty")
			}
			// Trustworthy-scope invariant: Rich ∩ _anchor(TruthScope) must
			// equal ExactSupport. Holds regardless of CanProjectParentByAnchor
			// — the flag only governs whether projection ABOVE TruthScope by
			// anchor is safe.
			richAtTruth := r.Rich.Clone().And(idx.anchor[r.TruthScope])
			assert.Equal(t, r.ExactSupport.ToArray(), richAtTruth.ToArray(),
				"Rich ∩ _anchor(TruthScope) must equal ExactSupport")
			assert.ElementsMatch(t, tc.wantDocs, idx.docIDs(r))

			// Ghost-free contract for CanProjectParentByAnchor=true: projecting
			// Rich to the parent anchor and masking to docIDs must give exactly
			// wantDocs. Any extra doc means a chain bit from a non-matching
			// element leaked through (a ghost ancestor).
			if r.CanProjectParentByAnchor {
				parent := parentPath(r.TruthScope)
				richAtParent := r.Rich.Clone().And(idx.anchor[parent])
				parentDoc, _ := idx.ops.MaskRootLeaf(richAtParent)
				assert.ElementsMatch(t, tc.wantDocs, parentDoc.ToArray(),
					"CanProject=true: Rich ∩ _anchor(parent) must yield wantDocs (no ghost ancestors)")
			}
		})
	}
}

// TestFastPathL2_SingleLeaf exercises the single-leaf shapes against the L2
// schema. Each subtest verifies the full result quadruple (TruthScope, Rich,
// ExactSupport, CanProjectParentByAnchor) plus the masked doc IDs from
// BitmapOps.MaskRootLeaf.
func TestFastPathL2_SingleLeaf(t *testing.T) {
	idx := buildL2(t)

	cases := []fastPathTestCase{
		{
			name: "cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.year", 2020)
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: true,
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
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			wantDocs:       []uint64{200, 600, 810, 820},
		},
		{
			name: "cars.year IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars.year")
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: true,
			wantDocs:       []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820},
		},
		{
			name: "cars[1].year IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.year",
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			wantDocs:       []uint64{100, 200, 500, 600, 810, 820},
		},
		{
			name: "cars.year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "countries.garages.cars.year")
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
			wantDocs:       []uint64{200, 300, 500, 600, 800, 830, 900},
		},
		{
			name: "cars[1].year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.year",
					[]pinSpec{{"countries.garages.cars", 1}})
			},
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			wantDocs:       []uint64{300, 400, 700, 800, 830, 900},
		},
		{
			name: "NOT cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.year", 2020)
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
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
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			// 810 FAILS: cars[1].year=2020 in the only garage. 820 FAILS:
			// every country's cars[1] has year=2020 — no garage witness anywhere.
			// 830 PASSES: no cars[1] → missing pinned slot satisfies NOT.
			// 800/900 PASS: cars[1] missing or has no year.
			wantDocs: []uint64{100, 300, 400, 500, 600, 700, 800, 830, 900},
		},
		{
			name: "cars IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars")
			},
			wantTruthScope: "countries.garages",
			wantCanProject: true,
			wantDocs:       []uint64{100, 200, 300, 400, 500, 600, 700, 800, 810, 820, 830, 900},
		},
		{
			name: "cars IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "countries.garages.cars")
			},
			wantTruthScope: "countries.garages",
			wantCanProject: false,
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
			wantTruthScope: "countries.garages.cars",
			wantCanProject: true,
			wantDocs:       []uint64{100, 200, 300},
		},
		{
			name: "NOT cars.colors=red",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "countries.garages.cars.colors", "red")
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
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
			wantTruthScope: "countries.garages.cars.accessories",
			wantCanProject: true,
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
			wantTruthScope: "countries.garages.cars.accessories",
			wantCanProject: false,
			// doc 100: cars[1]'s radio is non-spoiler witness.
			// doc 200: all spoilers → no witness → FAIL.
			// 300-820: no accessories → empty scope, no witness possible → FAIL.
			// 830: accessories[0] is radio → witness.
			wantDocs: []uint64{100, 830},
		},
		// Empty-result edge case — value present in the schema but no doc
		// has it. Exercises the no-match path through every helper
		// invariant (Rich/ExactSupport empty, MaskRootLeaf on empty input).
		{
			name: "cars.year=9999 (empty result)",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "countries.garages.cars.year", 9999)
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: true,
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
			wantTruthScope: "countries",
			wantCanProject: false,
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
			wantTruthScope: "countries",
			wantCanProject: false,
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
			wantTruthScope: "countries",
			wantCanProject: false,
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
			wantTruthScope: "countries",
			wantCanProject: false,
			// Same set as the IS NULL true case — the only doc where the
			// full pinned path has name=honda is doc 500. Every other doc
			// passes either via missing pin or via name != honda.
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 800, 810, 820, 830, 900},
		},
		// Intermediate-pin: pin at garages[1] with cars unpinned. Discriminates
		// from single-pin `cars.make=bmw` (which would also include doc 600).
		// Only doc 500 has garages[1] containing a car with make=bmw.
		// NOT and IS NULL true variants have an owner-level vs per-element
		// semantic ambiguity at intermediate-pin and are deferred.
		{
			name: "garages[1].cars.make=bmw",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx,
					"countries.garages.cars.make",
					"bmw",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantTruthScope: "countries",
			wantCanProject: false,
			wantDocs:       []uint64{500},
		},
		{
			name: "garages[1].cars.make IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx,
					"countries.garages.cars.make",
					[]pinSpec{{"countries.garages", 1}})
			},
			wantTruthScope: "countries",
			wantCanProject: false,
			// doc 500: g[1].cars[0].make=bmw. doc 700: g[1].cars[0].make=ford.
			// All other docs lack garages[1] or have no make on any car in g[1].
			wantDocs: []uint64{500, 700},
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
		// CanProject=false: Rich at parent (garages) keeps chain bits for
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
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
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
		// predicate". CanProject=true is preserved because Rich chain bits at
		// parent legitimately indicate "garages with at least one matching
		// car for either leaf" — the existential OR semantic at parent.
		{
			name: "cars.make=honda OR cars.year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "countries.garages.cars.make", "honda"),
					leafPositive(idx, "countries.garages.cars.year", 2020))
			},
			wantTruthScope: "countries.garages.cars",
			wantCanProject: true,
			// 100: cars[0].year=2020. 200: cars[0].make=honda. 300:
			// cars[1].make=honda. 600 country 1: cars[1].year=2020.
			// 810: cars[0] year=2020 (and make=honda). 820 country 0:
			// cars[1] year=2020 (and make=honda). 400/500/700/830 lack both.
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
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
			// doc 100: cars[1].colors=["blue","red"]. Other docs either lack
			// colors entirely (500/600/700/810/820/830), have only one of the
			// values (200 has blue and red split across cars[0]/cars[1]; 300
			// has only red; 400 has only "yellow"), or have neither.
			wantDocs: []uint64{100},
		},
		// Sibling branches under a shared owner. Each leaf's natural
		// TruthScope is its own array element (accessories vs tires); the
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
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
			// doc 100: cars[0] has both spoiler-accessory and tires.width=205.
			// doc 200: cars[0] and cars[1] both have both. doc 800 g[0].cars[0]
			// has both. Other docs lack either accessories or tires (or both);
			// doc 830 has a spoiler accessory on cars[0] but no tires.
			// doc 900 has spoiler in g[1].cars[0] and tires in g[1].cars[1] —
			// no single car has both, FAIL.
			wantDocs: []uint64{100, 200, 800},
		},
		// Pinned+pinned merge at owner. Both leaves naturally have
		// TruthScope=garages after pin consumption, so andLeaves works
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
			wantTruthScope: "countries.garages",
			wantCanProject: false,
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
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			// doc 100: g[0] cars[1]=2018 satisfies NOT, cars[1].make=honda.
			// doc 300: g[0] cars[1] missing year satisfies NOT (missing
			// pinned slot is a witness), cars[1].make=honda.
			// 200/810/820: cars[1].year=2020 fails NOT. 400/500/600/700/830:
			// no make=honda or no satisfying garage.
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
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
			wantDocs:       []uint64{800},
		},
		// Ancestor+child AND with both CP=true. Ancestor garages.city=warsaw
		// (TS=garages) AND child cars.doors.count=4 (TS=doors). Merge at
		// child.TS=doors with CP=child.CP=true. Ancestor.Rich broadcasts to
		// descendants via the chain encoding, so doors-scope intersection
		// keeps only doors whose garage chain includes a warsaw garage.
		{
			name: "garages.city=warsaw AND cars.doors.count=4",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "countries.garages.city", "warsaw"),
					leafPositive(idx, "countries.garages.cars.doors.count", 4))
			},
			wantTruthScope: "countries.garages.cars.doors",
			wantCanProject: true,
			// 800 g[0] (warsaw): cars[0].doors=4 → MATCH. 800 g[1] (krakow):
			// doors=4 but chain is krakow → no warsaw overlap.
			// 900 g[0] (warsaw): cars[0].doors=4 → MATCH. 900 g[1] (krakow):
			// doors=4 but krakow.
			// No other fixture sets city or doors.
			wantDocs: []uint64{800, 900},
		},
		// Ancestor+child AND with both CP=false. Ancestor pinned NOT
		// (TS=garages) AND child same-scope AND at cars (TS=cars). Both
		// inputs carry ghosts, so child gets lifted to ancestor.TS=garages
		// first (LiftToAncestor rebuilds child.Rich at garages from
		// authentic ExactSupport, wiping ghost bits), then same-scope AND
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
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			// Inner AND matches only doc 100 cars[1] — the only car with
			// both red and blue colors in any fixture. Outer NOT witness
			// covers doc 100 g[0] (cars[1].year=2018 ≠ 2020). Lifted child
			// at garages = {doc 100 g[0]}; ancestor ES at garages includes
			// that garage. Intersection → doc 100.
			wantDocs: []uint64{100},
		},
		// Contrast to the ancestor-constrained ghost-mitigation case above:
		// adding a same-scope sibling at cars (doors.count=4) does NOT clean
		// the ghost at the parent garage. The inner sibling-AND
		// (spoiler AND 205) carries chain bits at BOTH the warsaw garage
		// (real witness via cars[0]) and the krakow garage (ghost from
		// cars[0]=spoiler / cars[1]=205 split). doors.count=4 fires in cars
		// under BOTH garages of doc 800, so its chain bits overlap with the
		// ghost at krakow — the same-scope intersection preserves both
		// garages in Rich. ExactSupport at cars is still clean (only the
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
			wantTruthScope: "countries.garages.cars",
			wantCanProject: false,
			// doc 800: warsaw cars[0] has spoiler+205+doors=4 → MATCH.
			// krakow cars split spoiler/205 across cars, both have doors=4
			// → no single car satisfies all three at car-self → ghost only.
			// doc 900: warsaw cars[0] has only doors → no inner AND witness.
			// krakow cars split spoiler/205 like in 800 → ghost only.
			// Net witness across all garages: only doc 800 warsaw car.
			wantDocs: []uint64{800},
		},
	}

	runFastPathCases(t, idx, cases)
}
