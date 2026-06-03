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

// leafIsNullTrue realizes `path IS NULL true` for a scalar property inside an
// object array. Witness is the array element (parent of path) where the scalar
// is missing. Support-first negation: Rich subtracts the canonical positive
// ExactSupport from the outer rich carrier, so Rich is non-canonical.
//
//	TruthScope   = parent(path)
//	posExact     = _exists(path) ∩ _anchor(TruthScope)       canonical positive ES
//	ExactSupport = _anchor(TruthScope) ANDNOT posExact
//	Rich         = _exists(TruthScope) ANDNOT posExact
//	CanProjectParentByAnchor = false                          (support-first negation)
func leafIsNullTrue(idx *fastPathIndex, path string) *fastPathResult {
	truthScope := parentPath(path)
	truthAnchor := idx.anchor[truthScope]

	posExact := cloneOrEmpty(idx.exists[path]).And(truthAnchor)

	exactSupport := cloneOrEmpty(truthAnchor).AndNot(posExact)
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(posExact)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafNot realizes `NOT path = value`. Existential at the leaf's natural
// witness scope: there exists an array element where the property is missing
// or has a different value. Per §4.1.1.A, empty array scope (no elements at
// all) does NOT count as a witness.
//
//	TruthScope   = parent(path)
//	posExact     = value(path, value) ∩ _anchor(TruthScope)
//	ExactSupport = _anchor(TruthScope) ANDNOT posExact
//	Rich         = _exists(TruthScope) ANDNOT posExact
//	CanProjectParentByAnchor = false                          (support-first negation)
func leafNot(idx *fastPathIndex, path string, value any) *fastPathResult {
	truthScope := parentPath(path)
	truthAnchor := idx.anchor[truthScope]

	posExact := cloneOrEmpty(idx.values[path][value]).And(truthAnchor)

	exactSupport := cloneOrEmpty(truthAnchor).AndNot(posExact)
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(posExact)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafPinnedPositive realizes a pinned positive leaf `pinPath[pinIndex].x = value`,
// where valuePath = pinPath + ".x". Pin-to-owner: leaf evaluation narrows to
// the pinned slot, but the result is reinterpreted as a predicate about the
// pin's owner.
//
//	A            = value(valuePath, value) ∩ _idx(pinPath, pinIndex)
//	mPinnedMatch = A ∩ _anchor(pinPath)
//	ExactSupport = liftOneLevel(mPinnedMatch, _anchor(TruthScope))
//	Rich         = (A ANDNOT _anchor(TruthScope)) ∪ ExactSupport
//	TruthScope   = parent(pinPath)               (owner of the pinned array)
//	CanProjectParentByAnchor = false
//
// A also carries "ghost ancestors" — chain bits (country/garage selves) from
// docs where some other slot matched the value but the pinned slot did not.
// Stripping _anchor(TruthScope) removes ghost garage selves; OR-ing
// ExactSupport adds back the real ones. Higher-level ghosts (countries) stay
// in Rich; CanProjectParentByAnchor=false documents that only TruthScope is
// trustworthy, so downstream callers must not project above it by anchor.
func leafPinnedPositive(idx *fastPathIndex, valuePath, pinPath string, pinIndex int, value any) *fastPathResult {
	truthScope := parentPath(pinPath)
	ownerAnchor := idx.anchor[truthScope]

	// A = value ∩ _idx(pinPath, pinIndex). Carries chain + self + descendants
	// for real pinned matches, plus "ghost ancestors" (shared chain bits) from
	// docs where some other slot matched the value but the pinned slot did not.
	a := cloneOrEmpty(idx.values[valuePath][value]).
		And(idx.idx[pinPath][pinIndex])

	// ExactSupport: real pinned matches lifted to owner scope.
	// TODO aliszka:nested_filtering — make the lift lazy. If the result is only
	// consumed for doc-level output, MaskRootLeaf(mPinnedMatch) already gives
	// the correct docs; lift is only required when downstream actually needs
	// owner-scope ExactSupport (e.g., AND with a pin-incompatible sibling or
	// owner-scope output as the final bitmap).
	mPinnedMatch := a.Clone().And(idx.anchor[pinPath])
	exactSupport, _ := idx.ops.LiftOneLevel(mPinnedMatch, ownerAnchor)

	// Rich: strip owner-level ghosts from A in place, add back real owner
	// selves from ExactSupport. Higher-level ghosts (ancestors of owner)
	// remain; CanProjectParentByAnchor=false documents that only TruthScope
	// is trustworthy, so callers must not project above by anchor.
	rich := a.AndNot(ownerAnchor).Or(exactSupport)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafPinnedIsNullFalse realizes `pinPath[pinIndex].x IS NULL false`. Same
// ghost-strip + lift-addback structure as leafPinnedPositive; only the source
// bucket differs (_exists instead of value-keyed).
//
//	A            = _exists(valuePath) ∩ _idx(pinPath, pinIndex)
//	mPinnedMatch = A ∩ _anchor(pinPath)
//	ExactSupport = liftOneLevel(mPinnedMatch, _anchor(TruthScope))
//	Rich         = (A ANDNOT _anchor(TruthScope)) ∪ ExactSupport
//	TruthScope   = parent(pinPath)
//	CanProjectParentByAnchor = false
func leafPinnedIsNullFalse(idx *fastPathIndex, valuePath, pinPath string, pinIndex int) *fastPathResult {
	truthScope := parentPath(pinPath)
	ownerAnchor := idx.anchor[truthScope]

	a := cloneOrEmpty(idx.exists[valuePath]).
		And(idx.idx[pinPath][pinIndex])

	// TODO aliszka:nested_filtering — make the lift lazy. If the result is only
	// consumed for doc-level output, MaskRootLeaf(mPinnedMatch) already gives
	// the correct docs; lift is only required when downstream actually needs
	// owner-scope ExactSupport.
	mPinnedMatch := a.Clone().And(idx.anchor[pinPath])
	exactSupport, _ := idx.ops.LiftOneLevel(mPinnedMatch, ownerAnchor)

	rich := a.AndNot(ownerAnchor).Or(exactSupport)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafPinnedIsNullTrue realizes `pinPath[pinIndex].x IS NULL true`. Missing
// pinned slot counts as true (per design's owner-centric pinned semantic).
// Support-first negation at owner scope: the canonical positive ES at owner
// is computed via _anchor(pinPath) narrowing + LiftOneLevel, then subtracted
// from the universe. Skipping the lift (raw `E ∩ I1 ∩ G`) would leave ghost
// owners in the positive set and incorrectly drop them from ExactSupport.
//
//	A            = _exists(valuePath) ∩ _idx(pinPath, pinIndex)
//	mPinnedMatch = A ∩ _anchor(pinPath)
//	canonPos     = liftOneLevel(mPinnedMatch, _anchor(TruthScope))
//	ExactSupport = _anchor(TruthScope) ANDNOT canonPos
//	Rich         = _exists(TruthScope) ANDNOT canonPos
//	TruthScope   = parent(pinPath)
//	CanProjectParentByAnchor = false
func leafPinnedIsNullTrue(idx *fastPathIndex, valuePath, pinPath string, pinIndex int) *fastPathResult {
	truthScope := parentPath(pinPath)
	ownerAnchor := idx.anchor[truthScope]

	mPinnedMatch := cloneOrEmpty(idx.exists[valuePath]).
		And(idx.idx[pinPath][pinIndex]).
		And(idx.anchor[pinPath])
	// TODO aliszka:nested_filtering — make the lift lazy. If the result is only
	// consumed for doc-level output, the answer is `universe_docs \
	// MaskRootLeaf(mPinnedMatch)`; lift is only required when downstream needs
	// owner-scope ExactSupport (e.g., AND/OR composition at owner scope).
	canonPos, _ := idx.ops.LiftOneLevel(mPinnedMatch, ownerAnchor)

	exactSupport := cloneOrEmpty(ownerAnchor).AndNot(canonPos)
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(canonPos)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// pinSpec describes one pin in a multi-pin path, e.g.,
// `garages[1].cars[2].year=2020` carries two pins: `{path:"countries.garages",
// index:1}` and `{path:"countries.garages.cars", index:2}`.
type pinSpec struct {
	path  string // qualified path of the pinned array
	index int    // pinned index within that array
}

// leafMultiPinPositive realizes a multi-pin positive value leaf, e.g.,
// `garages[1].cars[2].name=honda`. Pins are listed outer-to-inner. Each pin
// gets consumed at its level (narrow by `_idx ∩ _anchor`), then we lift to
// the next outer pin's level. After processing all pins, a final lift takes
// us past the outermost pin's level to the owner per design's
// pin-consumption rule (parent of outermost pin path).
//
//	A   = value(valuePath, value) ∩ _idx(innermost) ∩ _anchor(innermost)
//	for each outer pin (inner→outer):
//	    A = LiftOneLevel(A, _anchor(pin.path))
//	    A = A ∩ _idx(pin.path, pin.index) ∩ _anchor(pin.path)
//	A   = LiftOneLevel(A, _anchor(TruthScope))
//	TruthScope   = parent(pins[0].path)
//	ExactSupport = A
//	Rich         = A   (trivially satisfies Rich ∩ _anchor(TruthScope) == ES)
//	CanProjectParentByAnchor = false
//
// Does not support a root-level pin (TruthScope would resolve to "" with no
// corresponding parent anchor in the index). Add separate handling if a
// 3-pin path including the root array is needed.
//
// TODO aliszka:nested_filtering — like other pinned helpers, the lift chain
// here is eager; downstream consumers that only need docs could mask the
// inner-pin result with successive `_idx ∩ _anchor` narrows and skip lifts.
func leafMultiPinPositive(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}

	inner := pins[len(pins)-1]
	a := cloneOrEmpty(idx.values[valuePath][value]).
		And(idx.idx[inner.path][inner.index]).
		And(idx.anchor[inner.path])

	for i := len(pins) - 2; i >= 0; i-- {
		pin := pins[i]
		a, _ = idx.ops.LiftOneLevel(a, idx.anchor[pin.path])
		a = a.And(idx.idx[pin.path][pin.index]).And(idx.anchor[pin.path])
	}

	truthScope := parentPath(pins[0].path)
	a, _ = idx.ops.LiftOneLevel(a, idx.anchor[truthScope])

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     a.Clone(),
		ExactSupport:             a,
		CanProjectParentByAnchor: false,
	}
}

// leafMultiPinIsNullFalse: positive existence under a multi-pin path. Same
// narrow+lift chain as leafMultiPinPositive but reads from _exists.
//
// TODO aliszka:nested_filtering — eager-lift like other pinned helpers; could
// be deferred for pure doc-level output.
func leafMultiPinIsNullFalse(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}

	inner := pins[len(pins)-1]
	a := cloneOrEmpty(idx.exists[valuePath]).
		And(idx.idx[inner.path][inner.index]).
		And(idx.anchor[inner.path])

	for i := len(pins) - 2; i >= 0; i-- {
		pin := pins[i]
		a, _ = idx.ops.LiftOneLevel(a, idx.anchor[pin.path])
		a = a.And(idx.idx[pin.path][pin.index]).And(idx.anchor[pin.path])
	}

	truthScope := parentPath(pins[0].path)
	a, _ = idx.ops.LiftOneLevel(a, idx.anchor[truthScope])

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     a.Clone(),
		ExactSupport:             a,
		CanProjectParentByAnchor: false,
	}
}

// leafMultiPinIsNullTrue: universe-subtract for `<path> IS NULL true` under a
// multi-pin path. Any missing pin or missing terminal property at the outer
// owner counts as success (§4.1.1.B applied at every pin level).
//
//	canonPos     = multi-pin chain over _exists(valuePath) — country selves
//	               where the full pinned path exists.
//	ExactSupport = _anchor(TruthScope) ANDNOT canonPos
//	Rich         = _exists(TruthScope) ANDNOT canonPos
//	TruthScope   = parent(pins[0].path)
//	CanProjectParentByAnchor = false
//
// TODO aliszka:nested_filtering — eager-lift like other pinned helpers.
func leafMultiPinIsNullTrue(idx *fastPathIndex, valuePath string, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}

	inner := pins[len(pins)-1]
	canonPos := cloneOrEmpty(idx.exists[valuePath]).
		And(idx.idx[inner.path][inner.index]).
		And(idx.anchor[inner.path])

	for i := len(pins) - 2; i >= 0; i-- {
		pin := pins[i]
		canonPos, _ = idx.ops.LiftOneLevel(canonPos, idx.anchor[pin.path])
		canonPos = canonPos.And(idx.idx[pin.path][pin.index]).And(idx.anchor[pin.path])
	}

	truthScope := parentPath(pins[0].path)
	truthAnchor := idx.anchor[truthScope]
	canonPos, _ = idx.ops.LiftOneLevel(canonPos, truthAnchor)

	exactSupport := cloneOrEmpty(truthAnchor).AndNot(canonPos)
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(canonPos)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafMultiPinNot: universe-subtract for `NOT <path>=value` under a multi-pin
// path. Same shape as leafMultiPinIsNullTrue but uses value lookup instead of
// _exists. Missing pin → witness (§4.1.1.B applied at every pin level).
//
// TODO aliszka:nested_filtering — eager-lift like other pinned helpers.
func leafMultiPinNot(idx *fastPathIndex, valuePath string, value any, pins []pinSpec) *fastPathResult {
	if len(pins) == 0 {
		panic("multi-pin needs at least one pin")
	}

	inner := pins[len(pins)-1]
	canonPos := cloneOrEmpty(idx.values[valuePath][value]).
		And(idx.idx[inner.path][inner.index]).
		And(idx.anchor[inner.path])

	for i := len(pins) - 2; i >= 0; i-- {
		pin := pins[i]
		canonPos, _ = idx.ops.LiftOneLevel(canonPos, idx.anchor[pin.path])
		canonPos = canonPos.And(idx.idx[pin.path][pin.index]).And(idx.anchor[pin.path])
	}

	truthScope := parentPath(pins[0].path)
	truthAnchor := idx.anchor[truthScope]
	canonPos, _ = idx.ops.LiftOneLevel(canonPos, truthAnchor)

	exactSupport := cloneOrEmpty(truthAnchor).AndNot(canonPos)
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(canonPos)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
	}
}

// leafPinnedNot realizes `NOT pinPath[pinIndex].x = value`. Missing pinned
// slot counts as success (per design's owner-centric pinned semantic — both
// "year != 2020" and "no cars[1]" make the NOT true). Same ghost-strip via
// lift as leafPinnedIsNullTrue; only the source bucket differs (value vs
// _exists).
//
//	A            = value(valuePath, value) ∩ _idx(pinPath, pinIndex)
//	mPinnedMatch = A ∩ _anchor(pinPath)
//	canonPos     = liftOneLevel(mPinnedMatch, _anchor(TruthScope))
//	ExactSupport = _anchor(TruthScope) ANDNOT canonPos
//	Rich         = _exists(TruthScope) ANDNOT canonPos
//	TruthScope   = parent(pinPath)
//	CanProjectParentByAnchor = false
func leafPinnedNot(idx *fastPathIndex, valuePath, pinPath string, pinIndex int, value any) *fastPathResult {
	truthScope := parentPath(pinPath)
	ownerAnchor := idx.anchor[truthScope]

	mPinnedMatch := cloneOrEmpty(idx.values[valuePath][value]).
		And(idx.idx[pinPath][pinIndex]).
		And(idx.anchor[pinPath])
	// TODO aliszka:nested_filtering — make the lift lazy. If the result is only
	// consumed for doc-level output, the answer is `universe_docs \
	// MaskRootLeaf(mPinnedMatch)`; lift is only required when downstream needs
	// owner-scope ExactSupport.
	canonPos, _ := idx.ops.LiftOneLevel(mPinnedMatch, ownerAnchor)

	exactSupport := cloneOrEmpty(ownerAnchor).AndNot(canonPos)
	rich := cloneOrEmpty(idx.exists[truthScope]).AndNot(canonPos)

	return &fastPathResult{
		TruthScope:               truthScope,
		Rich:                     rich,
		ExactSupport:             exactSupport,
		CanProjectParentByAnchor: false,
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

// TestFastPathL2_SingleLeaf exercises the single-leaf shapes against the L2
// schema. Each subtest verifies the full result quadruple (TruthScope, Rich,
// ExactSupport, CanProjectParentByAnchor) plus the masked doc IDs from
// BitmapOps.MaskRootLeaf.
func TestFastPathL2_SingleLeaf(t *testing.T) {
	idx := buildL2(t)

	cases := []struct {
		// name documents the query under test.
		name string
		// build constructs the result lazily so each subtest captures its own
		// expression — easier to scan than a slice of *fastPathResult.
		build func(*fastPathIndex) *fastPathResult
		// Expected result fields.
		wantTruthScope string
		wantCanProject bool
		// Expected docs after the real MaskRootLeaf projection.
		wantDocs []uint64
	}{
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
					"countries.garages.cars", 1, 2020)
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
					"countries.garages.cars", 1)
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
			wantDocs:       []uint64{200, 300, 500, 600, 830},
		},
		{
			name: "cars[1].year IS NULL true",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx,
					"countries.garages.cars.year",
					"countries.garages.cars", 1)
			},
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			wantDocs:       []uint64{300, 400, 700, 830},
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
			// PASSES: car has no year, which counts as "not 2020".
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 820, 830},
		},
		{
			name: "NOT cars[1].year=2020",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx,
					"countries.garages.cars.year",
					"countries.garages.cars", 1, 2020)
			},
			wantTruthScope: "countries.garages",
			wantCanProject: false,
			// 810 FAILS: cars[1].year=2020 in the only garage. 820 FAILS:
			// every country's cars[1] has year=2020 — no garage witness anywhere.
			// 830 PASSES: no cars[1] → missing slot ≡ true (§4.1.1.B).
			wantDocs: []uint64{100, 300, 400, 500, 600, 700, 830},
		},
		{
			name: "cars IS NULL false",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "countries.garages.cars")
			},
			wantTruthScope: "countries.garages",
			wantCanProject: true,
			wantDocs:       []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820, 830},
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
			wantDocs: []uint64{100, 200, 300, 400, 500, 600, 700, 810, 820, 830},
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
			// non-first array elements at depth 4.
			wantDocs: []uint64{100, 200, 830},
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
			// 300-820: no accessories → empty scope (§4.1.1.A) → FAIL.
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
				return leafMultiPinPositive(idx,
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
				return leafMultiPinIsNullFalse(idx,
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
				return leafMultiPinIsNullTrue(idx,
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
			// no cars[2]) all count as witnesses per §4.1.1.B.
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 810, 820, 830},
		},
		{
			name: "NOT garages[1].cars[2].name=honda",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafMultiPinNot(idx,
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
			wantDocs: []uint64{100, 200, 300, 400, 600, 700, 810, 820, 830},
		},
	}

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
