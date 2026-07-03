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

package nestedlegacy

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// L0 schema and fixtures: cars at the property root, no countries or
// garages above it. Mirrors the cars sub-schema from L2 so the same
// leaf shapes can be exercised, but with Scope at "cars" and
// parent("cars") at the property root. Doc IDs match L2's (100, 200,
// 300, 400, 810, 830) for each L0 doc that has an L2 counterpart, plus
// 850 as an L0-only addition (the spoiler-vs-205 split discriminator
// that L2 expresses via its krakow garage in doc 800).
//
// All shared infrastructure (fastPathIndex, leaf helpers, merge
// helpers, fastPathTestCase, runFastPathCases) lives in
// fast_path_test.go and is reused here unchanged — that's the whole
// point: the merge / lift logic is scope-agnostic and any helper that
// doesn't structurally require deeper nesting should behave identically
// at L0 and L2.

func l0Schema() *models.Property {
	tx := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationField,
		}
	}
	in := func(name string) *models.NestedProperty {
		return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeInt)}}
	}
	txArr := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeTextArray)},
			Tokenization: models.PropertyTokenizationField,
		}
	}
	txWord := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	txArrWord := func(name string) *models.NestedProperty {
		return &models.NestedProperty{
			Name: name, DataType: []string{string(schema.DataTypeTextArray)},
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	objArr := func(name string, nested ...*models.NestedProperty) *models.NestedProperty {
		return &models.NestedProperty{
			Name:             name,
			DataType:         []string{string(schema.DataTypeObjectArray)},
			NestedProperties: nested,
		}
	}
	return &models.Property{
		Name:     "cars",
		DataType: []string{string(schema.DataTypeObjectArray)},
		NestedProperties: []*models.NestedProperty{
			in("year"),
			tx("make"),
			tx("model"),
			tx("name"),
			txArr("colors"),
			txWord("description"),
			txArrWord("tags"),
			objArr("accessories", tx("type")),
			objArr("tires", in("width")),
			objArr("doors", in("count")),
		},
	}
}

// l0Docs returns the L0 fixtures, mirroring L2's car structures
// directly (stripping the garages wrapper that L2 uses to nest cars
// underneath). Doc IDs match L2's so a reader can cross-reference a
// behaviour observed at L0 with the same fixture shape at L2. Docs
// that are inherently L2-only (those that exercise garages-level
// predicates, multi-garage shapes, or multi-country roots) are not
// represented here. Doc 850 is an L0-only addition (no L2 counterpart)
// that carries the spoiler-vs-205 split discriminator L2 expresses via
// its krakow garage in doc 800.
func l0Docs() map[uint64]any {
	return map[uint64]any{
		// 100: three cars with full data. cars[0] is the same-car
		// witness for `spoiler AND tires=205` and `year=2020 AND
		// tires=205`. cars[1] supplies the radio/195/blue+red shape
		// used by ContainsAll on colors. cars[2] is minimal (year+name
		// only) — useful for NOT/IS-NULL discriminators. Mirrors L2
		// doc 100 with the garages wrapper stripped.
		100: []any{
			map[string]any{"year": 2020, "make": "toyota", "colors": []any{"blue"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
			map[string]any{"year": 2018, "make": "honda", "colors": []any{"blue", "red"}, "accessories": []any{map[string]any{"type": "radio"}}, "tires": []any{map[string]any{"width": 195}}},
			map[string]any{"year": 2017, "name": "honda"},
		},
		// 200: cars[0] and cars[1] both honda + spoiler + 205; cars[1]
		// is the unique car with year=2020 + spoiler + 205 + honda all
		// at the same element — the 3-way same-element andN witness.
		// cars[2] minimal (name=ford). Mirrors L2 doc 200.
		200: []any{
			map[string]any{"year": 2015, "make": "honda", "colors": []any{"red"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
			map[string]any{"year": 2020, "make": "honda", "colors": []any{"blue"}, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 205}}},
			map[string]any{"name": "ford"},
		},
		// 300: two cars, cars[1] missing year — the IS NULL true /
		// missing-scalar discriminator. cars[0]=ford+2011. Mirrors L2
		// doc 300.
		300: []any{
			map[string]any{"year": 2011, "make": "ford", "colors": []any{"green"}},
			map[string]any{"make": "honda", "colors": []any{"red"}},
		},
		// 400: single-car doc. cars[0]=ford+2012+yellow. Mirrors L2
		// doc 400. Pre-Phase ContainsAny [bmw, kia] fail discriminator
		// (no bmw/kia anywhere).
		400: []any{
			map[string]any{"year": 2012, "make": "ford", "colors": []any{"yellow"}},
		},
		// 810: every car has year=2020 — forces NOT cars.year=2020 to
		// FAIL at doc level (without this, every other fixture had at
		// least one car with year != 2020 or missing year, so the
		// unpinned NOT helper would pass even if subtraction were a
		// no-op). Mirrors L2 doc 810.
		810: []any{
			map[string]any{"year": 2020, "make": "honda"},
			map[string]any{"year": 2020, "make": "toyota"},
		},
		// 830: car with multiple accessories (radio at [0], spoiler at
		// [1]). At L2 this validates walker positions for non-first
		// array elements; at L0 (no pinned tests yet) it serves as the
		// "car with no year/make/colors, only accessories" witness for
		// IS NULL true on year and for ContainsNone/NOT-ContainsAny.
		// Mirrors L2 doc 830.
		830: []any{
			map[string]any{"accessories": []any{
				map[string]any{"type": "radio"},
				map[string]any{"type": "spoiler"},
			}},
		},
		// 850: L0-only split discriminator. cars[0] has spoiler but no
		// 205-tires; cars[1] has 205-tires but no spoiler. Sibling AND
		// `spoiler AND tires.width=205` must FAIL here (no same-car
		// witness), while sibling OR must PASS. This is the same
		// shape as the L2 krakow garage in doc 800, just flattened to
		// the property root. Year/make picked to not collide with
		// existing predicates (2019, no make).
		850: []any{
			map[string]any{"year": 2019, "accessories": []any{map[string]any{"type": "spoiler"}}, "tires": []any{map[string]any{"width": 195}}},
			map[string]any{"year": 2019, "accessories": []any{map[string]any{"type": "radio"}}, "tires": []any{map[string]any{"width": 205}}},
		},
		// 800: doors fixture mirroring the L2 doc 800 warsaw cars[0]
		// shape (without the garages wrapper). cars[0] has spoiler,
		// 205-tires, and doors=4 colocated in the same car. The
		// unique witness for `(spoiler AND 205) AND doors=4` and the
		// 3-leaf andN(spoiler, 205, doors=4). No year/make/colors —
		// so it adds witness coverage to negation-style tests but
		// doesn't contribute to year/make/colors positive leaves.
		800: []any{
			map[string]any{
				"accessories": []any{map[string]any{"type": "spoiler"}},
				"tires":       []any{map[string]any{"width": 205}},
				"doors":       []any{map[string]any{"count": 4}},
			},
		},
		// 700: empty cars array — doc exists in the universe but has
		// no nested data under the property. Discriminator for
		// `cars IS NULL true`, `NOT cars[1].year=2020` (missing-pin
		// path), and the pinned-root NOT/IS NULL true family. Without
		// this fixture those tests had no doc that satisfied "missing
		// at every level" and `cars IS NULL true` collapsed to ∅.
		// Mirrors L2 doc 700 (the country with a garage but cars=null).
		700: []any{},
	}
}

// buildL0 ingests every L0 fixture into a fresh index. The property
// name is "cars" so leaf paths look like "cars.year", "cars.make", etc.
func buildL0(t *testing.T) *fastPathIndex {
	t.Helper()
	prop := l0Schema()
	idx := newFastPathIndex("cars")
	for docID, value := range l0Docs() {
		idx.addDoc(t, prop, docID, value)
	}
	return idx
}

// TestFastPathL0_Merge exercises merge helpers against the L0 schema
// (cars at the property root). The intent is to confirm that the same
// merge / lift logic that works for the deeply-nested L2 schema produces
// the right results when Scope is closer to the root — same code
// path, different scope depth.
//
// Other L2 merge shapes (sibling AND, OR, NOT, ContainsNone, etc.) can
// be ported here incrementally; some won't be applicable (no positional
// pins on root, no ancestor-child merges since cars has no parent
// array), but every shape that doesn't structurally require deeper
// nesting should behave identically.
func TestFastPathL0_Merge(t *testing.T) {
	idx := buildL0(t)

	cases := []fastPathTestCase{
		// Single-leaf positive at the property root. Mirrors the L2
		// `cars.year=2020` single-leaf case — same construction, just
		// with Scope at depth 1 (= propName) instead of depth 3. The
		// harness's clean-range walk stops at pathRoot, never reading
		// anchor above propName.
		{
			name: "cars.year=2020 [L0, single leaf]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "cars.year", 2020)
			},
			wantScope:   "cars",
			wantCeiling: pathRoot, // propName — clean to property root
			// 100 cars[0]=2020 PASS. 200 cars[1]=2020 PASS. 810 cars[0,1]=
			// 2020 PASS. Others: no car has year=2020 → FAIL.
			wantDocs: []uint64{100, 200, 810},
		},
		// Same-Scope AND at L0. Scope=cars (the property root). Same-
		// element correlation comes from chain-bit intersection at
		// car-self, regardless of the surrounding schema depth.
		{
			name: "cars.make=honda AND cars.year=2020 [L0, same-element]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020))
			},
			wantScope: "cars",
			// Same-Scope AND: structural ceiling = Scope, so Ceiling=cars.
			// No clean claim above the merge scope.
			wantCeiling: "cars",
			// doc 100: cars[0]=toyota+2020, cars[1]=honda+2018 — split,
			// no same-car witness → FAIL.
			// doc 200 cars[1]=honda+2020 → same-car PASS.
			// doc 810 cars[0]=honda+2020 → same-car PASS.
			// Others: no same-car honda+2020 → FAIL.
			wantDocs: []uint64{200, 810},
		},
		// Same-Scope OR. Both positive leaves at Scope=cars with
		// Ceiling=pathRoot. No-op lift at the LCA preserves the
		// Ceilings, so result Ceiling = deepestPath(pathRoot, pathRoot)
		// = pathRoot. Owner-level semantic: doc satisfies the OR iff
		// some car matches honda OR some car matches 2020 (possibly
		// different cars — no same-car correlation).
		{
			name: "cars.make=honda OR cars.year=2020 [L0, same-Scope union]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020))
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// doc 100: cars[0]=2020 OR cars[1]=honda → PASS.
			// doc 200: cars[0,1] honda → PASS.
			// doc 300: cars[1]=honda → PASS.
			// doc 400: ford+2012 → FAIL.
			// doc 810: both predicates fire → PASS.
			// doc 830/850: no honda no 2020 → FAIL.
			wantDocs: []uint64{100, 200, 300, 810},
		},
		// Single-leaf NOT at L0. negate(leafPositive) collapses Ceiling
		// to Scope — Raw = _exists(Scope) AndNot Witnesses carries chain bits from
		// every doc that has any car, regardless of whether a NON-matching
		// witness exists. The boundary case at L0 is that Scope=propName, so
		// no "above Ceiling" assertion is even attempted in the harness loop.
		// Predicate: "doc has at least one car where year != 2020".
		{
			name: "NOT cars.year=2020 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "cars.year", 2020)
			},
			wantScope:   "cars",
			wantCeiling: "cars", // negate rule: Ceiling collapses to Scope
			// Per-car: car is witness iff year != 2020 (cars without
			// year set also count — they aren't in the value bucket).
			// doc 100 cars[1,2]: year=2018/2017 → witnesses PASS.
			// doc 200 cars[0]=2015, cars[2] no year → witnesses PASS.
			// doc 300 cars[0,1] both witnesses PASS.
			// doc 400 cars[0]=2012 → witness PASS.
			// doc 810: every car year=2020 → no witness FAIL.
			// doc 830 cars[0] no year → witness PASS.
			// doc 850 cars[0,1]=2019 → witnesses PASS.
			wantDocs: []uint64{100, 200, 300, 400, 800, 830, 850},
		},
		// Ancestor+child AND at L0. Scope pairing: cars.year
		// (Scope=cars = propName) AND cars.tires.width
		// (Scope=cars.tires, one below). The broadcasting branch in
		// andLeaves fires because the ancestor (positive leaf) has
		// Ceiling=pathRoot (depth 0), strictly shallower than its own
		// Scope=cars (depth 1) — so ancestor.ceilingAboveScope() is true.
		// Positive leaves earn this by construction: the elementPositions
		// encoding gives them authentic descendants below Scope, so the
		// ancestor's Raw at child.Scope is a real per-element descendant
		// of matching cars. Intersecting with child.Raw at child.Scope
		// gives same-car correlation. Merge at child.Scope=cars.tires,
		// ceiling=ancestor.Scope=cars. Mirrors the L2 ancestor+child
		// behavior — same depth-preservation, just one tree level
		// shallower.
		{
			name: "cars.year=2020 AND cars.tires.width=205 [L0, ancestor+child]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.year", 2020),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars.tires",
			wantCeiling: "cars",
			// Per-car same-car: year=2020 AND tires.width=205 colocated.
			// doc 100 cars[0]=2020+205 → SAME-CAR PASS.
			// doc 200 cars[1]=2020+205 → SAME-CAR PASS.
			// doc 810 cars have year but no tires → FAIL.
			// doc 850 cars[1]=205 but year=2019 → no same-car → FAIL.
			// Others: no → FAIL.
			wantDocs: []uint64{100, 200},
		},
		// Ancestor+child OR. Mirrors L2's `garages.city=warsaw OR
		// cars.make=ford` shape, just one tree level shallower.
		// commonScope=cars (the ancestor's Scope). orLeaves lifts the
		// deeper operand (tires.width, Ceiling=pathRoot) up to cars via
		// cheap-lift — depth(tires.Ceiling=pathRoot)=0 ≤ depth(cars)=1,
		// so Raw is reused unchanged and the operand's Ceiling=pathRoot
		// is preserved. The shallower operand (year) is already at
		// LCA=cars, no-op lift, Ceiling=pathRoot preserved. OR result
		// Ceiling = deepestPath(pathRoot, pathRoot) = pathRoot — both
		// operands are fully clean, the union stays fully clean.
		{
			name: "cars.year=2020 OR cars.tires.width=205 [L0, ancestor+child OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "cars.year", 2020),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// doc 100: 2020 OR 205 → PASS.
			// doc 200: cars[1]=2020 OR cars[0,1]=205 → PASS.
			// doc 810: cars[0,1]=2020 → PASS.
			// doc 850 cars[1]=205 → PASS via tires.
			// Others: no → FAIL.
			wantDocs: []uint64{100, 200, 800, 810, 850},
		},
		// ContainsAny at L0. Positive composite — union of value
		// buckets. Same shape as a single positive leaf but the value
		// set has multiple elements. Result Scope=cars (parent of the
		// value path), Ceiling=pathRoot (union of fully-authentic
		// per-value buckets is itself fully authentic — every bit still
		// traces to a real matching element's elementPositions encoding).
		{
			name: "cars.make ContainsAny [honda, ford] [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "cars.make", "honda", "ford")
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// doc 100 cars[1]=honda PASS. doc 200 cars[0,1]=honda PASS.
			// doc 300 cars[0]=ford or cars[1]=honda PASS. doc 400 ford
			// PASS. doc 810 honda PASS. doc 830/850 no make → FAIL.
			wantDocs: []uint64{100, 200, 300, 400, 810},
		},
		// Sibling AND at L0 (under cars). Mirrors L2's
		// `cars.accessories.type=spoiler AND cars.tires.width=205`
		// shape. Both operands are siblings under cars (different
		// nested arrays inside cars). orLeaves… err, andLeaves picks
		// the siblings branch: lift both to LCA=cars (cheap, both
		// have Ceiling=propName=cars), then recurse as same-Scope
		// AND. Same-Scope AND produces Ceiling=Scope=cars (ceiling
		// rule).
		{
			name: "cars.accessories.type=spoiler AND cars.tires.width=205 [L0, siblings]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Per-car: same car has spoiler accessory AND tires.width=205.
			// doc 100 cars[0] spoiler+205 → PASS.
			// doc 200 cars[0,1] both spoiler+205 → PASS.
			// doc 830 cars[0] spoiler but no tires → FAIL.
			// doc 850 cars[0]=spoiler+195, cars[1]=radio+205 — split,
			// no same-car witness → FAIL (the split discriminator).
			// Others: no spoiler → FAIL.
			wantDocs: []uint64{100, 200, 800},
		},
		// IS NULL false on a scalar leaf (cars.year). Symmetric with
		// the single-leaf positive case: Scope=cars, Ceiling=pathRoot.
		// The leaf fires for every car with a year value set, regardless
		// of the value.
		{
			name: "cars.year IS NULL false [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "cars.year")
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// At least one car with year set:
			// doc 100/200/300/400/810/850 each have ≥1 car with year → PASS.
			// doc 830 only car has no year → FAIL.
			wantDocs: []uint64{100, 200, 300, 400, 810, 850},
		},
		// IS NULL true on a scalar leaf — `negate(leafIsNullFalse)`.
		// Per-car: "this car has no year value". Doc passes if any
		// car lacks year.
		{
			name: "cars.year IS NULL true [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "cars.year")
			},
			wantScope:   "cars",
			wantCeiling: "cars", // negate collapses Ceiling to Scope
			// doc 200 cars[2]=name=ford (no year) → witness PASS.
			// doc 300 cars[1]=honda (no year) → witness PASS.
			// doc 830 cars[0] has only accessories, no year → witness PASS.
			// Every other doc: every car has year set → no witness.
			wantDocs: []uint64{200, 300, 800, 830},
		},
		// Sibling OR at L0 (under cars). Mirrors L2's
		// `cars.accessories.type=spoiler OR cars.tires.width=205`.
		// Both operands siblings under cars (different nested arrays).
		// orLeaves lifts both to LCA=cars via cheap-lift (each operand's
		// Ceiling=pathRoot has depth 0 ≤ depth(cars)=1, so Raw is
		// reused and Ceiling preserved). Result.Ceiling = deepestPath
		// (pathRoot, pathRoot) = pathRoot — both operands fully clean,
		// union stays fully clean.
		{
			name: "cars.accessories.type=spoiler OR cars.tires.width=205 [L0, siblings OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// doc 100 cars[0] both → PASS.
			// doc 200 cars[0,1] both → PASS.
			// doc 830 cars[0] spoiler → PASS.
			// doc 850 cars[0] spoiler OR cars[1] 205 → PASS (OR doesn't
			// require same-car, unlike the AND counterpart above).
			// Others: no spoiler, no 205 → FAIL.
			wantDocs: []uint64{100, 200, 800, 830, 850},
		},
		// Compound NOT (NOT-of-OR) at L0. Inner same-Scope OR over two
		// positive leaves keeps Ceiling at the shallower of the two
		// operands' Ceilings (pathRoot in both cases) → inner.Ceiling=
		// pathRoot. negate then collapses Ceiling to the inner's Scope
		// (cars), producing Scope=cars, Ceiling=cars. Per-car: a car is a
		// witness for the negation iff it is NOT in the OR's positive
		// Witnesses — i.e. not honda AND year≠2020 (or year missing).
		{
			name: "NOT (cars.make=honda OR cars.year=2020) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020)))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Per-car: car is witness iff NOT in OR Witnesses = make ≠ honda
			// AND year ≠ 2020.
			// doc 100 cars[0]=toyota+2020 → 2020 in OR → no. cars[1]=
			// honda+2018 → honda in OR → no. cars[2]=year=2017+name=honda
			// (name is NOT make; make undefined → make=honda doesn't
			// fire; year=2017 ≠ 2020) → witness PASS.
			// doc 200 cars[2]=name=ford (no make=honda, no year=2020)
			// → witness PASS.
			// doc 300 cars[0]=ford+2011 → witness PASS.
			// doc 400 cars[0]=ford+2012 → witness PASS.
			// doc 810 all cars honda+2020 → NO witness FAIL.
			// doc 830 cars[0] no make/year → witness PASS.
			// doc 850 cars[0,1]=2019 (no make) → witnesses PASS.
			wantDocs: []uint64{100, 200, 300, 400, 800, 830, 850},
		},
		// De Morgan equivalent of NOT-of-OR: NOT(A) AND NOT(B).
		// Two NOT operands at same Scope=cars, AND-ed. Same-Scope AND.
		// Per-car: car is witness iff NOT honda AND NOT 2020 (same-
		// element). Should produce IDENTICAL wantDocs as the NOT-of-OR
		// case above — that's the De Morgan invariant.
		{
			name: "NOT(cars.make=honda) AND NOT(cars.year=2020) [L0, De Morgan]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "cars.make", "honda"),
					leafNot(idx, "cars.year", 2020))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Must match NOT-of-OR above (per-element AND of negations
			// at same Scope is the De Morgan dual).
			wantDocs: []uint64{100, 200, 300, 400, 800, 830, 850},
		},
		// ContainsNone direct. Per-car: car is a witness iff its make
		// is NEITHER honda NOR ford (cars with no make field also
		// count — they aren't in any value bucket, so AndNot leaves
		// them in the universe).
		{
			name: "cars.make ContainsNone [honda, ford] [L0, direct]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsNone(idx, "cars.make", "honda", "ford")
			},
			wantScope:   "cars",
			wantCeiling: "cars", // negation shape — no claim above Scope
			// doc 100 cars[0]=toyota → witness PASS.
			// doc 200 cars[2]=name=ford (no make field) → witness PASS.
			// doc 300 cars[0]=ford, cars[1]=honda → no witness FAIL.
			// doc 400 cars[0]=ford → no witness FAIL.
			// doc 810 cars[1]=toyota → witness PASS.
			// doc 830 cars[0] no make → witness PASS.
			// doc 850 cars[0,1] no make → witnesses PASS.
			wantDocs: []uint64{100, 200, 800, 810, 830, 850},
		},
		// NOT(ContainsAny) equivalence: should produce identical docs
		// to the direct ContainsNone above. Both routes negate the
		// same positive Witnesses (cars with make ∈ {honda, ford}) at the
		// same Scope.
		{
			name: "NOT (cars.make ContainsAny [honda, ford]) [L0, via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafContainsAny(idx, "cars.make", "honda", "ford"))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Must match `ContainsNone direct` above.
			wantDocs: []uint64{100, 200, 800, 810, 830, 850},
		},
		// Compound NOT-of-AND. Inner same-element AND has Witnesses = cars
		// satisfying honda AND 2020 in the same car. negate excludes
		// those, leaves every other car as a witness.
		{
			name: "NOT (cars.make=honda AND cars.year=2020) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020)))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Per-car: car is witness iff NOT (honda AND 2020 same-
			// element) = NOT honda OR NOT 2020. Only docs where every
			// car satisfies honda AND 2020 in the same element FAIL.
			// doc 810: cars[0]=honda+2020 → in inner.Witnesses; cars[1]=
			// toyota+2020 → not honda → WITNESS → PASS.
			// Every other doc also has at least one such car.
			wantDocs: []uint64{100, 200, 300, 400, 800, 810, 830, 850},
		},
		// De Morgan dual of NOT-of-AND: NOT(A) OR NOT(B). Two NOT
		// operands at same Scope, OR-ed. Per-car: NOT honda OR NOT
		// 2020 = NOT (honda AND 2020). Should match the NOT-of-AND
		// case above.
		{
			name: "NOT(cars.make=honda) OR NOT(cars.year=2020) [L0, De Morgan]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafNot(idx, "cars.make", "honda"),
					leafNot(idx, "cars.year", 2020))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			wantDocs:    []uint64{100, 200, 300, 400, 800, 810, 830, 850},
		},
		// 3-leaf andN — same-element correlation across cars and
		// cars.tires. honda + 2020 + tires.width=205. Doc 200 cars[1]
		// (honda+2020+205 same car) is the unique witness.
		{
			name: "andN(cars.make=honda, cars.year=2020, cars.tires.width=205) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020),
					leafPositive(idx, "cars.tires.width", 205))
			},
			// andN sorts deepest-first: tires (cars.tires) precedes
			// honda and year (cars). First merge (tires AND honda)
			// fires the broadcasting branch: honda is a positive leaf
			// with Ceiling=pathRoot (depth 0) strictly shallower than
			// its Scope=cars (depth 1), so honda.ceilingAboveScope() is
			// true. Result lands at child.Scope=cars.tires with
			// ceiling=ancestor.Scope=cars. Then merge with year: year
			// is also ancestor (positive leaf, ceilingAboveScope), so
			// broadcasting fires again. Final result stays at cars.tires.
			wantScope:   "cars.tires",
			wantCeiling: "cars",
			// doc 200 cars[1]=honda+2020+205 same-car → PASS.
			// doc 100 cars[0]=toyota+2020+205 (no honda).
			// doc 810 honda+2020 but no tires. doc 850 has 205 but not
			// honda+2020 same-car. → all FAIL.
			wantDocs: []uint64{200},
		},
		// 3-leaf orN. Per-car owner-level: doc passes iff at least
		// one car matches any of the three predicates.
		{
			name: "orN(cars.make=honda, cars.year=2020, cars.tires.width=205) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020),
					leafPositive(idx, "cars.tires.width", 205))
			},
			// orN lifts all to LCA=cars (cheap, all Ceiling=pathRoot
			// already at or above cars). OR at cars. Result.Ceiling=
			// pathRoot — union of fully-clean operands stays fully clean.
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// 100 has all three. 200 has all three. 300 cars[1]=honda
			// → PASS. 400 (ford+2012) → FAIL. 810 honda+2020 → PASS.
			// 830 no → FAIL. 850 cars[1]=205 → PASS.
			wantDocs: []uint64{100, 200, 300, 800, 810, 850},
		},
		// ContainsAll on a scalar array (cars.colors text[]). Per-car:
		// the car's colors array must contain EVERY listed value (red
		// AND blue both present in this car's colors). leafContainsAll
		// intersects the per-value buckets at the cars scope — only
		// car selfMarkers that appear in both buckets survive, i.e.
		// cars whose elementPositions include both red and blue
		// emissions (which happens iff the same car array fires both
		// values). Same-array semantic, NOT cross-car owner-level.
		{
			name: "cars.colors ContainsAll [red, blue] [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAll(idx, "cars.colors", "red", "blue")
			},
			wantScope:   "cars",
			wantCeiling: "cars", // same-Scope intersection rule
			// doc 100 cars[1].colors=[blue, red] → same-array witness PASS.
			// doc 200 cars[0]=[red], cars[1]=[blue] → split, FAIL —
			// the split discriminator.
			// Others: no car has both red and blue → FAIL.
			wantDocs: []uint64{100},
		},
		// Single positive leaf on a text[] (cars.colors=red). Per-car:
		// car's colors array contains "red".
		{
			name: "cars.colors=red [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "cars.colors", "red")
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// doc 100 cars[1]=[blue,red] PASS. doc 200 cars[0]=[red] PASS.
			// doc 300 cars[1]=[red] PASS. Others: no red → FAIL.
			wantDocs: []uint64{100, 200, 300},
		},
		// NOT cars.colors=red. Per-car: car has no "red" in colors
		// (cars without any colors set count as witnesses — they're
		// in anchor[cars] but not in the value bucket).
		{
			name: "NOT cars.colors=red [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "cars.colors", "red")
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Every doc has at least one car lacking "red" — either
			// has colors without red, or has no colors at all.
			wantDocs: []uint64{100, 200, 300, 400, 800, 810, 830, 850},
		},
		// Single positive leaf on a deeper scope (cars.accessories.type).
		// Scope = parent(path) = cars.accessories (the accessories
		// element scope). Witnesses bits live at accessories selfMarkers
		// whose type=spoiler. Doc passes if some accessory satisfies.
		{
			name: "cars.accessories.type=spoiler [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "cars.accessories.type", "spoiler")
			},
			wantScope:   "cars.accessories",
			wantCeiling: pathRoot, // propName — positive leaf
			// doc 100 cars[0].accessories[0]=spoiler PASS.
			// doc 200 cars[0,1].accessories[0]=spoiler PASS.
			// doc 830 cars[0].accessories[1]=spoiler PASS.
			// doc 850 cars[0].accessories[0]=spoiler PASS.
			// Others: no accessories → FAIL.
			wantDocs: []uint64{100, 200, 800, 830, 850},
		},
		// NOT cars.accessories.type=spoiler. Per-accessory: accessory
		// has type ≠ spoiler. Cars without ANY accessories produce
		// no per-element witness (empty scope contributes nothing).
		{
			name: "NOT cars.accessories.type=spoiler [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "cars.accessories.type", "spoiler")
			},
			wantScope:   "cars.accessories",
			wantCeiling: "cars.accessories", // negate rule
			// doc 100 cars[1].accessories[0]=radio → witness PASS.
			// doc 200: cars[0,1] only have spoiler accessories,
			// cars[2] no accessories → no per-element witness → FAIL.
			// doc 830 cars[0].accessories[0]=radio → witness PASS.
			// doc 850 cars[1].accessories[0]=radio → witness PASS.
			// 300/400/810: no accessories at all → FAIL.
			wantDocs: []uint64{100, 830, 850},
		},
		// Empty-result edge case. cars.year=9999 — value not present
		// in any fixture. Exercises the no-match path through every
		// helper invariant.
		{
			name: "cars.year=9999 (empty result) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "cars.year", 9999)
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			wantDocs:    nil,
		},
		// Same-Scope AND on a text[] field. Per-car: car's colors
		// array contains BOTH blue and red. Logically equivalent to
		// ContainsAll [blue, red] but routed through general AND.
		{
			name: "cars.colors=blue AND cars.colors=red [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.colors", "blue"),
					leafPositive(idx, "cars.colors", "red"))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Same wantDocs as ContainsAll [red, blue] — De Morgan-
			// adjacent equivalence (intersection of value buckets ==
			// per-car ContainsAll).
			wantDocs: []uint64{100},
		},
		// Mixed NOT operand in OR. NOT spoiler at cars.accessories
		// (Ceiling=cars.accessories — negate collapses Ceiling to Scope)
		// OR'd with positive 205 at cars.tires (Ceiling=pathRoot).
		// orLeaves lifts both to LCA=cars. NOT spoiler needs a real lift
		// (depth(its Ceiling=cars.accessories)=2 > depth(cars)=1), so
		// its post-lift Ceiling resets to cars; 205 cheap-lifts and
		// keeps Ceiling=pathRoot. Result.Ceiling = deepestPath(cars,
		// pathRoot) = cars — the negate side caps the cleanness at LCA.
		{
			name: "NOT cars.accessories.type=spoiler OR cars.tires.width=205 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafNot(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Per-car at cars-scope (post-lift): car has at least one
			// non-spoiler accessory OR has 205 tires.
			// 100 cars[1]=radio+195 (non-spoiler) + cars[0]=205 → PASS.
			// 200 cars[0,1]=spoiler+205 → 205 → PASS.
			// 300/400/810: no accessories no 205 → FAIL.
			// 830 cars[0]=radio,spoiler → has non-spoiler radio → PASS.
			// 850 cars[1]=radio (non-spoiler) + cars[1]=205 → PASS.
			wantDocs: []uint64{100, 200, 800, 830, 850},
		},
		// Compound AND inside OR. Inner sibling AND (spoiler+205) at
		// cars (Ceiling=cars). OR with positive ford at cars.
		// Same-Scope OR.
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) OR cars.make=ford [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "cars.accessories.type", "spoiler"),
						leafPositive(idx, "cars.tires.width", 205)),
					leafPositive(idx, "cars.make", "ford"))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Inner AND (same-car spoiler+205): {100, 200}.
			// ford-cars: {300, 400}. Union: {100, 200, 300, 400}.
			wantDocs: []uint64{100, 200, 300, 400, 800},
		},
		// Same-Scope OR inside an AND. Per-car: car has (honda OR
		// ford) make AND year=2018.
		{
			name: "(cars.make=honda OR cars.make=ford) AND cars.year=2018 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					orLeaves(idx,
						leafPositive(idx, "cars.make", "honda"),
						leafPositive(idx, "cars.make", "ford")),
					leafPositive(idx, "cars.year", 2018))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// doc 100 cars[1]=honda+2018 → SAME-CAR PASS.
			// All other docs lack a single car with both predicates.
			wantDocs: []uint64{100},
		},
		// NOT of compound sibling AND. Per-car: car is NOT in inner
		// Witnesses (= NOT (spoiler AND 205 same-car)) = NOT spoiler at this
		// car OR NOT 205 at this car. Cars not in inner Witnesses are
		// witnesses.
		{
			name: "NOT (cars.accessories.type=spoiler AND cars.tires.width=205) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205)))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Every doc has at least one car not in (spoiler AND 205
			// same-car). E.g. doc 100 cars[1]=radio+195 → not in.
			wantDocs: []uint64{100, 200, 300, 400, 810, 830, 850},
		},
		// NOT of same-Scope OR. Should give identical docs to the
		// existing `ContainsNone direct` and the De Morgan AND-of-NOTs
		// — three routes to the same per-car predicate.
		{
			name: "NOT (cars.make=honda OR cars.make=ford) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.make", "ford")))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Must match ContainsNone [honda, ford] and NOT(honda)
			// AND NOT(ford) below.
			wantDocs: []uint64{100, 200, 800, 810, 830, 850},
		},
		// NOT of sibling OR. Per-car (after lift of inner OR to cars):
		// car has NEITHER a spoiler accessory NOR a 205 tire.
		{
			name: "NOT (cars.accessories.type=spoiler OR cars.tires.width=205) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205)))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// 100 cars[1]=radio+195 → no spoiler+no 205 in cars[1].
			// Also cars[2] no accessories no tires → witness. PASS.
			// 200 cars[2]=name=ford (no accessories, no tires) → witness.
			// PASS.
			// 300/400/810: no spoiler no 205 anywhere → witnesses PASS.
			// 830 cars[0]=spoiler → no witness FAIL.
			// 850 cars[0]=spoiler, cars[1]=205 → no witness → FAIL.
			wantDocs: []uint64{100, 200, 300, 400, 810},
		},
		// De Morgan same-Scope: NOT(honda) AND NOT(ford). Per-car:
		// car has make ∉ {honda, ford} (or no make). Should match
		// `NOT (honda OR ford)` above.
		{
			name: "NOT(cars.make=honda) AND NOT(cars.make=ford) [L0, De Morgan]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "cars.make", "honda"),
					leafNot(idx, "cars.make", "ford"))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			wantDocs:    []uint64{100, 200, 800, 810, 830, 850},
		},
		// De Morgan siblings: NOT(spoiler) AND NOT(205). Per-car at
		// cars-scope (post-lift): car has at least one non-spoiler
		// accessory AND at least one non-205 tire. Strictly stronger
		// than `NOT (spoiler AND 205)` — requires per-side witness.
		{
			name: "NOT(cars.accessories.type=spoiler) AND NOT(cars.tires.width=205) [L0, De Morgan siblings]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafNot(idx, "cars.accessories.type", "spoiler"),
					leafNot(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Only doc 100 cars[1]=radio+195 has BOTH a non-spoiler
			// accessory AND a non-205 tire in the same car.
			// doc 200 cars[0,1] only have spoiler+205; cars[2] has
			// neither → empty per-element witnesses → FAIL.
			// doc 830 cars[0]=radio,spoiler → non-spoiler witness, but
			// no tires at all → FAIL.
			// doc 850 cars[0]=spoiler+195 (no non-spoiler), cars[1]=
			// radio+205 (no non-205) → no single car has both → FAIL.
			// 300/400/810: no accessories, no tires → FAIL.
			wantDocs: []uint64{100},
		},
		// 4-leaf andN — spoiler + 205 + honda + 2020 same-car. The
		// 3-leaf andN above (without spoiler/205) only passed via
		// doc 200 cars[1]; the 4-leaf with the extra constraints
		// still passes via the same car (200 cars[1] has spoiler+
		// 205+honda+2020 colocated).
		{
			name: "andN(spoiler, tires.width=205, make=honda, year=2020) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205),
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020))
			},
			// Sort: spoiler+205 at depth 2 first (siblings under
			// cars), then honda+year at depth 1. First fold collapses
			// the siblings via lift-to-LCA=cars, so the accumulator
			// lands at cars-Scope and subsequent folds stay there
			// (no deeper child to broadcast into). Unlike the 3-leaf
			// andN above where the deepest operand is the only one at
			// its depth (so broadcasting preserves cars.tires), the
			// siblings collapse this dispatch back to cars.
			wantScope:   "cars",
			wantCeiling: "cars",
			wantDocs:    []uint64{200},
		},
		// 3-leaf same-Scope orN over honda + 2018 + blue. All three
		// at Scope=cars. Result is union of positive-leaf ESs.
		{
			name: "orN(cars.make=honda, cars.year=2018, cars.colors=blue) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2018),
					leafPositive(idx, "cars.colors", "blue"))
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// honda: {100, 200, 300, 810}. year=2018: {100} (cars[1]).
			// blue: {100, 200}. Union: {100, 200, 300, 810}.
			wantDocs: []uint64{100, 200, 300, 810},
		},
		// Mixed positive+NOT siblings AND. radio at cars.accessories
		// (positive, Ceiling=pathRoot) AND NOT 205 at cars.tires (NOT,
		// Ceiling=cars.tires). Lift both to LCA=cars. radio cheap-lifts
		// and keeps Ceiling=pathRoot; the NOT real-lifts (its Ceiling
		// was deeper than LCA) and resets to Ceiling=cars. Same-Scope
		// AND with ceiling=cars → result.Ceiling=cars.
		{
			name: "cars.accessories.type=radio AND NOT cars.tires.width=205 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "radio"),
					leafNot(idx, "cars.tires.width", 205))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			// Per-car: car has radio accessory AND has non-205 tire.
			// doc 100 cars[1]=radio+195 → PASS.
			// doc 200 cars[0,1] only spoiler → no radio → FAIL.
			// doc 830 cars[0]=radio,spoiler but no tires → FAIL.
			// doc 850 cars[0]=spoiler (no radio), cars[1]=radio+205
			// (radio but only 205) → no single car with both → FAIL.
			// 300/400/810: no accessories no tires → FAIL.
			wantDocs: []uint64{100},
		},
		// ContainsAny on a value set that has no hits in any fixture.
		// Verifies the empty-result path through ContainsAny's union
		// of (potentially-nil) value buckets.
		{
			name: "cars.make ContainsAny [bmw, kia] [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "cars.make", "bmw", "kia")
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			wantDocs:    nil,
		},
		// 3-leaf same-Scope siblings AND through a compound. Inner
		// sibling AND at cars (spoiler+205) collapses both leaves to
		// LCA=cars; the inner result has Scope=cars and Ceiling=cars
		// (same-Scope AND collapses Ceiling to the merge scope — the
		// Raw has bits only at cars-self, not at cars.accessories /
		// cars.tires / cars.doors selfMarkers). The outer AND with
		// doors=4 (Scope=cars.doors) therefore can't fire the
		// broadcasting branch: ancestor.Ceiling=cars equals
		// ancestor.Scope=cars, so ceilingAboveScope() is false. It falls
		// through to the cheap-merge branch (child.Ceiling=pathRoot is
		// shallower than A.Scope=cars) that merges at the ancestor's
		// Scope (= cars) without lift. Per-car: same car must have all
		// three. Only doc 800 cars[0] has spoiler+205+doors=4.
		{
			name: "(cars.accessories.type=spoiler AND cars.tires.width=205) AND cars.doors.count=4 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					andLeaves(idx,
						leafPositive(idx, "cars.accessories.type", "spoiler"),
						leafPositive(idx, "cars.tires.width", 205)),
					leafPositive(idx, "cars.doors.count", 4))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			wantDocs:    []uint64{800},
		},
		// 3-leaf siblings andN under cars. Same semantic as the
		// parenthesized AND above. andN sorts deepest-first; all three
		// operands are at depth 2 (cars.accessories, cars.tires,
		// cars.doors). Stable-sort keeps input order; first fold
		// collapses spoiler+205 via lift-to-LCA=cars (siblings — the
		// resulting same-Scope AND sets Ceiling=cars). Second fold is
		// ancestor+child (cars vs cars.doors) but with ancestor.Ceiling
		// =cars equal to ancestor.Scope=cars (ceilingAboveScope=false),
		// the broadcasting branch is skipped — falls through to the
		// cheap-merge branch and lands at cars.
		{
			name: "andN(spoiler, tires.width=205, doors.count=4) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andN(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205),
					leafPositive(idx, "cars.doors.count", 4))
			},
			wantScope:   "cars",
			wantCeiling: "cars",
			wantDocs:    []uint64{800},
		},
		// 3-leaf siblings orN under cars. Per-car: car has spoiler OR
		// 205 OR doors=4 (anywhere). Union of the three positive ESs
		// at cars-scope (after cheap lifts that preserve Ceiling).
		{
			name: "orN(spoiler, tires.width=205, doors.count=4) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orN(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205),
					leafPositive(idx, "cars.doors.count", 4))
			},
			wantScope:   "cars",
			wantCeiling: pathRoot,
			// spoiler: {100, 200, 800, 830, 850}.
			// 205: {100, 200, 800, 850}.
			// doors=4: {800}.
			// Union: {100, 200, 800, 830, 850}.
			wantDocs: []uint64{100, 200, 800, 830, 850},
		},
		// Pinned-root positive at L0 — outermost (only) pin sits at the
		// property root array. scope resolves to pathRoot via
		// parentPath("cars") = pathRoot; pinnedFromValueSet's MaskRootLeaf
		// branch projects matching cars[1] selfMarkers directly to
		// docIDs (= Encode(0, 0, docID) under the position layout).
		{
			name: "cars[1].year=2020 [L0, pinned root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx, "cars.year", 2020,
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 200 cars[1]=2020+honda → PASS.
			// doc 810 cars[1]=2020+toyota → PASS.
			// doc 100 cars[1]=2018 → FAIL. doc 300 cars[1] no year → FAIL.
			// doc 850 cars[1]=2019 → FAIL. doc 400/830/800 no cars[1] → FAIL.
			wantDocs: []uint64{200, 810},
		},
		{
			name: "cars[0].year=2020 [L0, pinned root slot 0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx, "cars.year", 2020,
					[]pinSpec{{"cars", 0}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 100 cars[0]=2020 → PASS. doc 810 cars[0]=2020 → PASS.
			// Others: not 2020 at slot 0 or no cars[0].
			wantDocs: []uint64{100, 810},
		},
		// Pinned-root IS NULL false — leafPinnedIsNullFalse routes through
		// pinnedFromValueSet just like the positive case, reading exists
		// instead of a value bucket. Scope=pathRoot, Ceiling=pathRoot.
		{
			name: "cars[1].year IS NULL false [L0, pinned root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx, "cars.year",
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 100 cars[1] has year → PASS.
			// doc 200 cars[1] has year → PASS.
			// doc 810 cars[1] has year → PASS.
			// doc 850 cars[1] has year → PASS.
			// doc 300 cars[1] no year → FAIL.
			// doc 400/830/800: no cars[1] → FAIL.
			wantDocs: []uint64{100, 200, 810, 850},
		},
		// Pinned-root IS NULL true — negate(pinned IS NULL false). With
		// pos.Scope = pathRoot, negate's universe is anchor[pathRoot]
		// (every ingested doc) — populated by addDoc alongside exists.
		{
			name: "cars[1].year IS NULL true [L0, pinned root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx, "cars.year",
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// All docs not in cars[1].year IS NULL false's witnesses:
			// {300, 400, 700, 800, 830} — cars[1] either lacks year or
			// doesn't exist (doc 700 has empty cars). Missing-pin slot
			// counts as owner-level success here (no pin-gap so
			// leafPinnedIsNullTrue takes the negate path).
			wantDocs: []uint64{300, 400, 700, 800, 830},
		},
		// Pinned-root NOT — negate(pinned positive). Same shape as IS NULL
		// true above, just over a specific value bucket instead of exists.
		{
			name: "NOT cars[1].year=2020 [L0, pinned root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx, "cars.year", 2020,
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// Anchor[pathRoot] AndNot pos.Witnesses. pos.Witnesses =
			// {200, 810}. Universe = all ingested docs = {100, 200, 300,
			// 400, 700, 800, 810, 830, 850}. Difference = {100, 300, 400,
			// 700, 800, 830, 850}.
			wantDocs: []uint64{100, 300, 400, 700, 800, 830, 850},
		},
		// Property-root IS NULL false — the object-array `cars` itself,
		// not a scalar leaf below it. leafIsNullFalse's pathRoot branch
		// uses MaskRootLeaf on idx.exists["cars"] because cars-self bits
		// aren't in anchor[pathRoot]. Every L0 fixture has at least one
		// car, so the result is the full universe.
		{
			name: "cars IS NULL false [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "cars")
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			wantDocs:    []uint64{100, 200, 300, 400, 800, 810, 830, 850},
		},
		// Property-root IS NULL true — negate. Only doc 700 has an empty
		// cars array (no nested data), so it's the sole witness.
		{
			name: "cars IS NULL true [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullTrue(idx, "cars")
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			wantDocs:    []uint64{700},
		},
		// OR across pathRoot and propName-Scope. Left has Scope=pathRoot
		// (IS NULL true on cars — empty here), right has Scope=cars
		// (positive leaf). commonScope(pathRoot, "cars") = pathRoot, so
		// the right operand is lifted via liftToScope's pathRoot branch
		// (MaskRootLeaf on its Witnesses). Result Scope=pathRoot,
		// Ceiling preserved at pathRoot via the cheap-lift convention.
		{
			name: "cars IS NULL true OR cars.make=honda [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafIsNullTrue(idx, "cars"),
					leafPositive(idx, "cars.make", "honda"))
			},
			wantScope: pathRoot,
			// Left operand had Ceiling=pathRoot (negate at pathRoot),
			// right had Ceiling=pathRoot (positive). Lift preserves
			// right's Ceiling. deepestPath(pathRoot, pathRoot) = pathRoot.
			wantCeiling: pathRoot,
			// IS NULL true = {700}. cars.make=honda witnesses = {100, 200, 300, 810}.
			// Union = {100, 200, 300, 700, 810}.
			wantDocs: []uint64{100, 200, 300, 700, 810},
		},
		// AND across pathRoot (ancestor) and propName-Scope (descendant).
		// ancestor=NOT cars[1].year=2020 (Scope=pathRoot, Ceiling=pathRoot).
		// ancestor.ceilingAboveScope is false (depths equal), so the
		// broadcasting branch doesn't fire. Falls through to the second
		// ancestor+child branch: depth(C.Ceiling=pathRoot)=0 <= depth
		// (A.Scope=pathRoot)=0 → direct merge at A.Scope=pathRoot, no lift.
		// liftToScope sends the child (cars.make=honda) through the
		// pathRoot branch to add doc-level bits to its Raw, then the
		// AND intersection at pathRoot gives the doc-level conjunction.
		{
			name: "NOT cars[1].year=2020 AND cars.make=honda [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 1}}),
					leafPositive(idx, "cars.make", "honda"))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// NOT cars[1].year=2020 witnesses = {100, 300, 400, 800, 830, 850}.
			// cars.make=honda witnesses = {100, 200, 300, 810}.
			// Intersection = {100, 300}.
			wantDocs: []uint64{100, 300},
		},
		// Same-scope pinned-root AND — both operands at Scope=pathRoot,
		// no ancestor+child dispatch. andLeaves hits the same-Scope
		// branch directly. Per-doc semantic: both cars[0].year=2020 AND
		// cars[1].year=2020 must hold at the doc level.
		{
			name: "cars[0].year=2020 AND cars[1].year=2020 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedPositive(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 0}}),
					leafPinnedPositive(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 1}}))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// cars[0]=2020: {100, 810}. cars[1]=2020: {200, 810}.
			// Intersection: {810}.
			wantDocs: []uint64{810},
		},
		// Same-Scope pinned-root AND mixing NOT and positive operand
		// shapes. Both operands have Scope=pathRoot, so andLeaves hits
		// the same-Scope branch and andAtScope intersects their
		// doc-level Bitmaps directly. Different pin slots (cars[0] vs
		// cars[1]) discriminate from the same-slot case — a doc must
		// independently satisfy "cars[0].year != 2020" AND
		// "cars[1].make=honda", which is owner-level conjunction at the
		// doc level (no per-element correlation possible at pathRoot —
		// there's no element scope shared between the two operands).
		{
			name: "NOT cars[0].year=2020 AND cars[1].make=honda [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 0}}),
					leafPinnedPositive(idx, "cars.make", "honda",
						[]pinSpec{{"cars", 1}}))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// NOT cars[0].year=2020 witnesses = docUniverse AndNot
			// {100, 810} = {200, 300, 400, 800, 830, 850}.
			// cars[1].make=honda witnesses = {100, 200, 300}.
			// Intersection = {200, 300}.
			// doc 200: cars[0]=honda+2015 (NOT 2020 ✓), cars[1]=honda ✓.
			// doc 300: cars[0]=ford+2011 (NOT ✓), cars[1]=honda ✓.
			wantDocs: []uint64{200, 300},
		},
		// Pinned-root NOT OR-ed with a non-pinned same-element compound.
		// Left has Scope=pathRoot (pinned NOT). Right has Scope=cars
		// (inner same-element AND at cars). orLeaves LCA = pathRoot;
		// right gets lifted through liftToScope's pathRoot branch
		// (MaskRootLeaf of its Witnesses) so its doc-level bits join
		// the union. Exercises liftToScope on a COMPOUND (not just a
		// leaf) at the pathRoot target.
		{
			name: "NOT cars[1].year=2020 OR (cars.make=honda AND cars.year=2020) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPinnedNot(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 1}}),
					andLeaves(idx,
						leafPositive(idx, "cars.make", "honda"),
						leafPositive(idx, "cars.year", 2020)))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// Left (NOT cars[1].year=2020): {100, 300, 400, 700, 800, 830, 850}.
			// Right (same-car honda + 2020): doc 200 cars[1] honda+2020,
			// doc 810 cars[0] honda+2020 → {200, 810}. Lifted to pathRoot
			// via MaskRootLeaf, both sides reach doc-level positions.
			// Union covers every ingested doc — left and right partition
			// the universe (every doc satisfies one or the other).
			wantDocs: []uint64{100, 200, 300, 400, 700, 800, 810, 830, 850},
		},
		// Pinned-root NOT (ancestor at pathRoot) AND-ed with a same-Scope
		// compound child at cars. The child is itself a same-Scope AND
		// (cars.colors=red AND cars.colors=blue) with Scope=cars and
		// Ceiling=cars — its Witnesses are per-car (cars where the
		// colors array contains BOTH values). andLeaves takes the
		// `ancestor.Scope == pathRoot` branch: lift the child to
		// pathRoot via liftToScope's pathRoot branch (which adds
		// authentic doc-level bits via MaskRootLeaf of child.Witnesses),
		// then intersect at pathRoot. Differs from earlier "leaf child"
		// tests by exercising the lift of a COMPOUND, which is what
		// originally surfaced the Ceiling bug in liftToScope.
		{
			name: "NOT cars[1].year=2020 AND (cars.colors=red AND cars.colors=blue) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPinnedNot(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 1}}),
					andLeaves(idx,
						leafPositive(idx, "cars.colors", "red"),
						leafPositive(idx, "cars.colors", "blue")))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// Inner per-car AND of red+blue same-array witnesses: only
			// doc 100 cars[1].colors=[blue,red] qualifies → inner = {100}.
			// NOT cars[1].year=2020 witnesses = {100, 300, 400, 800, 830, 850}.
			// Intersection at pathRoot = {100}.
			// doc 100: cars[1] year=2018 ≠ 2020 (NOT ✓), cars[1] colors
			//   contain red AND blue same-car (inner ✓) → PASS.
			wantDocs: []uint64{100},
		},
		// Double NOT around a pinned-root AND. Inner AND is the same
		// shape as the "NOT cars[0].year=2020 AND cars[1].make=honda"
		// test above (Witnesses={200,300}, Scope=pathRoot). Outer negate
		// inverts at pathRoot: docUniverse AndNot inner.Witnesses. Tests
		// that `negate` composes correctly when pos.Scope=pathRoot — i.e.
		// scopeAnchor=docUniverse is used uniformly via anchorAt.
		{
			name: "NOT (NOT cars[0].year=2020 AND cars[1].make=honda) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPinnedNot(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 0}}),
					leafPinnedPositive(idx, "cars.make", "honda",
						[]pinSpec{{"cars", 1}})))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// Inner AND witnesses = {200, 300}. Outer negate =
			// docUniverse AndNot {200, 300} = {100, 400, 700, 800, 810, 830, 850}.
			// Per-doc check:
			// doc 100 cars[0]=toyota+2020 → cars[0].year=2020 → inner-NOT
			//   fails → inner AND fails → outer PASS.
			// doc 400 no cars[1] → cars[1].make=honda fails → inner AND
			//   fails → outer PASS.
			// doc 700 no cars at all → both inner operands fail → outer PASS.
			// doc 800 no cars[1] → outer PASS.
			// doc 810 cars[0]=honda+2020 → inner-NOT fails → outer PASS.
			// doc 830 no cars[1] → outer PASS.
			// doc 850 cars[1] no make → outer PASS.
			wantDocs: []uint64{100, 400, 700, 800, 810, 830, 850},
		},
		// Same-scope pinned-root OR — both at Scope=pathRoot. Union at
		// doc level: any doc with cars[0]=2020 OR cars[1]=2020.
		{
			name: "cars[0].year=2020 OR cars[1].year=2020 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPinnedPositive(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 0}}),
					leafPinnedPositive(idx, "cars.year", 2020,
						[]pinSpec{{"cars", 1}}))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			wantDocs:    []uint64{100, 200, 810},
		},
		// Pinned-root ContainsAny — value union ∩ pin intersection through
		// pinnedFromValueSet's MaskRootLeaf branch. Verifies the same
		// helper handles multi-value composites at the root pin.
		{
			name: "cars[1].make ContainsAny [honda, toyota] [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAny(idx, "cars.make",
					[]pinSpec{{"cars", 1}}, "honda", "toyota")
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// cars[1].make: doc 100=honda, 200=honda, 300=honda, 810=toyota.
			wantDocs: []uint64{100, 200, 300, 810},
		},
		// Pinned-root ContainsAll on a text[] — both values constrained to
		// the same pinned car-element. pinnedFromValueSet's pathRoot
		// branch intersects the per-value buckets through MaskRootLeaf.
		// Pin forces both values into the same cars[1], so no
		// cross-element ghosts can creep in.
		{
			name: "cars[1].colors ContainsAll [red, blue] [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx, "cars.colors",
					[]pinSpec{{"cars", 1}}, "red", "blue")
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 100 cars[1].colors=[blue,red] → has both → PASS.
			// doc 200 cars[1].colors=[blue] only → FAIL.
			// doc 300 cars[1].colors=[red] only → FAIL.
			// Others: no cars[1] or cars[1] has no colors → FAIL.
			wantDocs: []uint64{100},
		},
		// Pinned-root ContainsAll on a scalar field — degenerate but
		// must not crash or produce false positives. A single make slot
		// can only hold one value, so values[honda] ∩ values[ford]
		// (the chain bits at cars-self) is ∅ for every car: a car
		// either has make=honda or make=ford, never both. The pinned-
		// root ContainsAll path produces an empty Raw and Witnesses.
		// Verifies pinnedFromValueSet's pathRoot branch tolerates an
		// already-empty `a` from intersection-of-disjoint-buckets.
		{
			name: "cars[1].make ContainsAll [honda, ford] [L0, degenerate scalar]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsAll(idx, "cars.make",
					[]pinSpec{{"cars", 1}}, "honda", "ford")
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			wantDocs:    nil,
		},
		// Pinned-root ContainsNone direct. Owner-level negation of
		// pinned ContainsAny — doc passes iff cars[1].make ∉ {honda, ford},
		// including missing-pin and missing-make as vacuous passes.
		// Routes through leafPinnedContainsNone → negate(pinned ContainsAny).
		// This is one corner of the three-way equivalence below.
		{
			name: "cars[1].make ContainsNone [honda, ford] [L0, direct]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedContainsNone(idx, "cars.make",
					[]pinSpec{{"cars", 1}}, "honda", "ford")
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// pinned ContainsAny [honda, ford] witnesses (docs with
			// cars[1].make ∈ {honda, ford}):
			//   doc 100/200/300 cars[1]=honda → in.
			//   doc 810 cars[1]=toyota → out.
			//   doc 400/700/800/830 no cars[1] → out.
			//   doc 850 cars[1] no make → out.
			// ContainsNone = docUniverse AndNot {100, 200, 300}.
			wantDocs: []uint64{400, 700, 800, 810, 830, 850},
		},
		// Equivalence #2: NOT (pinned ContainsAny). Internally same
		// construction as the direct ContainsNone above (negate of pinned
		// ContainsAny). wantDocs must match.
		{
			name: "NOT (cars[1].make ContainsAny [honda, ford]) [L0, via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafPinnedContainsAny(idx, "cars.make",
					[]pinSpec{{"cars", 1}}, "honda", "ford"))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			wantDocs:    []uint64{400, 700, 800, 810, 830, 850},
		},
		// Equivalence #3: NOT (pinned positive OR pinned positive).
		// Different construction path — two pinned-positive operands at
		// Scope=pathRoot, OR-ed at pathRoot, then negated. The OR's
		// witnesses are the union of cars[1].make=honda witnesses
		// ({100,200,300}) and cars[1].make=ford witnesses (∅ — no fixture
		// has cars[1].make=ford). Negate of {100,200,300} matches the
		// direct ContainsNone above.
		{
			name: "NOT (cars[1].make=honda OR cars[1].make=ford) [L0, via NOT-of-OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPinnedPositive(idx, "cars.make", "honda",
						[]pinSpec{{"cars", 1}}),
					leafPinnedPositive(idx, "cars.make", "ford",
						[]pinSpec{{"cars", 1}})))
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			wantDocs:    []uint64{400, 700, 800, 810, 830, 850},
		},
		// Intermediate-pin positive at the property root — root pin with
		// gap to a value path one level below the pinned element scope
		// (cars.tires.width). leafPinnedPositive routes through
		// pinnedFromValueSet's MaskRootLeaf branch, which already handles
		// the gap correctly because the doc-level lift is via MaskRootLeaf
		// regardless of how many levels lie between scope=pathRoot
		// and the value path. Per-element semantic: doc passes iff some
		// tire under cars[1] has width=205.
		{
			name: "cars[1].tires.width=205 [L0, intermediate-pin positive at root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx, "cars.tires.width", 205,
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 100 cars[1].tires=[195] → no 205 → FAIL.
			// doc 200 cars[1].tires=[205] → PASS.
			// doc 300 cars[1] no tires → FAIL.
			// doc 400 no cars[1] → FAIL (positive needs a witness).
			// doc 810 cars[1] no tires → FAIL.
			// doc 830 no cars[1] → FAIL.
			// doc 850 cars[1].tires=[205] → PASS.
			// doc 800 no cars[1] → FAIL.
			wantDocs: []uint64{200, 850},
		},
		// Intermediate-pin IS NULL false at the property root — same
		// route as the positive above, just reading the exists bucket
		// instead of a specific value bucket. Per-element semantic: doc
		// passes iff some tire under cars[1] has a width value set.
		{
			name: "cars[1].tires.width IS NULL false [L0, intermediate-pin IS NULL false at root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullFalse(idx, "cars.tires.width",
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// All width values in the L0 fixtures are integers, so any
			// tire entry has width set. wantDocs reduces to "docs with at
			// least one tire under cars[1]":
			// doc 100 cars[1].tires=[195] → PASS.
			// doc 200 cars[1].tires=[205] → PASS.
			// doc 850 cars[1].tires=[205] → PASS.
			// Others: cars[1] missing or has no tires → FAIL.
			wantDocs: []uint64{100, 200, 850},
		},
		// Multi-pin at the property root — two pins (cars[1] +
		// cars.tires[0]). leafPinnedPositive intersects values[width=205]
		// with idx["cars"][1] and idx["cars.tires"][0] before handing
		// off to pinnedFromValueSet's MaskRootLeaf branch. hasPinGap
		// returns false (len(pins)=2 == levelsBetween=2: cars→cars.tires
		// is fully pinned), so the negate/IsNullTrue counterparts would
		// route through negate(positive) rather than the per-element
		// formula. Result happens to match the single-pin
		// cars[1].tires.width=205 case because no L0 fixture has more
		// than one tire per car — the multi-pin path itself is what's
		// being exercised, not a different semantic.
		{
			name: "cars[1].tires[0].width=205 [L0, multi-pin at root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx, "cars.tires.width", 205,
					[]pinSpec{{"cars", 1}, {"cars.tires", 0}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 200 cars[1].tires[0]=205 → PASS.
			// doc 850 cars[1].tires[0]=205 → PASS.
			// doc 100 cars[1].tires[0]=195 → FAIL.
			// Others: no cars[1] or cars[1] no tires → FAIL.
			wantDocs: []uint64{200, 850},
		},
		// Multi-pin at the property root, discriminating shape. Only
		// doc 830 has cars[0].accessories[1] (its accessories array has
		// 2 elements: radio at [0] and spoiler at [1]); every other
		// fixture has at most one accessory per car so the [1] slot is
		// missing. The pin on cars.accessories[1] filters out emissions
		// at accessories[0] from values[spoiler], leaving only doc 830.
		{
			name: "cars[0].accessories[1].type=spoiler [L0, multi-pin at root, discriminator]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedPositive(idx, "cars.accessories.type", "spoiler",
					[]pinSpec{{"cars", 0}, {"cars.accessories", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// Only doc 830 has cars[0].accessories[1] (=spoiler). All
			// other docs lack the accessories[1] slot — their
			// values[spoiler] emissions sit at accessories[0] which the
			// pin excludes.
			wantDocs: []uint64{830},
		},
		// Intermediate-pin NOT at the property root — outermost pin at
		// cars (= propName) with a gap to the value path at cars.tires.
		// hasPinGap returns true (len(pins)=1 != levelsBetween=2), so
		// leafPinnedNot routes through perElementNotFromSubtractands with
		// scope=pathRoot. Per-element semantic: doc passes iff some
		// cars[1].tires has width != 205, OR cars[1] doesn't exist.
		{
			name: "NOT cars[1].tires.width=205 [L0, intermediate-pin NOT at root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedNot(idx, "cars.tires.width", 205,
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 100 cars[1].tires=[195] → 195≠205 → per-elem witness PASS.
			// doc 200 cars[1].tires=[205] only → no witness; cars[1] exists → FAIL.
			// doc 300 cars[1] no tires → no per-elem witness; cars[1] exists → FAIL.
			// doc 400 no cars[1] → missing-pin → PASS.
			// doc 700 no cars at all → missing-pin → PASS.
			// doc 810 cars[1] no tires → cars[1] exists, no witness → FAIL.
			// doc 830 no cars[1] → missing-pin → PASS.
			// doc 850 cars[1].tires=[205] only → no witness; cars[1] exists → FAIL.
			// doc 800 no cars[1] → missing-pin → PASS.
			wantDocs: []uint64{100, 400, 700, 800, 830},
		},
		// Intermediate-pin IS NULL true at the property root — same route
		// as the NOT case above, just with the _exists bitmap as
		// subtractand instead of a specific value bucket. Per-element
		// semantic: doc
		// passes iff some cars[1].tires lacks width, OR cars[1] doesn't
		// exist.
		{
			name: "cars[1].tires.width IS NULL true [L0, intermediate-pin IS NULL at root]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPinnedIsNullTrue(idx, "cars.tires.width",
					[]pinSpec{{"cars", 1}})
			},
			wantScope:   pathRoot,
			wantCeiling: pathRoot,
			// doc 100 cars[1].tires=[195] (has width) → no per-elem witness;
			//   cars[1] exists → FAIL.
			// doc 200 cars[1].tires=[205] → same → FAIL.
			// doc 300 cars[1] no tires → no per-elem witness; cars[1] exists → FAIL.
			// doc 400 no cars[1] → missing-pin → PASS.
			// doc 700 no cars at all → missing-pin → PASS.
			// doc 810 cars[1] no tires → no per-elem witness; cars[1] exists → FAIL.
			// doc 830 no cars[1] → missing-pin → PASS.
			// doc 850 cars[1].tires=[205] (has width) → no witness → FAIL.
			// doc 800 no cars[1] → missing-pin → PASS.
			wantDocs: []uint64{400, 700, 800, 830},
		},
	}

	runFastPathCases(t, idx, cases)
}
