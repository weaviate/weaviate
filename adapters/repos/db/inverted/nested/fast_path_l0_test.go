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
		Name:     "cars",
		DataType: []string{string(schema.DataTypeObjectArray)},
		NestedProperties: []*models.NestedProperty{
			in("year"),
			tx("make"),
			tx("name"),
			txArr("colors"),
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
		// with TS at depth 1 (= propName) instead of depth 3. The
		// CleanAbove walk in the harness must stop before going above
		// propName ("" is never read), which was the boundary case that
		// originally motivated the CleanAbove migration: under the old
		// CP=true bool the assertion did `idx.anchor[parentPath(TS)]`
		// unconditionally and tripped on parentPath("cars")="".
		{
			name: "cars.year=2020 [L0, single leaf]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafPositive(idx, "cars.year", 2020)
			},
			wantScope:      "cars",
			wantCleanAbove: "cars", // propName — clean to property root
			// 100 cars[0]=2020 PASS. 200 cars[1]=2020 PASS. 810 cars[0,1]=
			// 2020 PASS. Others: no car has year=2020 → FAIL.
			wantDocs: []uint64{100, 200, 810},
		},
		// Same-TS AND at L0. TS=cars (the property root). Same-element
		// correlation must come from chain-bit intersection at car-self,
		// just like at L2. The merge helpers (andLeaves, andAtScope,
		// same-TS dispatch) are scope-agnostic — they should produce the
		// same docID set whether the truth scope is "countries.garages.
		// cars" or just "cars".
		{
			name: "cars.make=honda AND cars.year=2020 [L0, same-element]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020))
			},
			wantScope: "cars",
			// Same-Scope AND: structural ceiling = Scope, so CleanAbove
			// = Scope. No claim above cars (which at L0 is the property
			// root, so "above cars" is just the doc level — non-trivial
			// only because the harness mustn't try to read anchor[""]).
			wantCleanAbove: "cars",
			// doc 100: cars[0]=toyota+2020, cars[1]=honda+2018 — split,
			// no same-car witness → FAIL.
			// doc 200 cars[1]=honda+2020 → same-car PASS.
			// doc 810 cars[0]=honda+2020 → same-car PASS.
			// Others: no same-car honda+2020 → FAIL.
			wantDocs: []uint64{200, 810},
		},
		// Same-TS OR. Mirrors L2 `cars.make=honda OR cars.year=2020`.
		// Both positive leaves at TS=cars with CleanAbove=propName.
		// OR preserves the higher CleanAbove through the no-op lift
		// (TS already at LCA), so result CleanAbove = propName = cars.
		// Owner-level semantic: doc satisfies the OR iff some car
		// matches honda OR some car matches 2020 (possibly different
		// cars). Discriminator doc 1200 PASSES here precisely because
		// the OR doesn't require same-car correlation.
		{
			name: "cars.make=honda OR cars.year=2020 [L0, same-Scope union]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// doc 100: cars[0]=2020 OR cars[1]=honda → PASS.
			// doc 200: cars[0,1] honda → PASS.
			// doc 300: cars[1]=honda → PASS.
			// doc 400: ford+2012 → FAIL.
			// doc 810: both predicates fire → PASS.
			// doc 830/850: no honda no 2020 → FAIL.
			wantDocs: []uint64{100, 200, 300, 810},
		},
		// Single-leaf NOT at L0. negate(leafPositive) collapses CleanAbove
		// to TS — Rich = _exists(TS) AndNot ES carries chain bits from
		// every doc that has any car, regardless of whether a NON-matching
		// witness exists. The boundary case at L0 is that TS=propName, so
		// no "above CS" assertion is even attempted in the harness loop.
		// Predicate: "doc has at least one car where year != 2020".
		{
			name: "NOT cars.year=2020 [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafNot(idx, "cars.year", 2020)
			},
			wantScope:      "cars",
			wantCleanAbove: "cars", // negate rule: CleanAbove collapses to Scope
			// Per-car: car is witness iff year != 2020 (cars without
			// year set also count — they aren't in the value bucket).
			// doc 100 cars[1,2]: year=2018/2017 → witnesses PASS.
			// doc 200 cars[0]=2015, cars[2] no year → witnesses PASS.
			// doc 300 cars[0,1] both witnesses PASS.
			// doc 400 cars[0]=2012 → witness PASS.
			// doc 810: every car year=2020 → no witness FAIL.
			// doc 830 cars[0] no year → witness PASS.
			// doc 850 cars[0,1]=2019 → witnesses PASS.
			wantDocs: []uint64{100, 200, 300, 400, 830, 850},
		},
		// Ancestor+child AND at L0. Scope pairing: cars.year
		// (Scope=cars = propName) AND cars.tires.width
		// (Scope=cars.tires, one below). The broadcasting branch in
		// andLeaves fires here under the property-root carve-out:
		// even though ancestor.CleanAbove == ancestor.Scope ==
		// propName (so aboveScopeClean() returns false), the dispatch
		// guard's `|| ancestor.Scope == idx.propName` clause lets the
		// branch fire because there's no scope above propName to be
		// unclean at. Combined with ancestor.CleanBelow=true
		// (positive leaf has authentic descendants via the
		// elementPositions encoding), the broadcasting argument
		// holds: ancestor.Bitmap at C.Scope is a real descendant of
		// matching cars, intersecting with child.Bitmap at C.Scope
		// gives same-car correlation. Merge at C.Scope=cars.tires,
		// ceiling=A.Scope=cars. Mirrors the L2 ancestor+child
		// behavior — same depth-preservation, just one tree level
		// shallower.
		{
			name: "cars.year=2020 AND cars.tires.width=205 [L0, ancestor+child]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.year", 2020),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:      "cars.tires",
			wantCleanAbove: "cars",
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
		// deeper operand (tires.width) up to cars via cheap-lift —
		// target=cars is at-or-below tires.CleanAbove=cars (=propName),
		// so Bitmap is reused unchanged and CleanAbove is preserved.
		// The shallower operand (year) is already at LCA=cars, no-op
		// lift. OR result has CleanAbove=cars, CleanBelow=true (both
		// operands' CleanBelow=true).
		{
			name: "cars.year=2020 OR cars.tires.width=205 [L0, ancestor+child OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "cars.year", 2020),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// doc 100: 2020 OR 205 → PASS.
			// doc 200: cars[1]=2020 OR cars[0,1]=205 → PASS.
			// doc 810: cars[0,1]=2020 → PASS.
			// doc 850 cars[1]=205 → PASS via tires.
			// Others: no → FAIL.
			wantDocs: []uint64{100, 200, 810, 850},
		},
		// ContainsAny at L0. Positive composite — union of value
		// buckets. Same shape as a single positive leaf but the value
		// set has multiple elements. Result Scope=cars (parent of the
		// value path), CleanAbove=propName=cars, CleanBelow=true (union
		// of authentic-descendant buckets is authentic).
		{
			name: "cars.make ContainsAny [honda, ford] [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafContainsAny(idx, "cars.make", "honda", "ford")
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
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
		// have CleanAbove=propName=cars), then recurse as same-Scope
		// AND. Same-Scope AND produces CleanAbove=Scope=cars (ceiling
		// rule).
		{
			name: "cars.accessories.type=spoiler AND cars.tires.width=205 [L0, siblings]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return andLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// Per-car: same car has spoiler accessory AND tires.width=205.
			// doc 100 cars[0] spoiler+205 → PASS.
			// doc 200 cars[0,1] both spoiler+205 → PASS.
			// doc 830 cars[0] spoiler but no tires → FAIL.
			// doc 850 cars[0]=spoiler+195, cars[1]=radio+205 — split,
			// no same-car witness → FAIL (the split discriminator).
			// Others: no spoiler → FAIL.
			wantDocs: []uint64{100, 200},
		},
		// IS NULL false on a scalar leaf (cars.year). Symmetric with
		// the single-leaf positive case: TS=cars, CleanAbove=propName,
		// CleanBelow=true. The leaf fires for every car with a year
		// value set, regardless of the value.
		{
			name: "cars.year IS NULL false [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return leafIsNullFalse(idx, "cars.year")
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
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
			wantScope:      "cars",
			wantCleanAbove: "cars", // negate collapses CleanAbove to Scope
			// doc 200 cars[2]=name=ford (no year) → witness PASS.
			// doc 300 cars[1]=honda (no year) → witness PASS.
			// doc 830 cars[0] has only accessories, no year → witness PASS.
			// Every other doc: every car has year set → no witness.
			wantDocs: []uint64{200, 300, 830},
		},
		// Sibling OR at L0 (under cars). Mirrors L2's
		// `cars.accessories.type=spoiler OR cars.tires.width=205`.
		// Both operands siblings under cars (different nested arrays).
		// orLeaves lifts both to LCA=cars via cheap-lift (CleanAbove=
		// propName=cars, target=cars, equal-depth path) which
		// preserves CleanAbove. Result.CleanAbove=cars, CleanBelow=
		// true && true.
		{
			name: "cars.accessories.type=spoiler OR cars.tires.width=205 [L0, siblings OR]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return orLeaves(idx,
					leafPositive(idx, "cars.accessories.type", "spoiler"),
					leafPositive(idx, "cars.tires.width", 205))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// doc 100 cars[0] both → PASS.
			// doc 200 cars[0,1] both → PASS.
			// doc 830 cars[0] spoiler → PASS.
			// doc 850 cars[0] spoiler OR cars[1] 205 → PASS (OR doesn't
			// require same-car, unlike the AND counterpart above).
			// Others: no spoiler, no 205 → FAIL.
			wantDocs: []uint64{100, 200, 830, 850},
		},
		// Compound NOT (NOT-of-OR) at L0. Inner OR has Scope=cars,
		// CleanAbove=cars, CleanBelow=true. negate of that produces
		// Scope=cars, CleanAbove=cars, CleanBelow=false. Per-car: a
		// car is a witness for the negation iff it is NOT in the OR's
		// positive ES — i.e. not honda AND year≠2020 (or year missing).
		{
			name: "NOT (cars.make=honda OR cars.year=2020) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, orLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020)))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// Per-car: car is witness iff NOT in OR ES = make ≠ honda
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
			wantDocs: []uint64{100, 200, 300, 400, 830, 850},
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
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// Must match NOT-of-OR above (per-element AND of negations
			// at same Scope is the De Morgan dual).
			wantDocs: []uint64{100, 200, 300, 400, 830, 850},
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
			wantScope:      "cars",
			wantCleanAbove: "cars", // negation shape — no claim above Scope
			// doc 100 cars[0]=toyota → witness PASS.
			// doc 200 cars[2]=name=ford (no make field) → witness PASS.
			// doc 300 cars[0]=ford, cars[1]=honda → no witness FAIL.
			// doc 400 cars[0]=ford → no witness FAIL.
			// doc 810 cars[1]=toyota → witness PASS.
			// doc 830 cars[0] no make → witness PASS.
			// doc 850 cars[0,1] no make → witnesses PASS.
			wantDocs: []uint64{100, 200, 810, 830, 850},
		},
		// NOT(ContainsAny) equivalence: should produce identical docs
		// to the direct ContainsNone above. Both routes negate the
		// same positive ES (cars with make ∈ {honda, ford}) at the
		// same Scope.
		{
			name: "NOT (cars.make ContainsAny [honda, ford]) [L0, via negate]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, leafContainsAny(idx, "cars.make", "honda", "ford"))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// Must match `ContainsNone direct` above.
			wantDocs: []uint64{100, 200, 810, 830, 850},
		},
		// Compound NOT-of-AND. Inner same-element AND has ES = cars
		// satisfying honda AND 2020 in the same car. negate excludes
		// those, leaves every other car as a witness.
		{
			name: "NOT (cars.make=honda AND cars.year=2020) [L0]",
			build: func(idx *fastPathIndex) *fastPathResult {
				return negate(idx, andLeaves(idx,
					leafPositive(idx, "cars.make", "honda"),
					leafPositive(idx, "cars.year", 2020)))
			},
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// Per-car: car is witness iff NOT (honda AND 2020 same-
			// element) = NOT honda OR NOT 2020. Only docs where every
			// car satisfies honda AND 2020 in the same element FAIL.
			// doc 810: cars[0]=honda+2020 → in inner.ES; cars[1]=
			// toyota+2020 → not honda → WITNESS → PASS.
			// Every other doc also has at least one such car.
			wantDocs: []uint64{100, 200, 300, 400, 810, 830, 850},
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
			wantScope:      "cars",
			wantCleanAbove: "cars",
			wantDocs:       []uint64{100, 200, 300, 400, 810, 830, 850},
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
			// fires the broadcasting branch (honda is positive, so
			// CleanBelow=true; and honda.Scope=cars=propName triggers
			// the property-root carve-out). Result lands at C.Scope=
			// cars.tires with ceiling=cars. Then merge with year:
			// year is also ancestor at cars, broadcasting again,
			// lands at cars.tires.
			wantScope:      "cars.tires",
			wantCleanAbove: "cars",
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
			// orN lifts all to LCA=cars (cheap, all CleanAbove=cars).
			// OR at cars. Result.CleanAbove=cars, CleanBelow=true.
			wantScope:      "cars",
			wantCleanAbove: "cars",
			// 100 has all three. 200 has all three. 300 cars[1]=honda
			// → PASS. 400 (ford+2012) → FAIL. 810 honda+2020 → PASS.
			// 830 no → FAIL. 850 cars[1]=205 → PASS.
			wantDocs: []uint64{100, 200, 300, 810, 850},
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
			wantScope:      "cars",
			wantCleanAbove: "cars", // same-Scope intersection rule
			// doc 100 cars[1].colors=[blue, red] → same-array witness PASS.
			// doc 200 cars[0]=[red], cars[1]=[blue] → split, FAIL —
			// the split discriminator.
			// Others: no car has both red and blue → FAIL.
			wantDocs: []uint64{100},
		},
	}

	runFastPathCases(t, idx, cases)
}
