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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Helper to build a nested property schema quickly
func textProp(name string) *models.NestedProperty {
	return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeText)}}
}

func intProp(name string) *models.NestedProperty {
	return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeInt)}}
}

func textArrayProp(name string) *models.NestedProperty {
	return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeTextArray)}}
}

func intArrayProp(name string) *models.NestedProperty {
	return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeIntArray)}}
}

func numberArrayProp(name string) *models.NestedProperty {
	return &models.NestedProperty{Name: name, DataType: []string{string(schema.DataTypeNumberArray)}}
}

func objectProp(name string, nested ...*models.NestedProperty) *models.NestedProperty {
	return &models.NestedProperty{
		Name:             name,
		DataType:         []string{string(schema.DataTypeObject)},
		NestedProperties: nested,
	}
}

func objectArrayProp(name string, nested ...*models.NestedProperty) *models.NestedProperty {
	return &models.NestedProperty{
		Name:             name,
		DataType:         []string{string(schema.DataTypeObjectArray)},
		NestedProperties: nested,
	}
}

func topLevelObject(name string, nested ...*models.NestedProperty) *models.Property {
	return &models.Property{
		Name:             name,
		DataType:         []string{string(schema.DataTypeObject)},
		NestedProperties: nested,
	}
}

func topLevelObjectArray(name string, nested ...*models.NestedProperty) *models.Property {
	return &models.Property{
		Name:             name,
		DataType:         []string{string(schema.DataTypeObjectArray)},
		NestedProperties: nested,
	}
}

// positions builds a []ElemIdx from the given globally-monotone element
// indices (1-based). The expected values in assertions are always raw indices;
// callers expand them for bitmap operations via PositionsWithDocID.
func positions(elems ...uint32) []ElemIdx {
	out := make([]ElemIdx, len(elems))
	for i, e := range elems {
		out[i] = ElemIdx(e)
	}
	return out
}

// assertValue checks that a value at the given path with the given raw value
// and expected positions exists. Matches on path+value+positions to handle
// duplicate values across different elements (e.g. float64(18) in e1 and e18).
func assertValue(t *testing.T, result *AssignResult, path string, value any, expected []ElemIdx) {
	t.Helper()
	for _, v := range result.Values {
		if v.Path == path && v.Value == value && assert.ObjectsAreEqual(expected, result.Positions(v.Pos)) {
			return
		}
	}
	t.Errorf("value %v with positions %v not found at path %s", value, expected, path)
}

// assertIdx checks the aggregated positions for _idx entries at a given path
// and index. Multiple raw entries for the same path+index (from different
// parent array elements) are merged, matching what the write path does.
func assertIdx(t *testing.T, result *AssignResult, path string, index int, expected []ElemIdx) {
	t.Helper()
	var actual []ElemIdx
	for _, idx := range result.Idx {
		if idx.Path == path && idx.Index == index {
			actual = append(actual, result.Positions(idx.Pos)...)
		}
	}
	if len(actual) == 0 {
		t.Errorf("_idx.%s[%d] not found", path, index)
		return
	}
	assert.ElementsMatch(t, expected, actual, "_idx.%s[%d]", path, index)
}

// assertExists checks the aggregated positions for _exists entries at a given
// path. Multiple raw entries for the same path (from different parent array
// elements) are merged, matching what the write path does.
func assertExists(t *testing.T, result *AssignResult, path string, expected []ElemIdx) {
	t.Helper()
	var actual []ElemIdx
	for _, e := range result.Exists {
		if e.Path == path {
			actual = append(actual, result.Positions(e.Pos)...)
		}
	}
	if len(actual) == 0 {
		t.Errorf("_exists.%s not found", path)
		return
	}
	assert.ElementsMatch(t, expected, actual, "_exists.%s", path)
}

// assertAnchor checks the aggregated marker indices for _anchor entries at
// a given path. Multiple raw entries (one per element instance) are merged,
// matching what the write path does.
func assertAnchor(t *testing.T, result *AssignResult, path string, expected []ElemIdx) {
	t.Helper()
	var actual []ElemIdx
	for _, a := range result.Anchors {
		if a.Path == path {
			actual = append(actual, a.Position)
		}
	}
	if len(actual) == 0 {
		t.Errorf("_anchor.%s not found", path)
		return
	}
	assert.ElementsMatch(t, expected, actual, "_anchor.%s", path)
}

// findValues returns all ValueEntries for a given path.
func findValues(result *AssignResult, path string) []ValueEntry {
	var out []ValueEntry
	for _, v := range result.Values {
		if v.Path == path {
			out = append(out, v)
		}
	}
	return out
}

// findIdx returns IdxEntry for a given path and index
func findIdx(result *AssignResult, path string, index int) *IdxEntry {
	for _, idx := range result.Idx {
		if idx.Path == path && idx.Index == index {
			return &idx
		}
	}
	return nil
}

// findExists returns ExistsEntry for a given path
func findExists(result *AssignResult, path string) *ExistsEntry {
	for _, e := range result.Exists {
		if e.Path == path {
			return &e
		}
	}
	return nil
}

// findAnchor returns the first AnchorEntry for a given path, or nil if
// none exists. Use assertAnchor when you need to validate the aggregated
// marker bitmap across multiple element emissions.
func findAnchor(result *AssignResult, path string) *AnchorEntry {
	for _, a := range result.Anchors {
		if a.Path == path {
			return &a
		}
	}
	return nil
}

func TestAssignPositions_NilValue(t *testing.T) {
	prop := topLevelObject("nested", textProp("name"))
	result, err := AssignPositions(prop, nil)
	require.NoError(t, err)
	assert.Empty(t, result.Values)
	assert.Empty(t, result.Idx)
	assert.Empty(t, result.Exists)
	assert.Empty(t, result.Anchors)
}

func TestAssignPositions_EmptyObjectArray(t *testing.T) {
	prop := topLevelObjectArray("nested", textProp("name"))
	result, err := AssignPositions(prop, []any{})
	require.NoError(t, err)
	assert.Empty(t, result.Values)
	assert.Empty(t, result.Anchors)
}

func TestAssignPositions_SimpleScalar(t *testing.T) {
	// Object with a single scalar property — root element has no nested
	// children, so its self marker (e1) is the only position. Scalar `name`
	// inherits the element's positions.
	prop := topLevelObject("nested", textProp("name"))
	value := map[string]any{"name": "hello"}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	require.Len(t, result.Values, 1)
	assert.Equal(t, "name", result.Values[0].Path)
	assert.Equal(t, "hello", result.Values[0].Value)
	assert.Equal(t, schema.DataTypeText, result.Values[0].DataType)
	assert.Equal(t, positions(1), result.Positions(result.Values[0].Pos))

	// _exists.name and root _exists
	nameExists := findExists(result, "name")
	require.NotNil(t, nameExists)
	assert.Equal(t, positions(1), result.Positions(nameExists.Pos))

	rootExists := findExists(result, "")
	require.NotNil(t, rootExists)
	assert.Equal(t, positions(1), result.Positions(rootExists.Pos))

	// _anchor.""  = root element's self marker
	assertAnchor(t, result, "", positions(1))
}

func TestAssignPositions_ScalarArray(t *testing.T) {
	// Object with scalar array: tags=["german", "premium"]. Root marker
	// takes e1; each scalar-array element gets its own marker (e2, e3) and
	// inherits the root marker as its ancestor chain. Anchor entries are
	// exact (self markers only, no chain).
	prop := topLevelObject("nested", textArrayProp("tags"))
	value := map[string]any{
		"tags": []any{"german", "premium"},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	tags := findValues(result, "tags")
	require.Len(t, tags, 2)
	assert.Equal(t, "german", tags[0].Value)
	assert.Equal(t, positions(1, 2), result.Positions(tags[0].Pos))
	assert.Equal(t, "premium", tags[1].Value)
	assert.Equal(t, positions(1, 3), result.Positions(tags[1].Pos))

	// _idx for each element (chain + self)
	idx0 := findIdx(result, "tags", 0)
	require.NotNil(t, idx0)
	assert.Equal(t, positions(1, 2), result.Positions(idx0.Pos))

	idx1 := findIdx(result, "tags", 1)
	require.NotNil(t, idx1)
	assert.Equal(t, positions(1, 3), result.Positions(idx1.Pos))

	// _exists.tags — chain + every element's self marker
	tagsExists := findExists(result, "tags")
	require.NotNil(t, tagsExists)
	assert.Equal(t, positions(1, 2, 3), result.Positions(tagsExists.Pos))

	// _anchor — exact: root marker, plus per-tag-element markers (no chain)
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "tags", positions(2, 3))
}

// TestAssignPositions_OwnerDoc123 tests position assignment for doc123's
// owner section. Each element's positions = ancestor chain + self +
// descendant selves; anchors are exact (self-only).
//
//	root (chain=∅, self=e1, desc={e2..e4}) → {e1, e2, e3, e4}
//	owner (chain={e1}, self=e2, desc={e3, e4}) → {e1, e2, e3, e4}
//	├─ firstname="Marsha"          inherits owner → {e1, e2, e3, e4}
//	├─ lastname="Mallow"           inherits owner → {e1, e2, e3, e4}
//	├─ nicknames[0]="Marshmallow"  chain={e1, e2}, self=e3 → {e1, e2, e3}
//	└─ nicknames[1]="M&M"          chain={e1, e2}, self=e4 → {e1, e2, e4}
func TestAssignPositions_OwnerDoc123(t *testing.T) {
	prop := topLevelObject("nested",
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
			textArrayProp("nicknames"),
		),
	)

	value := map[string]any{
		"owner": map[string]any{
			"firstname": "Marsha",
			"lastname":  "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// nicknames are scalar-array elements: chain (root + owner) + self.
	nicknames := findValues(result, "owner.nicknames")
	require.Len(t, nicknames, 2)
	assert.Equal(t, "Marshmallow", nicknames[0].Value)
	assert.Equal(t, positions(1, 2, 3), result.Positions(nicknames[0].Pos))
	assert.Equal(t, "M&M", nicknames[1].Value)
	assert.Equal(t, positions(1, 2, 4), result.Positions(nicknames[1].Pos))

	// firstname / lastname inherit owner's full elementPositions.
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, "Marsha", firstnames[0].Value)
	assert.Equal(t, positions(1, 2, 3, 4), result.Positions(firstnames[0].Pos))

	lastnames := findValues(result, "owner.lastname")
	require.Len(t, lastnames, 1)
	assert.Equal(t, "Mallow", lastnames[0].Value)
	assert.Equal(t, positions(1, 2, 3, 4), result.Positions(lastnames[0].Pos))

	// _exists.owner — owner's chain + owner self + descendants.
	ownerExists := findExists(result, "owner")
	require.NotNil(t, ownerExists)
	assert.Equal(t, positions(1, 2, 3, 4), result.Positions(ownerExists.Pos))

	// _exists.owner.nicknames — chain (root + owner) + every nickname self.
	nickExists := findExists(result, "owner.nicknames")
	require.NotNil(t, nickExists)
	assert.Equal(t, positions(1, 2, 3, 4), result.Positions(nickExists.Pos))

	// _idx.owner.nicknames[K] — chain + self of the K-th element.
	assert.Equal(t, positions(1, 2, 3), result.Positions(findIdx(result, "owner.nicknames", 0).Pos))
	assert.Equal(t, positions(1, 2, 4), result.Positions(findIdx(result, "owner.nicknames", 1).Pos))

	// _anchor — exact, self-only at every level.
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assertAnchor(t, result, "owner.nicknames", positions(3, 4))
}

// TestAssignPositions_OwnerDoc125LeafNode tests owner with no nicknames in
// data. Owner still owns its self marker even without descendants; positions
// reflect chain + self.
//
//	root (chain=∅, self=e1, desc={e2}) → {e1, e2}
//	owner (chain={e1}, self=e2, desc=∅)  → {e1, e2}
//	├─ firstname="Anna"  inherits owner → {e1, e2}
//	└─ lastname="Wanna"  inherits owner → {e1, e2}
func TestAssignPositions_OwnerDoc125LeafNode(t *testing.T) {
	prop := topLevelObject("nested",
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
			textArrayProp("nicknames"), // present in schema but absent in data
		),
	)

	value := map[string]any{
		"owner": map[string]any{
			"firstname": "Anna",
			"lastname":  "Wanna",
			// no nicknames in data; owner still owns its self marker e2
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, positions(1, 2), result.Positions(firstnames[0].Pos))

	lastnames := findValues(result, "owner.lastname")
	require.Len(t, lastnames, 1)
	assert.Equal(t, positions(1, 2), result.Positions(lastnames[0].Pos))

	// _exists.owner — chain + owner self (no descendants in data).
	ownerExists := findExists(result, "owner")
	require.NotNil(t, ownerExists)
	assert.Equal(t, positions(1, 2), result.Positions(ownerExists.Pos))

	// No _exists.owner.nicknames (nicknames not present in data)
	assert.Nil(t, findExists(result, "owner.nicknames"))

	// _anchor — exact, self-only; no nicknames anchor since nicknames absent.
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assert.Nil(t, findAnchor(result, "owner.nicknames"))
}

// TestAssignPositions_Doc124Addresses tests mixed addresses with and
// without scalar-array descendants. Each element's positions = chain +
// self + descendants; anchors are exact.
//
//	root           (chain=∅,        self=e1, desc={e2..e6}) → {e1..e6}
//	owner          (chain={e1},     self=e2, desc={e3})     → {e1, e2, e3}
//	├─ nicknames[0]="watch"   chain={e1,e2}, self=e3        → {e1, e2, e3}
//	addresses[0]   (chain={e1},     self=e4, desc={e5})     → {e1, e4, e5}
//	├─ city="Madrid"          inherits addr[0]              → {e1, e4, e5}
//	├─ postcode="28001"       inherits addr[0]              → {e1, e4, e5}
//	└─ numbers[0]=124         chain={e1,e4}, self=e5        → {e1, e4, e5}
//	addresses[1]   (chain={e1},     self=e6, desc=∅)        → {e1, e6}
//	├─ city="London"          inherits addr[1]              → {e1, e6}
//	└─ postcode="SW1"         inherits addr[1]              → {e1, e6}
func TestAssignPositions_Doc124Addresses(t *testing.T) {
	prop := topLevelObject("nested",
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
			textArrayProp("nicknames"),
		),
		objectArrayProp("addresses",
			textProp("city"),
			textProp("postcode"),
			numberArrayProp("numbers"),
		),
	)

	value := map[string]any{
		"owner": map[string]any{
			"firstname": "Justin",
			"lastname":  "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{
				"city":     "Madrid",
				"postcode": "28001",
				"numbers":  []any{float64(124)},
			},
			map[string]any{
				"city":     "London",
				"postcode": "SW1",
				// no numbers → leaf
			},
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// owner.nicknames[0]="watch" — chain (root + owner) + self.
	nicknames := findValues(result, "owner.nicknames")
	require.Len(t, nicknames, 1)
	assert.Equal(t, positions(1, 2, 3), result.Positions(nicknames[0].Pos))

	// addresses[0].numbers[0]=124 — chain (root + addr[0]) + self.
	numbers := findValues(result, "addresses.numbers")
	require.Len(t, numbers, 1)
	assert.Equal(t, float64(124), numbers[0].Value)
	assert.Equal(t, positions(1, 4, 5), result.Positions(numbers[0].Pos))

	// addr[0] city/postcode inherit addr[0]'s elementPositions = chain + self + desc.
	cities := findValues(result, "addresses.city")
	require.Len(t, cities, 2)
	assert.Equal(t, "Madrid", cities[0].Value)
	assert.Equal(t, positions(1, 4, 5), result.Positions(cities[0].Pos))

	// addr[1] has no descendants; city/postcode inherit chain + self.
	assert.Equal(t, "London", cities[1].Value)
	assert.Equal(t, positions(1, 6), result.Positions(cities[1].Pos))

	postcodes := findValues(result, "addresses.postcode")
	require.Len(t, postcodes, 2)
	assert.Equal(t, "28001", postcodes[0].Value)
	assert.Equal(t, positions(1, 4, 5), result.Positions(postcodes[0].Pos))
	assert.Equal(t, "SW1", postcodes[1].Value)
	assert.Equal(t, positions(1, 6), result.Positions(postcodes[1].Pos))

	// _idx.addresses[K] — chain + K-th element's self + descendants.
	assert.Equal(t, positions(1, 4, 5), result.Positions(findIdx(result, "addresses", 0).Pos))
	assert.Equal(t, positions(1, 6), result.Positions(findIdx(result, "addresses", 1).Pos))

	// _exists.addresses — chain + union of every addr element's subtree selves.
	addrExists := findExists(result, "addresses")
	require.NotNil(t, addrExists)
	assert.Equal(t, positions(1, 4, 5, 6), result.Positions(addrExists.Pos))

	// _exists.addresses.numbers — chain (root + addr[0]) + numbers[0] self.
	numExists := findExists(result, "addresses.numbers")
	require.NotNil(t, numExists)
	assert.Equal(t, positions(1, 4, 5), result.Positions(numExists.Pos))

	// _anchor — exact, self-only at every level.
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assertAnchor(t, result, "owner.nicknames", positions(3))
	assertAnchor(t, result, "addresses", positions(4, 6))
	assertAnchor(t, result, "addresses.numbers", positions(5))
}

// TestAssignPositions_EmptyScalarArray tests that an empty scalar array
// produces no leaf positions for its elements. Owner still gets its own
// self marker (Phase 0) under per-element-anchor encoding.
func TestAssignPositions_EmptyScalarArray(t *testing.T) {
	prop := topLevelObject("nested",
		objectProp("owner",
			textProp("firstname"),
			textArrayProp("nicknames"),
		),
	)

	value := map[string]any{
		"owner": map[string]any{
			"firstname": "Test",
			"nicknames": []any{}, // empty array
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Empty nicknames produce no descendants; owner's elementPositions
	// reduce to chain + self = {e1, e2}. firstname inherits them.
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, positions(1, 2), result.Positions(firstnames[0].Pos))

	// No nicknames values
	assert.Empty(t, findValues(result, "owner.nicknames"))

	// _anchor — exact, self-only; no nicknames anchor since no elements emitted
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assert.Nil(t, findAnchor(result, "owner.nicknames"))
}

func TestAssignPositions_NotNestedType(t *testing.T) {
	prop := &models.Property{
		Name:     "flat",
		DataType: []string{"text"},
	}
	_, err := AssignPositions(prop, "hello")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a nested type")
}

// TestNextElem_OverflowReturnsError pins the overflow guard in nextElem.
// Valid indices are 1..MaxElems-1; once elemIdx reaches MaxElems the guard
// fires and an error is returned instead of silently clipping or panicking.
func TestNextElem_OverflowReturnsError(t *testing.T) {
	w := &walker{elemIdx: uint32(MaxElems), result: &AssignResult{}}
	_, err := w.nextElem()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

// TestAssignPositions_OverflowReturnsError pins the full error chain:
// nextElem → walkObject → AssignPositions. Each object element consumes
// exactly one slot (its Phase 0 self marker). MaxElems elements exhaust
// all valid indices (1..MaxElems-1); the final element tries to claim
// slot MaxElems and AssignPositions must return a non-nil error.
func TestAssignPositions_OverflowReturnsError(t *testing.T) {
	prop := topLevelObjectArray("data", textProp("v"))
	value := make([]any, MaxElems)
	for i := range value {
		value[i] = map[string]any{"v": "x"}
	}
	_, err := AssignPositions(prop, value)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

// TestAssignPositions_ElementPositionsIncludeAncestorChain pins the encoding
// rule that an element's positions must contain the full ancestor chain
// (every enclosing element's self marker), not just self+descendants.
//
// Single-chain country → garage → car → year=2020:
//
//	Phase 0 allocations (DFS): e1=M_country, e2=M_garage, e3=M_car.
//	The car has no array descendants, so year inherits car's elementPositions.
//
// Per the encoding rule, year=2020 must carry {e1, e2, e3}.
func TestAssignPositions_ElementPositionsIncludeAncestorChain(t *testing.T) {
	prop := topLevelObjectArray("countries",
		objectArrayProp("garages",
			objectArrayProp("cars",
				intProp("year"),
			),
		),
	)

	value := []any{
		map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"year": 2020},
			}},
		}},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// year=2020 lives on the car (Phase 3 scalar). Its positions are the
	// car's elementPositions, which must contain the country and garage
	// markers from the ancestor chain in addition to the car self marker.
	assertValue(t, result, "garages.cars.year", 2020, positions(1, 2, 3))
}

// TestAssignPositions_DeepChainPropagatesThroughFiveLevels exercises the
// chain-propagation algorithm at depth 5 — beyond the 3-level cases used
// elsewhere. A single chain of object[] levels with a scalar leaf at the
// bottom verifies that the chain accumulates correctly through every
// Phase 0 allocation and lands at the deepest emission.
//
//	countries[0]         (chain=∅,                  self=e1) → {e1, e2, e3, e4, e5}
//	└── regions[0]       (chain={e1},               self=e2) → {e1, e2, e3, e4, e5}
//	    └── cities[0]    (chain={e1, e2},           self=e3) → {e1, e2, e3, e4, e5}
//	        └── streets[0]   (chain={e1, e2, e3},   self=e4) → {e1, e2, e3, e4, e5}
//	            └── buildings[0]  (chain={e1..e4},  self=e5) → {e1, e2, e3, e4, e5}
//	                └── year=1999  inherits building            → {e1, e2, e3, e4, e5}
func TestAssignPositions_DeepChainPropagatesThroughFiveLevels(t *testing.T) {
	prop := topLevelObjectArray("countries",
		objectArrayProp("regions",
			objectArrayProp("cities",
				objectArrayProp("streets",
					objectArrayProp("buildings",
						intProp("year"),
					),
				),
			),
		),
	)

	value := []any{
		map[string]any{"regions": []any{
			map[string]any{"cities": []any{
				map[string]any{"streets": []any{
					map[string]any{"buildings": []any{
						map[string]any{"year": 1999},
					}},
				}},
			}},
		}},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Leaf scalar inherits the full chain from country down to building.
	assertValue(t, result, "regions.cities.streets.buildings.year", 1999, positions(1, 2, 3, 4, 5))

	// _idx entries: each level's K-th element's elementPositions =
	// chain + self + descendants. The single chain means every level's
	// Idx[0] sees the full {e1..e5}.
	assertIdx(t, result, "regions", 0, positions(1, 2, 3, 4, 5))
	assertIdx(t, result, "regions.cities", 0, positions(1, 2, 3, 4, 5))
	assertIdx(t, result, "regions.cities.streets", 0, positions(1, 2, 3, 4, 5))
	assertIdx(t, result, "regions.cities.streets.buildings", 0, positions(1, 2, 3, 4, 5))
	// Root-level Idx: chain ∅, subtree = all leaves.
	assertIdx(t, result, "", 0, positions(1, 2, 3, 4, 5))

	// _exists at each level — same shape because there's a single chain.
	assertExists(t, result, "", positions(1, 2, 3, 4, 5))
	assertExists(t, result, "regions", positions(1, 2, 3, 4, 5))
	assertExists(t, result, "regions.cities", positions(1, 2, 3, 4, 5))
	assertExists(t, result, "regions.cities.streets", positions(1, 2, 3, 4, 5))
	assertExists(t, result, "regions.cities.streets.buildings", positions(1, 2, 3, 4, 5))
	assertExists(t, result, "regions.cities.streets.buildings.year", positions(1, 2, 3, 4, 5))

	// _anchor — exact, self-only at every level. The five anchors should
	// land on e1..e5 with no chain bits.
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "regions", positions(2))
	assertAnchor(t, result, "regions.cities", positions(3))
	assertAnchor(t, result, "regions.cities.streets", positions(4))
	assertAnchor(t, result, "regions.cities.streets.buildings", positions(5))
}

// ---------------------------------------------------------------------------
// Full document tests from design summary.
// Each test runs with both the full shared schema and a minimal per-document
// schema to verify that appending new sub-properties to the schema does not
// change position assignment for existing documents.
// ---------------------------------------------------------------------------

// fullNestedSchema returns the shared schema used by doc123/doc124/doc125 in the design.
// It is a superset of all per-document schemas.
func fullNestedSchema() []*models.NestedProperty {
	return []*models.NestedProperty{
		textProp("name"),
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
			textArrayProp("nicknames"),
		),
		objectArrayProp("addresses",
			textProp("city"),
			textProp("postcode"),
			numberArrayProp("numbers"),
		),
		textArrayProp("tags"),
		objectArrayProp("cars",
			textProp("make"),
			objectArrayProp("tires",
				intProp("width"),
				intArrayProp("radiuses"),
			),
			objectArrayProp("accessories",
				textProp("type"),
			),
			textArrayProp("colors"),
		),
	}
}

// doc123Schema returns the minimal schema inferred from doc123's data only.
// No accessories (only doc125 has them).
func doc123Schema() []*models.NestedProperty {
	return []*models.NestedProperty{
		textProp("name"),
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
			textArrayProp("nicknames"),
		),
		objectArrayProp("addresses",
			textProp("city"),
			textProp("postcode"),
			numberArrayProp("numbers"),
		),
		textArrayProp("tags"),
		objectArrayProp("cars",
			textProp("make"),
			objectArrayProp("tires",
				intProp("width"),
				intArrayProp("radiuses"),
			),
			textArrayProp("colors"),
		),
	}
}

// doc124Schema returns the minimal schema inferred from doc124's data only.
// No accessories, no colors on Audi (but colors on Kia).
func doc124Schema() []*models.NestedProperty {
	return []*models.NestedProperty{
		textProp("name"),
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
			textArrayProp("nicknames"),
		),
		objectArrayProp("addresses",
			textProp("city"),
			textProp("postcode"),
			numberArrayProp("numbers"),
		),
		textArrayProp("tags"),
		objectArrayProp("cars",
			textProp("make"),
			objectArrayProp("tires",
				intProp("width"),
				intArrayProp("radiuses"),
			),
			textArrayProp("colors"),
		),
	}
}

// doc125Schema returns the minimal schema inferred from doc125's data only.
// Has accessories, no nicknames in data (but in schema since other docs have it).
// Actually, minimal means only what this doc uses: no nicknames property at all.
func doc125Schema() []*models.NestedProperty {
	return []*models.NestedProperty{
		textProp("name"),
		objectProp("owner",
			textProp("firstname"),
			textProp("lastname"),
		),
		objectArrayProp("addresses",
			textProp("city"),
			textProp("postcode"),
			numberArrayProp("numbers"),
		),
		textArrayProp("tags"),
		objectArrayProp("cars",
			textProp("make"),
			objectArrayProp("tires",
				intProp("width"),
				intArrayProp("radiuses"),
			),
			objectArrayProp("accessories",
				textProp("type"),
			),
			textArrayProp("colors"),
		),
	}
}

// TestAssignPositions_Doc123Full tests complete doc123 (Marsha). Every
// element's positions = chain + self + descendant selves; anchors are
// exact. 15 elements total (e1..e15). Runs with both full shared schema and
// minimal doc123-only schema to verify that extra properties in the schema
// don't affect position assignment.
//
//	root           (chain=∅,           self=e1,  desc={e2..e15}) → {e1..e15}
//	owner          (chain={e1},        self=e2,  desc={e3, e4})  → {e1, e2, e3, e4}
//	├─ firstname="Marsha"               inherits owner            → {e1, e2, e3, e4}
//	├─ lastname="Mallow"                inherits owner            → {e1, e2, e3, e4}
//	├─ nicknames[0]="Marshmallow"   chain={e1, e2}, self=e3       → {e1, e2, e3}
//	└─ nicknames[1]="M&M"           chain={e1, e2}, self=e4       → {e1, e2, e4}
//	addresses[0]   (chain={e1},        self=e5,  desc={e6, e7})   → {e1, e5, e6, e7}
//	├─ city="Berlin"                    inherits addr[0]          → {e1, e5, e6, e7}
//	├─ postcode="10115"                 inherits addr[0]          → {e1, e5, e6, e7}
//	├─ numbers[0]=123               chain={e1, e5}, self=e6       → {e1, e5, e6}
//	└─ numbers[1]=1123              chain={e1, e5}, self=e7       → {e1, e5, e7}
//	tags[0]="german"                chain={e1},     self=e8       → {e1, e8}
//	tags[1]="premium"               chain={e1},     self=e9       → {e1, e9}
//	cars[0]        (chain={e1},        self=e10, desc={e11..e15}) → {e1, e10..e15}
//	├─ make="BMW"                       inherits car[0]           → {e1, e10..e15}
//	├─ tires[0]    (chain={e1, e10},   self=e11, desc={e12, e13}) → {e1, e10..e13}
//	│  ├─ width=225                     inherits tire[0]          → {e1, e10..e13}
//	│  ├─ radiuses[0]=18            chain={e1, e10, e11}, self=e12 → {e1, e10, e11, e12}
//	│  └─ radiuses[1]=19            chain={e1, e10, e11}, self=e13 → {e1, e10, e11, e13}
//	├─ colors[0]="black"            chain={e1, e10}, self=e14      → {e1, e10, e14}
//	└─ colors[1]="orange"           chain={e1, e10}, self=e15      → {e1, e10, e15}
//	name="subdoc_123"                   inherits root              → {e1..e15}
func TestAssignPositions_Doc123Full(t *testing.T) {
	schemas := map[string][]*models.NestedProperty{
		"full_schema":    fullNestedSchema(),
		"minimal_schema": doc123Schema(),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			assertDoc123(t, schema)
		})
	}
}

func assertDoc123(t *testing.T, schema []*models.NestedProperty) {
	t.Helper()
	prop := topLevelObject("nestedObject", schema...)

	value := map[string]any{
		"name": "subdoc_123",
		"owner": map[string]any{
			"firstname": "Marsha",
			"lastname":  "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
		"addresses": []any{
			map[string]any{
				"city":     "Berlin",
				"postcode": "10115",
				"numbers":  []any{float64(123), float64(1123)},
			},
		},
		"tags": []any{"german", "premium"},
		"cars": []any{
			map[string]any{
				"make": "BMW",
				"tires": []any{
					map[string]any{
						"width":    float64(225),
						"radiuses": []any{float64(18), float64(19)},
					},
				},
				"colors": []any{"black", "orange"},
			},
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Values: 17 entries
	require.Len(t, result.Values, 17)
	assertValue(t, result, "owner.nicknames", "Marshmallow", positions(1, 2, 3))
	assertValue(t, result, "owner.nicknames", "M&M", positions(1, 2, 4))
	assertValue(t, result, "owner.firstname", "Marsha", positions(1, 2, 3, 4))
	assertValue(t, result, "owner.lastname", "Mallow", positions(1, 2, 3, 4))
	assertValue(t, result, "addresses.numbers", float64(123), positions(1, 5, 6))
	assertValue(t, result, "addresses.numbers", float64(1123), positions(1, 5, 7))
	assertValue(t, result, "addresses.city", "Berlin", positions(1, 5, 6, 7))
	assertValue(t, result, "addresses.postcode", "10115", positions(1, 5, 6, 7))
	assertValue(t, result, "tags", "german", positions(1, 8))
	assertValue(t, result, "tags", "premium", positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 10, 11, 12))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(1, 10, 11, 13))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 10, 11, 12, 13))
	assertValue(t, result, "cars.make", "BMW", positions(1, 10, 11, 12, 13, 14, 15))
	assertValue(t, result, "cars.colors", "black", positions(1, 10, 14))
	assertValue(t, result, "cars.colors", "orange", positions(1, 10, 15))
	assertValue(t, result, "name", "subdoc_123", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))

	// Idx: 15 entries (14 sub-array + 1 root). Each IdxEntry = chain + the
	// K-th element's self + descendant selves.
	require.Len(t, result.Idx, 15)
	assertIdx(t, result, "owner", 0, positions(1, 2, 3, 4))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 2, 3))
	assertIdx(t, result, "owner.nicknames", 1, positions(1, 2, 4))
	assertIdx(t, result, "addresses", 0, positions(1, 5, 6, 7))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 5, 6))
	assertIdx(t, result, "addresses.numbers", 1, positions(1, 5, 7))
	assertIdx(t, result, "tags", 0, positions(1, 8))
	assertIdx(t, result, "tags", 1, positions(1, 9))
	assertIdx(t, result, "cars", 0, positions(1, 10, 11, 12, 13, 14, 15))
	assertIdx(t, result, "cars.tires", 0, positions(1, 10, 11, 12, 13))
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 10, 11, 12))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 10, 11, 13))
	assertIdx(t, result, "cars.colors", 0, positions(1, 10, 14))
	assertIdx(t, result, "cars.colors", 1, positions(1, 10, 15))
	// root-level _idx entry for arr[N] positional filtering; chain is ∅ at root.
	assertIdx(t, result, "", 0, positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))

	// Exists: 17 entries. Each ExistsEntry = chain + union of all subtree selves.
	require.Len(t, result.Exists, 17)
	assertExists(t, result, "", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "name", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "owner", positions(1, 2, 3, 4))
	assertExists(t, result, "owner.firstname", positions(1, 2, 3, 4))
	assertExists(t, result, "owner.lastname", positions(1, 2, 3, 4))
	assertExists(t, result, "owner.nicknames", positions(1, 2, 3, 4))
	assertExists(t, result, "addresses", positions(1, 5, 6, 7))
	assertExists(t, result, "addresses.city", positions(1, 5, 6, 7))
	assertExists(t, result, "addresses.postcode", positions(1, 5, 6, 7))
	assertExists(t, result, "addresses.numbers", positions(1, 5, 6, 7))
	assertExists(t, result, "tags", positions(1, 8, 9))
	assertExists(t, result, "cars", positions(1, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "cars.make", positions(1, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "cars.tires", positions(1, 10, 11, 12, 13))
	assertExists(t, result, "cars.tires.width", positions(1, 10, 11, 12, 13))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 10, 11, 12, 13))
	assertExists(t, result, "cars.colors", positions(1, 10, 14, 15))

	// Anchors: 15 entries (10 distinct paths)
	require.Len(t, result.Anchors, 15)
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assertAnchor(t, result, "owner.nicknames", positions(3, 4))
	assertAnchor(t, result, "addresses", positions(5))
	assertAnchor(t, result, "addresses.numbers", positions(6, 7))
	assertAnchor(t, result, "tags", positions(8, 9))
	assertAnchor(t, result, "cars", positions(10))
	assertAnchor(t, result, "cars.tires", positions(11))
	assertAnchor(t, result, "cars.tires.radiuses", positions(12, 13))
	assertAnchor(t, result, "cars.colors", positions(14, 15))
}

// TestAssignPositions_Doc124Full tests complete doc124 (Justin). Every
// element's positions = chain + self + descendant selves; anchors are
// exact. 17 elements total (e1..e17). Covers tires[1] without radiuses and
// Kia tires[0] with radiuses=[].
//
//	root            (chain=∅,                 self=e1,  desc={e2..e17}) → {e1..e17}
//	owner           (chain={e1},              self=e2,  desc={e3})       → {e1, e2, e3}
//	├─ firstname/lastname            inherits owner                       → {e1, e2, e3}
//	└─ nicknames[0]="watch"        chain={e1, e2},     self=e3            → {e1, e2, e3}
//	addresses[0]    (chain={e1},              self=e4,  desc={e5})        → {e1, e4, e5}
//	├─ city/postcode (Madrid/28001) inherits addr[0]                       → {e1, e4, e5}
//	└─ numbers[0]=124              chain={e1, e4},     self=e5            → {e1, e4, e5}
//	addresses[1]    (chain={e1},              self=e6,  desc=∅)           → {e1, e6}
//	├─ city/postcode (London/SW1)    inherits addr[1]                      → {e1, e6}
//	tags[0..2]                      chain={e1},         self=e7/e8/e9     → {e1, e7|e8|e9}
//	cars[0] Audi    (chain={e1},              self=e10, desc={e11..e14})  → {e1, e10..e14}
//	├─ make="Audi"                  inherits car[0]                        → {e1, e10..e14}
//	├─ tires[0]     (chain={e1, e10},         self=e11, desc={e12, e13})  → {e1, e10..e13}
//	│  ├─ width=205                 inherits tire[0]                       → {e1, e10..e13}
//	│  ├─ radiuses[0]=17            chain={e1, e10, e11}, self=e12         → {e1, e10, e11, e12}
//	│  └─ radiuses[1]=18            chain={e1, e10, e11}, self=e13         → {e1, e10, e11, e13}
//	└─ tires[1]     (chain={e1, e10},         self=e14, desc=∅)           → {e1, e10, e14}
//	    └─ width=225                inherits tire[1]                       → {e1, e10, e14}
//	cars[1] Kia     (chain={e1},              self=e15, desc={e16, e17})  → {e1, e15, e16, e17}
//	├─ make="Kia"                   inherits car[1]                        → {e1, e15, e16, e17}
//	├─ tires[0]     (chain={e1, e15},         self=e16, desc=∅, radiuses=[]) → {e1, e15, e16}
//	│  └─ width=195                 inherits tire                          → {e1, e15, e16}
//	└─ colors[0]="white"           chain={e1, e15},    self=e17            → {e1, e15, e17}
//	name="subdoc_124"               inherits root                          → {e1..e17}
func TestAssignPositions_Doc124Full(t *testing.T) {
	schemas := map[string][]*models.NestedProperty{
		"full_schema":    fullNestedSchema(),
		"minimal_schema": doc124Schema(),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			assertDoc124(t, schema)
		})
	}
}

func assertDoc124(t *testing.T, schema []*models.NestedProperty) {
	t.Helper()
	prop := topLevelObject("nestedObject", schema...)

	value := map[string]any{
		"name": "subdoc_124",
		"owner": map[string]any{
			"firstname": "Justin",
			"lastname":  "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{
				"city":     "Madrid",
				"postcode": "28001",
				"numbers":  []any{float64(124)},
			},
			map[string]any{
				"city":     "London",
				"postcode": "SW1",
			},
		},
		"tags": []any{"german", "japanese", "sedan"},
		"cars": []any{
			map[string]any{
				"make": "Audi",
				"tires": []any{
					map[string]any{
						"width":    float64(205),
						"radiuses": []any{float64(17), float64(18)},
					},
					map[string]any{
						"width": float64(225),
					},
				},
			},
			map[string]any{
				"make": "Kia",
				"tires": []any{
					map[string]any{
						"width":    float64(195),
						"radiuses": []any{},
					},
				},
				"colors": []any{"white"},
			},
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Values: 20 entries
	require.Len(t, result.Values, 20)
	assertValue(t, result, "owner.nicknames", "watch", positions(1, 2, 3))
	assertValue(t, result, "owner.firstname", "Justin", positions(1, 2, 3))
	assertValue(t, result, "owner.lastname", "Time", positions(1, 2, 3))
	assertValue(t, result, "addresses.numbers", float64(124), positions(1, 4, 5))
	assertValue(t, result, "addresses.city", "Madrid", positions(1, 4, 5))
	assertValue(t, result, "addresses.postcode", "28001", positions(1, 4, 5))
	assertValue(t, result, "addresses.city", "London", positions(1, 6))
	assertValue(t, result, "addresses.postcode", "SW1", positions(1, 6))
	assertValue(t, result, "tags", "german", positions(1, 7))
	assertValue(t, result, "tags", "japanese", positions(1, 8))
	assertValue(t, result, "tags", "sedan", positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(17), positions(1, 10, 11, 12))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 10, 11, 13))
	assertValue(t, result, "cars.tires.width", float64(205), positions(1, 10, 11, 12, 13))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 10, 14))
	assertValue(t, result, "cars.tires.width", float64(195), positions(1, 15, 16))
	assertValue(t, result, "cars.make", "Audi", positions(1, 10, 11, 12, 13, 14))
	assertValue(t, result, "cars.make", "Kia", positions(1, 15, 16, 17))
	assertValue(t, result, "cars.colors", "white", positions(1, 15, 17))
	assertValue(t, result, "name", "subdoc_124", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))

	// Idx: 17 entries (16 sub-array + 1 root, aggregated by path+index).
	// _idx.cars.tires[0] aggregates Audi t0 (chain {e1, e10}, subtree
	// {e11, e12, e13}) with Kia t0 (chain {e1, e15}, subtree {e16}).
	// Chain bit e1 appears twice in the aggregate.
	require.Len(t, result.Idx, 17)
	assertIdx(t, result, "owner", 0, positions(1, 2, 3))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 2, 3))
	assertIdx(t, result, "addresses", 0, positions(1, 4, 5))
	assertIdx(t, result, "addresses", 1, positions(1, 6))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 4, 5))
	assertIdx(t, result, "tags", 0, positions(1, 7))
	assertIdx(t, result, "tags", 1, positions(1, 8))
	assertIdx(t, result, "tags", 2, positions(1, 9))
	assertIdx(t, result, "cars", 0, positions(1, 10, 11, 12, 13, 14))
	assertIdx(t, result, "cars", 1, positions(1, 15, 16, 17))
	// Two emissions (Audi t0 + Kia t0): chain bit e1 appears twice in the aggregate.
	assertIdx(t, result, "cars.tires", 0, positions(1, 1, 10, 11, 12, 13, 15, 16))
	assertIdx(t, result, "cars.tires", 1, positions(1, 10, 14)) // Audi t1 only
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 10, 11, 12))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 10, 11, 13))
	assertIdx(t, result, "cars.colors", 0, positions(1, 15, 17))
	// root-level _idx — chain ∅, subtree = all leaves
	assertIdx(t, result, "", 0, positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))

	// Exists: 23 raw entries, 17 unique paths (aggregated).
	// _exists.cars.tires aggregates Audi's call ({e1, e10} + {e11..e14})
	// with Kia's call ({e1, e15} + {e16}).
	require.Len(t, result.Exists, 23)
	assertExists(t, result, "", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	assertExists(t, result, "name", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	assertExists(t, result, "owner", positions(1, 2, 3))
	assertExists(t, result, "owner.firstname", positions(1, 2, 3))
	assertExists(t, result, "owner.lastname", positions(1, 2, 3))
	assertExists(t, result, "owner.nicknames", positions(1, 2, 3))
	assertExists(t, result, "addresses", positions(1, 4, 5, 6))
	// city/postcode emit once per address (Phase 3 inside each addr element).
	// Two emissions → chain bit e1 appears twice in the aggregate.
	assertExists(t, result, "addresses.city", positions(1, 1, 4, 5, 6))
	assertExists(t, result, "addresses.postcode", positions(1, 1, 4, 5, 6))
	assertExists(t, result, "addresses.numbers", positions(1, 4, 5))
	assertExists(t, result, "tags", positions(1, 7, 8, 9))
	assertExists(t, result, "cars", positions(1, 10, 11, 12, 13, 14, 15, 16, 17))
	// make emits once per car (2 cars) → chain bit e1 twice.
	assertExists(t, result, "cars.make", positions(1, 1, 10, 11, 12, 13, 14, 15, 16, 17))
	// walkNestedArray for tires is called once per car (2 cars) → chain bit e1 twice.
	assertExists(t, result, "cars.tires", positions(1, 1, 10, 11, 12, 13, 14, 15, 16))
	// width emits once per tire (Audi t0, Audi t1, Kia t0 = 3 tires) → chain bit e1 thrice;
	// chain bit e10 appears in both Audi tires.
	assertExists(t, result, "cars.tires.width", positions(1, 1, 1, 10, 10, 11, 12, 13, 14, 15, 16))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 10, 11, 12, 13))
	assertExists(t, result, "cars.colors", positions(1, 15, 17))

	// Anchors: 17 entries (10 distinct paths)
	require.Len(t, result.Anchors, 17)
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assertAnchor(t, result, "owner.nicknames", positions(3))
	assertAnchor(t, result, "addresses", positions(4, 6))
	assertAnchor(t, result, "addresses.numbers", positions(5))
	assertAnchor(t, result, "tags", positions(7, 8, 9))
	assertAnchor(t, result, "cars", positions(10, 15))
	assertAnchor(t, result, "cars.tires", positions(11, 14, 16))
	assertAnchor(t, result, "cars.tires.radiuses", positions(12, 13))
	assertAnchor(t, result, "cars.colors", positions(17))
}

// TestAssignPositions_Doc125Full tests complete doc125 (Anna). Every
// element's positions = chain + self + descendant selves; anchors are
// exact. 13 elements total (e1..e13). Owner has no nicknames so its
// positions reduce to chain + self.
//
//	root              (chain=∅,             self=e1,  desc={e2..e13}) → {e1..e13}
//	owner             (chain={e1},          self=e2,  desc=∅)         → {e1, e2}
//	├─ firstname/lastname              inherits owner                  → {e1, e2}
//	addresses[0]      (chain={e1},          self=e3,  desc={e4})       → {e1, e3, e4}
//	├─ city/postcode                   inherits addr[0]                → {e1, e3, e4}
//	└─ numbers[0]=125               chain={e1, e3},  self=e4           → {e1, e3, e4}
//	tags[0]="electric"              chain={e1},      self=e5           → {e1, e5}
//	cars[0] Tesla     (chain={e1},          self=e6,  desc={e7..e13})  → {e1, e6..e13}
//	├─ make="Tesla"                    inherits car[0]                  → {e1, e6..e13}
//	├─ tires[0]       (chain={e1, e6},     self=e7,  desc={e8..e10})  → {e1, e6, e7..e10}
//	│  ├─ width=245                    inherits tire[0]                 → {e1, e6, e7..e10}
//	│  ├─ radiuses[0]=18           chain={e1, e6, e7}, self=e8         → {e1, e6, e7, e8}
//	│  ├─ radiuses[1]=19           chain={e1, e6, e7}, self=e9         → {e1, e6, e7, e9}
//	│  └─ radiuses[2]=20           chain={e1, e6, e7}, self=e10        → {e1, e6, e7, e10}
//	├─ accessories[0] charger (chain={e1, e6}, self=e11, desc=∅)        → {e1, e6, e11}
//	│  └─ type="charger"               inherits acc[0]                  → {e1, e6, e11}
//	├─ accessories[1] mats    (chain={e1, e6}, self=e12, desc=∅)        → {e1, e6, e12}
//	│  └─ type="mats"                  inherits acc[1]                  → {e1, e6, e12}
//	└─ colors[0]="yellow"          chain={e1, e6},  self=e13           → {e1, e6, e13}
//	name="subdoc_125"                  inherits root                    → {e1..e13}
func TestAssignPositions_Doc125Full(t *testing.T) {
	schemas := map[string][]*models.NestedProperty{
		"full_schema":    fullNestedSchema(),
		"minimal_schema": doc125Schema(),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			assertDoc125(t, schema)
		})
	}
}

func assertDoc125(t *testing.T, schema []*models.NestedProperty) {
	t.Helper()
	prop := topLevelObject("nestedObject", schema...)

	value := map[string]any{
		"name": "subdoc_125",
		"owner": map[string]any{
			"firstname": "Anna",
			"lastname":  "Wanna",
		},
		"addresses": []any{
			map[string]any{
				"city":     "Paris",
				"postcode": "75001",
				"numbers":  []any{float64(125)},
			},
		},
		"tags": []any{"electric"},
		"cars": []any{
			map[string]any{
				"make": "Tesla",
				"tires": []any{
					map[string]any{
						"width":    float64(245),
						"radiuses": []any{float64(18), float64(19), float64(20)},
					},
				},
				"accessories": []any{
					map[string]any{"type": "charger"},
					map[string]any{"type": "mats"},
				},
				"colors": []any{"yellow"},
			},
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Values: 15 entries
	require.Len(t, result.Values, 15)
	assertValue(t, result, "owner.firstname", "Anna", positions(1, 2))
	assertValue(t, result, "owner.lastname", "Wanna", positions(1, 2))
	assertValue(t, result, "addresses.numbers", float64(125), positions(1, 3, 4))
	assertValue(t, result, "addresses.city", "Paris", positions(1, 3, 4))
	assertValue(t, result, "addresses.postcode", "75001", positions(1, 3, 4))
	assertValue(t, result, "tags", "electric", positions(1, 5))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 6, 7, 8))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(1, 6, 7, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(20), positions(1, 6, 7, 10))
	assertValue(t, result, "cars.tires.width", float64(245), positions(1, 6, 7, 8, 9, 10))
	assertValue(t, result, "cars.accessories.type", "charger", positions(1, 6, 11))
	assertValue(t, result, "cars.accessories.type", "mats", positions(1, 6, 12))
	assertValue(t, result, "cars.colors", "yellow", positions(1, 6, 13))
	assertValue(t, result, "cars.make", "Tesla", positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertValue(t, result, "name", "subdoc_125", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

	// Idx: 13 entries (12 sub-array + 1 root; no owner.nicknames — Anna has no nicknames)
	require.Len(t, result.Idx, 13)
	assertIdx(t, result, "owner", 0, positions(1, 2))
	assertIdx(t, result, "addresses", 0, positions(1, 3, 4))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 3, 4))
	assertIdx(t, result, "tags", 0, positions(1, 5))
	assertIdx(t, result, "cars", 0, positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertIdx(t, result, "cars.tires", 0, positions(1, 6, 7, 8, 9, 10))
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 6, 7, 8))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 6, 7, 9))
	assertIdx(t, result, "cars.tires.radiuses", 2, positions(1, 6, 7, 10))
	assertIdx(t, result, "cars.accessories", 0, positions(1, 6, 11))
	assertIdx(t, result, "cars.accessories", 1, positions(1, 6, 12))
	assertIdx(t, result, "cars.colors", 0, positions(1, 6, 13))
	// root-level _idx — chain ∅, subtree = all leaves.
	assertIdx(t, result, "", 0, positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

	// Exists: 19 raw entries, 17 unique paths (no owner.nicknames).
	// accessories.type emits twice (one Phase 3 per acc element) — chain bits e1, e6 each appear twice.
	require.Len(t, result.Exists, 19)
	assertExists(t, result, "", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "name", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "owner", positions(1, 2))
	assertExists(t, result, "owner.firstname", positions(1, 2))
	assertExists(t, result, "owner.lastname", positions(1, 2))
	assertExists(t, result, "addresses", positions(1, 3, 4))
	assertExists(t, result, "addresses.city", positions(1, 3, 4))
	assertExists(t, result, "addresses.postcode", positions(1, 3, 4))
	assertExists(t, result, "addresses.numbers", positions(1, 3, 4))
	assertExists(t, result, "tags", positions(1, 5))
	assertExists(t, result, "cars", positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "cars.make", positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "cars.tires", positions(1, 6, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires.width", positions(1, 6, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 6, 7, 8, 9, 10))
	assertExists(t, result, "cars.accessories", positions(1, 6, 11, 12))
	assertExists(t, result, "cars.accessories.type", positions(1, 1, 6, 6, 11, 12))
	assertExists(t, result, "cars.colors", positions(1, 6, 13))

	// Anchors: 13 entries (10 distinct paths; no owner.nicknames anchor)
	require.Len(t, result.Anchors, 13)
	assertAnchor(t, result, "", positions(1))
	assertAnchor(t, result, "owner", positions(2))
	assertAnchor(t, result, "addresses", positions(3))
	assertAnchor(t, result, "addresses.numbers", positions(4))
	assertAnchor(t, result, "tags", positions(5))
	assertAnchor(t, result, "cars", positions(6))
	assertAnchor(t, result, "cars.tires", positions(7))
	assertAnchor(t, result, "cars.tires.radiuses", positions(8, 9, 10))
	assertAnchor(t, result, "cars.accessories", positions(11, 12))
	assertAnchor(t, result, "cars.colors", positions(13))
}

// TestAssignPositions_Doc999ObjectArray tests the object[] case at the top
// level: two root elements (Justin=elem[0] with 17 positions, Anna=elem[1]
// with 13). A single walker persists across both elements, so Anna's
// elemIdx counter continues from where Justin's left off (e18..e30) rather
// than resetting to e1. Roots interact only via aggregation at shared paths.
//
// elem[0] (Justin) — e1..e17 (mirrors Doc124):
//
//	root (chain=∅, self=e1, desc={e2..e17}) → {e1..e17}
//	owner (chain={e1}, self=e2, desc={e3}) → {e1, e2, e3}
//	…same structure as Doc124 with e1..e17…
//
// elem[1] (Anna) — e18..e30 (mirrors Doc125, but counter continues):
//
//	root (chain=∅, self=e18, desc={e19..e30}) → {e18..e30}
//	owner (chain={e18}, self=e19, desc=∅) → {e18, e19}
//	addresses[0] (chain={e18}, self=e20, desc={e21}) → {e18, e20, e21}
//	tags[0]="electric" chain={e18}, self=e22 → {e18, e22}
//	cars[0] Tesla (chain={e18}, self=e23, desc={e24..e30}) → {e18, e23..e30}
//	…tires[0] self=e24, radiuses e25/e26/e27, accessories e28/e29, colors e30…
func TestAssignPositions_Doc999ObjectArray(t *testing.T) {
	// doc999 is object[] containing doc124+doc125 data, so the full schema
	// is the union of both. The minimal schema for doc999 is also the full
	// schema since it contains data from both docs.
	schemas := map[string][]*models.NestedProperty{
		"full_schema":    fullNestedSchema(),
		"minimal_schema": fullNestedSchema(),
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			assertDoc999(t, schema)
		})
	}
}

func assertDoc999(t *testing.T, schema []*models.NestedProperty) {
	t.Helper()
	prop := topLevelObjectArray("nestedArray", schema...)

	value := []any{
		// elem[0]: Justin (same data as doc124)
		map[string]any{
			"name": "subdoc_124",
			"owner": map[string]any{
				"firstname": "Justin",
				"lastname":  "Time",
				"nicknames": []any{"watch"},
			},
			"addresses": []any{
				map[string]any{
					"city": "Madrid", "postcode": "28001",
					"numbers": []any{float64(124)},
				},
				map[string]any{
					"city": "London", "postcode": "SW1",
				},
			},
			"tags": []any{"german", "japanese", "sedan"},
			"cars": []any{
				map[string]any{
					"make": "Audi",
					"tires": []any{
						map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
						map[string]any{"width": float64(225)},
					},
				},
				map[string]any{
					"make": "Kia",
					"tires": []any{
						map[string]any{"width": float64(195), "radiuses": []any{}},
					},
					"colors": []any{"white"},
				},
			},
		},
		// elem[1]: Anna (same data as doc125; elemIdx continues from e18)
		map[string]any{
			"name": "subdoc_125",
			"owner": map[string]any{
				"firstname": "Anna",
				"lastname":  "Wanna",
			},
			"addresses": []any{
				map[string]any{
					"city": "Paris", "postcode": "75001",
					"numbers": []any{float64(125)},
				},
			},
			"tags": []any{"electric"},
			"cars": []any{
				map[string]any{
					"make": "Tesla",
					"tires": []any{
						map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}},
					},
					"accessories": []any{
						map[string]any{"type": "charger"},
						map[string]any{"type": "mats"},
					},
					"colors": []any{"yellow"},
				},
			},
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Values: 35 (20 from elem[0]/Justin + 15 from elem[1]/Anna).
	// assertValue matches a single Value entry by (Path, Value, Positions);
	// no aggregation. Per-entry positions = chain + self + descendants of
	// the owning element.
	require.Len(t, result.Values, 35)
	// elem[0] (Justin) — mirrors Doc124 (e1..e17)
	assertValue(t, result, "owner.nicknames", "watch", positions(1, 2, 3))
	assertValue(t, result, "owner.firstname", "Justin", positions(1, 2, 3))
	assertValue(t, result, "owner.lastname", "Time", positions(1, 2, 3))
	assertValue(t, result, "addresses.numbers", float64(124), positions(1, 4, 5))
	assertValue(t, result, "addresses.city", "Madrid", positions(1, 4, 5))
	assertValue(t, result, "addresses.postcode", "28001", positions(1, 4, 5))
	assertValue(t, result, "addresses.city", "London", positions(1, 6))
	assertValue(t, result, "addresses.postcode", "SW1", positions(1, 6))
	assertValue(t, result, "tags", "german", positions(1, 7))
	assertValue(t, result, "tags", "japanese", positions(1, 8))
	assertValue(t, result, "tags", "sedan", positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(17), positions(1, 10, 11, 12))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 10, 11, 13))
	assertValue(t, result, "cars.tires.width", float64(205), positions(1, 10, 11, 12, 13))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 10, 14))
	assertValue(t, result, "cars.tires.width", float64(195), positions(1, 15, 16))
	assertValue(t, result, "cars.make", "Audi", positions(1, 10, 11, 12, 13, 14))
	assertValue(t, result, "cars.make", "Kia", positions(1, 15, 16, 17))
	assertValue(t, result, "cars.colors", "white", positions(1, 15, 17))
	assertValue(t, result, "name", "subdoc_124", positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	// elem[1] (Anna) — mirrors Doc125 but elemIdx continues from e18
	assertValue(t, result, "owner.firstname", "Anna", positions(18, 19))
	assertValue(t, result, "owner.lastname", "Wanna", positions(18, 19))
	assertValue(t, result, "addresses.numbers", float64(125), positions(18, 20, 21))
	assertValue(t, result, "addresses.city", "Paris", positions(18, 20, 21))
	assertValue(t, result, "addresses.postcode", "75001", positions(18, 20, 21))
	assertValue(t, result, "tags", "electric", positions(18, 22))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(18, 23, 24, 25))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(18, 23, 24, 26))
	assertValue(t, result, "cars.tires.radiuses", float64(20), positions(18, 23, 24, 27))
	assertValue(t, result, "cars.tires.width", float64(245), positions(18, 23, 24, 25, 26, 27))
	assertValue(t, result, "cars.accessories.type", "charger", positions(18, 23, 28))
	assertValue(t, result, "cars.accessories.type", "mats", positions(18, 23, 29))
	assertValue(t, result, "cars.colors", "yellow", positions(18, 23, 30))
	assertValue(t, result, "cars.make", "Tesla", positions(18, 23, 24, 25, 26, 27, 28, 29, 30))
	assertValue(t, result, "name", "subdoc_125", positions(18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30))

	// Idx: 30 (17 from elem[0]/Justin + 13 from elem[1]/Anna). Per-(path,index)
	// entries from elem[0] and elem[1] aggregate; chain bits within an element
	// repeat once per emission.
	require.Len(t, result.Idx, 30)
	assertIdx(t, result, "owner", 0, append(positions(1, 2, 3), positions(18, 19)...))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 2, 3))
	assertIdx(t, result, "addresses", 0, append(positions(1, 4, 5), positions(18, 20, 21)...))
	assertIdx(t, result, "addresses", 1, positions(1, 6))
	assertIdx(t, result, "addresses.numbers", 0, append(positions(1, 4, 5), positions(18, 20, 21)...))
	assertIdx(t, result, "tags", 0, append(positions(1, 7), positions(18, 22)...))
	assertIdx(t, result, "tags", 1, positions(1, 8))
	assertIdx(t, result, "tags", 2, positions(1, 9))
	assertIdx(t, result, "cars", 0, append(positions(1, 10, 11, 12, 13, 14), positions(18, 23, 24, 25, 26, 27, 28, 29, 30)...))
	assertIdx(t, result, "cars", 1, positions(1, 15, 16, 17))
	// cars.tires[0]: elem[0] Audi (chain {e1,e10}) + elem[0] Kia (chain {e1,e15}) + elem[1] Tesla (chain {e18,e23}).
	assertIdx(t, result, "cars.tires", 0,
		append(append(positions(1, 10, 11, 12, 13), positions(1, 15, 16)...),
			positions(18, 23, 24, 25, 26, 27)...))
	assertIdx(t, result, "cars.tires", 1, positions(1, 10, 14))
	assertIdx(t, result, "cars.tires.radiuses", 0, append(positions(1, 10, 11, 12), positions(18, 23, 24, 25)...))
	assertIdx(t, result, "cars.tires.radiuses", 1, append(positions(1, 10, 11, 13), positions(18, 23, 24, 26)...))
	assertIdx(t, result, "cars.tires.radiuses", 2, positions(18, 23, 24, 27))
	assertIdx(t, result, "cars.accessories", 0, positions(18, 23, 28))
	assertIdx(t, result, "cars.accessories", 1, positions(18, 23, 29))
	assertIdx(t, result, "cars.colors", 0, append(positions(1, 15, 17), positions(18, 23, 30)...))
	// root-level _idx entries for arr[N] positional filtering; chain ∅ at root.
	assertIdx(t, result, "", 0, positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	assertIdx(t, result, "", 1, positions(18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30))

	// Exists: 41 raw entries (23 from elem[0] + 17 from elem[1] + 1 root-level).
	// Each emission contributes its own chain bits to the aggregated slice,
	// so paths emitted multiple times within an element see that element's
	// chain repeated.
	require.Len(t, result.Exists, 41)
	allPositions := append(
		positions(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
		positions(18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)...,
	)
	// "" emitted once at the AssignPositions root level (union of root subtreeSelves).
	assertExists(t, result, "", allPositions)
	// "name" emitted in Phase 3 of each root walkObject — two emissions, same union.
	assertExists(t, result, "name", allPositions)
	// owner / owner.firstname / owner.lastname: one emission per root.
	assertExists(t, result, "owner", append(positions(1, 2, 3), positions(18, 19)...))
	assertExists(t, result, "owner.firstname", append(positions(1, 2, 3), positions(18, 19)...))
	assertExists(t, result, "owner.lastname", append(positions(1, 2, 3), positions(18, 19)...))
	// nicknames: only elem[0] has them (1 emission).
	assertExists(t, result, "owner.nicknames", positions(1, 2, 3))
	// addresses: one walkNestedArray emission per root.
	assertExists(t, result, "addresses", append(positions(1, 4, 5, 6), positions(18, 20, 21)...))
	// addresses.city / postcode: Phase 3 per addr — elem[0] has 2 addrs, elem[1] has 1.
	// elem[0]'s chain bit e1 appears twice; elem[1]'s e18 appears once.
	assertExists(t, result, "addresses.city",
		append(positions(1, 1, 4, 5, 6), positions(18, 20, 21)...))
	assertExists(t, result, "addresses.postcode",
		append(positions(1, 1, 4, 5, 6), positions(18, 20, 21)...))
	// addresses.numbers: elem[0]'s addr[0] emits; addr[1] doesn't (no numbers); elem[1]'s addr[0] emits.
	assertExists(t, result, "addresses.numbers",
		append(positions(1, 4, 5), positions(18, 20, 21)...))
	// tags: one walkScalarArray emission per root.
	assertExists(t, result, "tags", append(positions(1, 7, 8, 9), positions(18, 22)...))
	// cars: one walkNestedArray emission per root.
	assertExists(t, result, "cars",
		append(positions(1, 10, 11, 12, 13, 14, 15, 16, 17), positions(18, 23, 24, 25, 26, 27, 28, 29, 30)...))
	// cars.make: Phase 3 per car — elem[0] has 2 cars (Audi+Kia), elem[1] has 1 (Tesla).
	// elem[0]'s chain bit e1 appears twice; elem[1]'s once.
	assertExists(t, result, "cars.make",
		append(positions(1, 1, 10, 11, 12, 13, 14, 15, 16, 17), positions(18, 23, 24, 25, 26, 27, 28, 29, 30)...))
	// cars.tires: walkNestedArray per car — elem[0] has 2 cars, elem[1] has 1.
	// elem[0]'s chain bit e1 appears twice; elem[1]'s once.
	assertExists(t, result, "cars.tires",
		append(positions(1, 1, 10, 11, 12, 13, 14, 15, 16), positions(18, 23, 24, 25, 26, 27)...))
	// cars.tires.width: Phase 3 per tire — elem[0] has 3 tires (Audi t0, Audi t1, Kia t0), elem[1] has 1.
	// elem[0]'s e1 ×3, e10 ×2 (Audi's two tires share it); elem[1]'s e18 ×1, e23 ×1.
	assertExists(t, result, "cars.tires.width",
		append(positions(1, 1, 1, 10, 10, 11, 12, 13, 14, 15, 16), positions(18, 23, 24, 25, 26, 27)...))
	// cars.tires.radiuses: walkScalarArray emits only when array non-empty.
	// elem[0]: Audi t0 emits; Audi t1 (no radiuses) and Kia t0 (radiuses=[]) don't.
	// elem[1]: Tesla t0 emits.
	assertExists(t, result, "cars.tires.radiuses",
		append(positions(1, 10, 11, 12, 13), positions(18, 23, 24, 25, 26, 27)...))
	// cars.accessories: only elem[1] has accessories.
	assertExists(t, result, "cars.accessories", positions(18, 23, 28, 29))
	// cars.accessories.type: Phase 3 per acc — only elem[1] has 2 acc elements.
	// elem[1]'s chain bits e18, e23 each appear twice.
	assertExists(t, result, "cars.accessories.type", positions(18, 18, 23, 23, 28, 29))
	// cars.colors: walkScalarArray per car with colors — elem[0] Kia, elem[1] Tesla.
	assertExists(t, result, "cars.colors", append(positions(1, 15, 17), positions(18, 23, 30)...))

	// Anchors: 30 entries (17 from elem[0] + 13 from elem[1])
	require.Len(t, result.Anchors, 30)
	assertAnchor(t, result, "", append(positions(1), positions(18)...))
	assertAnchor(t, result, "owner", append(positions(2), positions(19)...))
	assertAnchor(t, result, "owner.nicknames", positions(3))
	assertAnchor(t, result, "addresses", append(positions(4, 6), positions(20)...))
	assertAnchor(t, result, "addresses.numbers", append(positions(5), positions(21)...))
	assertAnchor(t, result, "tags", append(positions(7, 8, 9), positions(22)...))
	assertAnchor(t, result, "cars", append(positions(10, 15), positions(23)...))
	assertAnchor(t, result, "cars.tires", append(positions(11, 14, 16), positions(24)...))
	assertAnchor(t, result, "cars.tires.radiuses", append(positions(12, 13), positions(25, 26, 27)...))
	assertAnchor(t, result, "cars.accessories", positions(28, 29))
	assertAnchor(t, result, "cars.colors", append(positions(17), positions(30)...))
}

// TestAssignPositions_MultiRootElemIdxContinuous pins that the element
// index counter advances globally across all top-level elements and never
// resets per root. Two root elements share the same walker:
//
//	elem[0]: self=e1, tags[0]="x"→e2, tags[1]="y"→e3, name="a" inherits {e1,e2,e3}
//	elem[1]: self=e4 (NOT e1), tags[0]="z"→e5, name="b" inherits {e4,e5}
//
// If the counter reset per root, elem[1]'s self would be e1 and tags[0]
// would be e2, producing positions(1, 2) instead of positions(4, 5) for
// name="b". That would also corrupt LiftToAncestor: elem[1]'s self at e4
// must be strictly greater than Justin's subtree (e1..e3) for the
// predecessor scan to find elem[1] rather than Justin's nodes.
func TestAssignPositions_MultiRootElemIdxContinuous(t *testing.T) {
	prop := topLevelObjectArray("objects",
		textProp("name"),
		textArrayProp("tags"),
	)

	value := []any{
		map[string]any{"name": "a", "tags": []any{"x", "y"}},
		map[string]any{"name": "b", "tags": []any{"z"}},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// elem[0]: name="a" inherits elementPositions={e1, e2, e3}
	assertValue(t, result, "name", "a", positions(1, 2, 3))
	assertValue(t, result, "tags", "x", positions(1, 2))
	assertValue(t, result, "tags", "y", positions(1, 3))

	// elem[1]: self marker must be e4 (counter continues, not reset).
	// If counter reset, name="b" would carry positions(1, 2) = {e1, e2}.
	assertValue(t, result, "name", "b", positions(4, 5))
	assertValue(t, result, "tags", "z", positions(4, 5))

	// Root-level _idx: each element's subtree selves.
	assertIdx(t, result, "", 0, positions(1, 2, 3))
	assertIdx(t, result, "", 1, positions(4, 5))

	// Anchor markers span both roots without collision.
	assertAnchor(t, result, "", positions(1, 4))
	assertAnchor(t, result, "tags", positions(2, 3, 5))
}

// TestBuildLevelSchema_PreservesOrder verifies that buildLevelSchema splits
// properties into the two sub-slices while preserving their original relative
// order within each group. Correct ordering is load-bearing: the walker
// iterates arrayOrNested in Phase 1 (claiming elemIdx) then scalars in
// Phase 3, so any reordering would corrupt LiftToAncestor predecessor scans.
//
// Schema: text=a, text[]=b, object=c(text=x), int=d, int[]=e
// Expected split:
//
//	arrayOrNested: [b, c, e]  — scalar-arrays and nested in original relative order
//	scalars:       [a, d]     — plain scalars in original relative order
func TestBuildLevelSchema_PreservesOrder(t *testing.T) {
	nestedProps := []*models.NestedProperty{
		textProp("a"),
		textArrayProp("b"),
		objectProp("c", textProp("x")),
		intProp("d"),
		intArrayProp("e"),
	}
	ls := buildLevelSchema("", nestedProps)

	// arrayOrNested: b (scalarArray), c (nested), e (scalarArray) in original order
	require.Len(t, ls.arrayOrNested, 3)

	assert.Equal(t, "b", ls.arrayOrNested[0].name)
	assert.Equal(t, propKindScalarArray, ls.arrayOrNested[0].kind)
	assert.Equal(t, "b", ls.arrayOrNested[0].path)

	assert.Equal(t, "c", ls.arrayOrNested[1].name)
	assert.Equal(t, propKindNested, ls.arrayOrNested[1].kind)
	assert.Equal(t, "c", ls.arrayOrNested[1].path)

	assert.Equal(t, "e", ls.arrayOrNested[2].name)
	assert.Equal(t, propKindScalarArray, ls.arrayOrNested[2].kind)
	assert.Equal(t, "e", ls.arrayOrNested[2].path)

	// scalars: a (scalar), d (scalar) in original order
	require.Len(t, ls.scalars, 2)

	assert.Equal(t, "a", ls.scalars[0].name)
	assert.Equal(t, propKindScalar, ls.scalars[0].kind)
	assert.Equal(t, "a", ls.scalars[0].path)

	assert.Equal(t, "d", ls.scalars[1].name)
	assert.Equal(t, propKindScalar, ls.scalars[1].kind)
	assert.Equal(t, "d", ls.scalars[1].path)

	// Children of "c" must be built recursively; "x" is a plain scalar.
	require.Len(t, ls.arrayOrNested[1].children.scalars, 1)
	assert.Empty(t, ls.arrayOrNested[1].children.arrayOrNested)
	assert.Equal(t, "x", ls.arrayOrNested[1].children.scalars[0].name)
	assert.Equal(t, "c.x", ls.arrayOrNested[1].children.scalars[0].path)

	// maxDepth recurrence:
	//   c.children has only scalars → maxDepth=0 (no arrayOrNested in loop).
	//   Root loop:
	//     b (scalarArray): conservative branch fires, depth→1.
	//     c (nested): d = 0+1 = 1; not > depth(1), no update.
	//     e (scalarArray): 1 > depth(1) is false, no update.
	//   Root ls.maxDepth = 1.
	assert.Equal(t, 0, ls.arrayOrNested[1].children.maxDepth, "c's children are all scalars — maxDepth must be 0")
	assert.Equal(t, 1, ls.maxDepth, "scalar-array b and nested c both contribute depth 1 — root maxDepth must be 1")

	// With a prefix, paths include the prefix via joinPath.
	ls2 := buildLevelSchema("root", nestedProps)
	assert.Equal(t, "root.b", ls2.arrayOrNested[0].path)
	assert.Equal(t, "root.c", ls2.arrayOrNested[1].path)
	assert.Equal(t, "root.c.x", ls2.arrayOrNested[1].children.scalars[0].path)
	assert.Equal(t, "root.a", ls2.scalars[0].path)
}
