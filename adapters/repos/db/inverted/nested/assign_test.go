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

// positions builds encoded position uint64s for given root and leaf indices, with docID=0.
func positions(root uint16, leaves ...uint16) []uint64 {
	out := make([]uint64, len(leaves))
	for i, l := range leaves {
		out[i] = Encode(root, l, 0)
	}
	return out
}

// assertValue checks that a value at the given path with the given raw value
// and expected positions exists. Matches on path+value+positions to handle
// duplicate values across different roots (e.g. float64(18) in r1 and r2).
func assertValue(t *testing.T, result *AssignResult, path string, value any, expected []uint64) {
	t.Helper()
	for _, v := range result.Values {
		if v.Path == path && v.Value == value && assert.ObjectsAreEqual(expected, v.Positions) {
			return
		}
	}
	t.Errorf("value %v with positions %v not found at path %s", value, expected, path)
}

// assertIdx checks the aggregated positions for _idx entries at a given path
// and index. Multiple raw entries for the same path+index (from different
// parent array elements) are merged, matching what the write path does.
func assertIdx(t *testing.T, result *AssignResult, path string, index int, expected []uint64) {
	t.Helper()
	var actual []uint64
	for _, idx := range result.Idx {
		if idx.Path == path && idx.Index == index {
			actual = append(actual, idx.Positions...)
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
func assertExists(t *testing.T, result *AssignResult, path string, expected []uint64) {
	t.Helper()
	var actual []uint64
	for _, e := range result.Exists {
		if e.Path == path {
			actual = append(actual, e.Positions...)
		}
	}
	if len(actual) == 0 {
		t.Errorf("_exists.%s not found", path)
		return
	}
	assert.ElementsMatch(t, expected, actual, "_exists.%s", path)
}

// assertAnchor checks the aggregated marker positions for _anchor entries at
// a given path. Multiple raw entries (one per element instance) are merged,
// matching what the write path does.
func assertAnchor(t *testing.T, result *AssignResult, path string, expected []uint64) {
	t.Helper()
	var actual []uint64
	for _, a := range result.Anchors {
		if a.Path == path {
			actual = append(actual, a.Positions...)
		}
	}
	if len(actual) == 0 {
		t.Errorf("_anchor.%s not found", path)
		return
	}
	assert.ElementsMatch(t, expected, actual, "_anchor.%s", path)
}

// findValues returns all PositionedValues for a given path
func findValues(result *AssignResult, path string) []PositionedValue {
	var out []PositionedValue
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
	// children, so its self marker (l1) is the only position. Scalar `name`
	// inherits the element's positions.
	prop := topLevelObject("nested", textProp("name"))
	value := map[string]any{"name": "hello"}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	require.Len(t, result.Values, 1)
	assert.Equal(t, "name", result.Values[0].Path)
	assert.Equal(t, "hello", result.Values[0].Value)
	assert.Equal(t, schema.DataTypeText, result.Values[0].DataType)
	assert.Equal(t, positions(1, 1), result.Values[0].Positions)

	// _exists.name and root _exists
	nameExists := findExists(result, "name")
	require.NotNil(t, nameExists)
	assert.Equal(t, positions(1, 1), nameExists.Positions)

	rootExists := findExists(result, "")
	require.NotNil(t, rootExists)
	assert.Equal(t, positions(1, 1), rootExists.Positions)

	// _anchor.""  = root element's self marker
	assertAnchor(t, result, "", positions(1, 1))
}

func TestAssignPositions_ScalarArray(t *testing.T) {
	// Object with scalar array: tags=["german", "premium"]. Root marker
	// takes l1, then each scalar-array element gets its own marker (l2, l3).
	prop := topLevelObject("nested", textArrayProp("tags"))
	value := map[string]any{
		"tags": []any{"german", "premium"},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	tags := findValues(result, "tags")
	require.Len(t, tags, 2)
	assert.Equal(t, "german", tags[0].Value)
	assert.Equal(t, positions(1, 2), tags[0].Positions)
	assert.Equal(t, "premium", tags[1].Value)
	assert.Equal(t, positions(1, 3), tags[1].Positions)

	// _idx for each element
	idx0 := findIdx(result, "tags", 0)
	require.NotNil(t, idx0)
	assert.Equal(t, positions(1, 2), idx0.Positions)

	idx1 := findIdx(result, "tags", 1)
	require.NotNil(t, idx1)
	assert.Equal(t, positions(1, 3), idx1.Positions)

	// _exists.tags
	tagsExists := findExists(result, "tags")
	require.NotNil(t, tagsExists)
	assert.Equal(t, positions(1, 2, 3), tagsExists.Positions)

	// _anchor — root marker + per-tag-element markers
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "tags", positions(1, 2, 3))
}

// TestAssignPositions_OwnerDoc123 tests position assignment for doc123's
// owner section under per-element-anchor encoding:
//
//	root (self) → l1
//	owner (self+desc) → {l2, l3, l4}
//	├─ firstname="Marsha" → {l2, l3, l4}
//	├─ lastname="Mallow" → {l2, l3, l4}
//	├─ nicknames[0]="Marshmallow" → l3
//	└─ nicknames[1]="M&M" → l4
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

	// nicknames[0]="Marshmallow" → l3, nicknames[1]="M&M" → l4
	nicknames := findValues(result, "owner.nicknames")
	require.Len(t, nicknames, 2)
	assert.Equal(t, "Marshmallow", nicknames[0].Value)
	assert.Equal(t, positions(1, 3), nicknames[0].Positions)
	assert.Equal(t, "M&M", nicknames[1].Value)
	assert.Equal(t, positions(1, 4), nicknames[1].Positions)

	// firstname / lastname inherit owner's positions → {l2, l3, l4}
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, "Marsha", firstnames[0].Value)
	assert.Equal(t, positions(1, 2, 3, 4), firstnames[0].Positions)

	lastnames := findValues(result, "owner.lastname")
	require.Len(t, lastnames, 1)
	assert.Equal(t, "Mallow", lastnames[0].Value)
	assert.Equal(t, positions(1, 2, 3, 4), lastnames[0].Positions)

	// _exists.owner = {l2, l3, l4}
	ownerExists := findExists(result, "owner")
	require.NotNil(t, ownerExists)
	assert.Equal(t, positions(1, 2, 3, 4), ownerExists.Positions)

	// _exists.owner.nicknames = {l3, l4}
	nickExists := findExists(result, "owner.nicknames")
	require.NotNil(t, nickExists)
	assert.Equal(t, positions(1, 3, 4), nickExists.Positions)

	// _idx.owner.nicknames
	assert.Equal(t, positions(1, 3), findIdx(result, "owner.nicknames", 0).Positions)
	assert.Equal(t, positions(1, 4), findIdx(result, "owner.nicknames", 1).Positions)

	// _anchor
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
	assertAnchor(t, result, "owner.nicknames", positions(1, 3, 4))
}

// TestAssignPositions_OwnerDoc125LeafNode tests owner with no nicknames in
// data. Under per-element-anchor encoding owner still gets its own self
// marker even without descendants.
//
//	root (self) → l1
//	owner (self only) → l2
//	├─ firstname="Anna" → {l2}
//	└─ lastname="Wanna" → {l2}
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
			// no nicknames in data; owner still owns its self marker l2
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, positions(1, 2), firstnames[0].Positions)

	lastnames := findValues(result, "owner.lastname")
	require.Len(t, lastnames, 1)
	assert.Equal(t, positions(1, 2), lastnames[0].Positions)

	// _exists.owner = {l2}
	ownerExists := findExists(result, "owner")
	require.NotNil(t, ownerExists)
	assert.Equal(t, positions(1, 2), ownerExists.Positions)

	// No _exists.owner.nicknames (nicknames not present in data)
	assert.Nil(t, findExists(result, "owner.nicknames"))

	// _anchor — root + owner; no nicknames anchors since nicknames absent
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
	assert.Nil(t, findAnchor(result, "owner.nicknames"))
}

// TestAssignPositions_Doc124Addresses tests mixed addresses with and
// without scalar-array descendants under per-element-anchor encoding:
//
//	root (self) → l1
//	owner (self+desc) → {l2, l3}
//	├─ nicknames[0]="watch" → l3
//	addresses[0] (self+desc) → {l4, l5}
//	├─ city="Madrid" → {l4, l5}
//	├─ postcode="28001" → {l4, l5}
//	└─ numbers[0]=124 → l5
//	addresses[1] (self only) → l6
//	├─ city="London" → {l6}
//	└─ postcode="SW1" → {l6}
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

	// owner.nicknames[0]="watch" → l3
	nicknames := findValues(result, "owner.nicknames")
	require.Len(t, nicknames, 1)
	assert.Equal(t, positions(1, 3), nicknames[0].Positions)

	// addresses[0].numbers[0]=124 → l5
	numbers := findValues(result, "addresses.numbers")
	require.Len(t, numbers, 1)
	assert.Equal(t, float64(124), numbers[0].Value)
	assert.Equal(t, positions(1, 5), numbers[0].Positions)

	// addresses[0] self+desc = {l4, l5}; city/postcode inherit
	cities := findValues(result, "addresses.city")
	require.Len(t, cities, 2)
	assert.Equal(t, "Madrid", cities[0].Value)
	assert.Equal(t, positions(1, 4, 5), cities[0].Positions)

	// addresses[1] self only = {l6}; city/postcode inherit
	assert.Equal(t, "London", cities[1].Value)
	assert.Equal(t, positions(1, 6), cities[1].Positions)

	postcodes := findValues(result, "addresses.postcode")
	require.Len(t, postcodes, 2)
	assert.Equal(t, "28001", postcodes[0].Value)
	assert.Equal(t, positions(1, 4, 5), postcodes[0].Positions)
	assert.Equal(t, "SW1", postcodes[1].Value)
	assert.Equal(t, positions(1, 6), postcodes[1].Positions)

	// _idx.addresses[0] = {l4, l5}, _idx.addresses[1] = {l6}
	assert.Equal(t, positions(1, 4, 5), findIdx(result, "addresses", 0).Positions)
	assert.Equal(t, positions(1, 6), findIdx(result, "addresses", 1).Positions)

	// _exists.addresses = {l4, l5, l6}
	addrExists := findExists(result, "addresses")
	require.NotNil(t, addrExists)
	assert.Equal(t, positions(1, 4, 5, 6), addrExists.Positions)

	// _exists.addresses.numbers = {l5} (only addresses[0] has numbers)
	numExists := findExists(result, "addresses.numbers")
	require.NotNil(t, numExists)
	assert.Equal(t, positions(1, 5), numExists.Positions)

	// _anchor — root + owner + per-element markers
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
	assertAnchor(t, result, "owner.nicknames", positions(1, 3))
	assertAnchor(t, result, "addresses", positions(1, 4, 6))
	assertAnchor(t, result, "addresses.numbers", positions(1, 5))
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

	// Empty nicknames produce no descendants; owner's self marker is l2.
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, positions(1, 2), firstnames[0].Positions)

	// No nicknames values
	assert.Empty(t, findValues(result, "owner.nicknames"))

	// _anchor — root + owner; no nicknames anchor since no elements emitted
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
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

// TestAssignPositions_Doc123Full tests complete doc123 (Marsha) under
// per-element-anchor encoding. Every object element owns a self marker
// allocated before its descendants; 15 leaves total (was 10 pre-encoding-
// change). Runs with both full shared schema and minimal doc123-only
// schema to verify that extra properties in the schema don't affect
// position assignment.
//
//	root (self) → l1
//	owner (self+desc) → {l2, l3, l4}
//	├─ firstname="Marsha" → {l2, l3, l4}
//	├─ lastname="Mallow" → {l2, l3, l4}
//	├─ nicknames[0]="Marshmallow" → l3
//	└─ nicknames[1]="M&M" → l4
//	addresses[0] (self+desc) → {l5, l6, l7}
//	├─ city="Berlin" → {l5, l6, l7}
//	├─ postcode="10115" → {l5, l6, l7}
//	├─ numbers[0]=123 → l6
//	└─ numbers[1]=1123 → l7
//	tags[0]="german" → l8
//	tags[1]="premium" → l9
//	cars[0] (self+desc) → {l10..l15}
//	├─ make="BMW" → {l10..l15}
//	├─ tires[0] (self+desc) → {l11, l12, l13}
//	│  ├─ width=225 → {l11, l12, l13}
//	│  ├─ radiuses[0]=18 → l12
//	│  └─ radiuses[1]=19 → l13
//	├─ colors[0]="black" → l14
//	└─ colors[1]="orange" → l15
//	name="subdoc_123" → {l1..l15}
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
	assertValue(t, result, "owner.nicknames", "Marshmallow", positions(1, 3))
	assertValue(t, result, "owner.nicknames", "M&M", positions(1, 4))
	assertValue(t, result, "owner.firstname", "Marsha", positions(1, 2, 3, 4))
	assertValue(t, result, "owner.lastname", "Mallow", positions(1, 2, 3, 4))
	assertValue(t, result, "addresses.numbers", float64(123), positions(1, 6))
	assertValue(t, result, "addresses.numbers", float64(1123), positions(1, 7))
	assertValue(t, result, "addresses.city", "Berlin", positions(1, 5, 6, 7))
	assertValue(t, result, "addresses.postcode", "10115", positions(1, 5, 6, 7))
	assertValue(t, result, "tags", "german", positions(1, 8))
	assertValue(t, result, "tags", "premium", positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 12))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(1, 13))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 11, 12, 13))
	assertValue(t, result, "cars.make", "BMW", positions(1, 10, 11, 12, 13, 14, 15))
	assertValue(t, result, "cars.colors", "black", positions(1, 14))
	assertValue(t, result, "cars.colors", "orange", positions(1, 15))
	assertValue(t, result, "name", "subdoc_123", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))

	// Idx: 15 entries (14 sub-array + 1 root)
	require.Len(t, result.Idx, 15)
	assertIdx(t, result, "owner", 0, positions(1, 2, 3, 4))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 3))
	assertIdx(t, result, "owner.nicknames", 1, positions(1, 4))
	assertIdx(t, result, "addresses", 0, positions(1, 5, 6, 7))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 6))
	assertIdx(t, result, "addresses.numbers", 1, positions(1, 7))
	assertIdx(t, result, "tags", 0, positions(1, 8))
	assertIdx(t, result, "tags", 1, positions(1, 9))
	assertIdx(t, result, "cars", 0, positions(1, 10, 11, 12, 13, 14, 15))
	assertIdx(t, result, "cars.tires", 0, positions(1, 11, 12, 13))
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 12))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 13))
	assertIdx(t, result, "cars.colors", 0, positions(1, 14))
	assertIdx(t, result, "cars.colors", 1, positions(1, 15))
	// root-level _idx entry for arr[N] positional filtering
	assertIdx(t, result, "", 0, positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))

	// Exists: 17 entries
	require.Len(t, result.Exists, 17)
	assertExists(t, result, "", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "name", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "owner", positions(1, 2, 3, 4))
	assertExists(t, result, "owner.firstname", positions(1, 2, 3, 4))
	assertExists(t, result, "owner.lastname", positions(1, 2, 3, 4))
	assertExists(t, result, "owner.nicknames", positions(1, 3, 4))
	assertExists(t, result, "addresses", positions(1, 5, 6, 7))
	assertExists(t, result, "addresses.city", positions(1, 5, 6, 7))
	assertExists(t, result, "addresses.postcode", positions(1, 5, 6, 7))
	assertExists(t, result, "addresses.numbers", positions(1, 6, 7))
	assertExists(t, result, "tags", positions(1, 8, 9))
	assertExists(t, result, "cars", positions(1, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "cars.make", positions(1, 10, 11, 12, 13, 14, 15))
	assertExists(t, result, "cars.tires", positions(1, 11, 12, 13))
	assertExists(t, result, "cars.tires.width", positions(1, 11, 12, 13))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 12, 13))
	assertExists(t, result, "cars.colors", positions(1, 14, 15))

	// Anchors: 15 entries (10 distinct paths)
	require.Len(t, result.Anchors, 15)
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
	assertAnchor(t, result, "owner.nicknames", positions(1, 3, 4))
	assertAnchor(t, result, "addresses", positions(1, 5))
	assertAnchor(t, result, "addresses.numbers", positions(1, 6, 7))
	assertAnchor(t, result, "tags", positions(1, 8, 9))
	assertAnchor(t, result, "cars", positions(1, 10))
	assertAnchor(t, result, "cars.tires", positions(1, 11))
	assertAnchor(t, result, "cars.tires.radiuses", positions(1, 12, 13))
	assertAnchor(t, result, "cars.colors", positions(1, 14, 15))
}

// TestAssignPositions_Doc124Full tests complete doc124 (Justin) under
// per-element-anchor encoding: 17 leaves total (was 11 pre-encoding-
// change). Every object element owns a self marker, including tires[1]
// without radiuses and Kia tires[0] with radiuses=[].
//
//	root (self) → l1
//	owner (self+desc) → {l2, l3}
//	├─ firstname="Justin" → {l2, l3}
//	├─ lastname="Time" → {l2, l3}
//	└─ nicknames[0]="watch" → l3
//	addresses[0] (self+desc) → {l4, l5}
//	├─ city="Madrid" → {l4, l5}
//	├─ postcode="28001" → {l4, l5}
//	└─ numbers[0]=124 → l5
//	addresses[1] (self only) → l6
//	├─ city="London" → {l6}
//	└─ postcode="SW1" → {l6}
//	tags[0]="german" → l7
//	tags[1]="japanese" → l8
//	tags[2]="sedan" → l9
//	cars[0] (self+desc) → {l10..l14}
//	├─ make="Audi" → {l10..l14}
//	├─ tires[0] (self+desc) → {l11, l12, l13}
//	│  ├─ width=205 → {l11, l12, l13}
//	│  ├─ radiuses[0]=17 → l12
//	│  └─ radiuses[1]=18 → l13
//	└─ tires[1] (self only) → l14
//	    └─ width=225 → {l14}
//	cars[1] (self+desc) → {l15, l16, l17}
//	├─ make="Kia" → {l15, l16, l17}
//	├─ tires[0] (self only, radiuses=[]) → l16
//	│  └─ width=195 → {l16}
//	└─ colors[0]="white" → l17
//	name="subdoc_124" → {l1..l17}
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
	assertValue(t, result, "owner.nicknames", "watch", positions(1, 3))
	assertValue(t, result, "owner.firstname", "Justin", positions(1, 2, 3))
	assertValue(t, result, "owner.lastname", "Time", positions(1, 2, 3))
	assertValue(t, result, "addresses.numbers", float64(124), positions(1, 5))
	assertValue(t, result, "addresses.city", "Madrid", positions(1, 4, 5))
	assertValue(t, result, "addresses.postcode", "28001", positions(1, 4, 5))
	assertValue(t, result, "addresses.city", "London", positions(1, 6))
	assertValue(t, result, "addresses.postcode", "SW1", positions(1, 6))
	assertValue(t, result, "tags", "german", positions(1, 7))
	assertValue(t, result, "tags", "japanese", positions(1, 8))
	assertValue(t, result, "tags", "sedan", positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(17), positions(1, 12))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 13))
	assertValue(t, result, "cars.tires.width", float64(205), positions(1, 11, 12, 13))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 14))
	assertValue(t, result, "cars.tires.width", float64(195), positions(1, 16))
	assertValue(t, result, "cars.make", "Audi", positions(1, 10, 11, 12, 13, 14))
	assertValue(t, result, "cars.make", "Kia", positions(1, 15, 16, 17))
	assertValue(t, result, "cars.colors", "white", positions(1, 17))
	assertValue(t, result, "name", "subdoc_124", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))

	// Idx: 17 entries (16 sub-array + 1 root, aggregated by path+index)
	require.Len(t, result.Idx, 17)
	assertIdx(t, result, "owner", 0, positions(1, 2, 3))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 3))
	assertIdx(t, result, "addresses", 0, positions(1, 4, 5))
	assertIdx(t, result, "addresses", 1, positions(1, 6))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 5))
	assertIdx(t, result, "tags", 0, positions(1, 7))
	assertIdx(t, result, "tags", 1, positions(1, 8))
	assertIdx(t, result, "tags", 2, positions(1, 9))
	assertIdx(t, result, "cars", 0, positions(1, 10, 11, 12, 13, 14))
	assertIdx(t, result, "cars", 1, positions(1, 15, 16, 17))
	assertIdx(t, result, "cars.tires", 0, positions(1, 11, 12, 13, 16)) // Audi t0 + Kia t0
	assertIdx(t, result, "cars.tires", 1, positions(1, 14))             // Audi t1 only
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 12))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 13))
	assertIdx(t, result, "cars.colors", 0, positions(1, 17))
	// root-level _idx entry for arr[N] positional filtering
	assertIdx(t, result, "", 0, positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))

	// Exists: 23 raw entries, 17 unique paths (aggregated)
	require.Len(t, result.Exists, 23)
	assertExists(t, result, "", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	assertExists(t, result, "name", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	assertExists(t, result, "owner", positions(1, 2, 3))
	assertExists(t, result, "owner.firstname", positions(1, 2, 3))
	assertExists(t, result, "owner.lastname", positions(1, 2, 3))
	assertExists(t, result, "owner.nicknames", positions(1, 3))
	assertExists(t, result, "addresses", positions(1, 4, 5, 6))
	assertExists(t, result, "addresses.city", positions(1, 4, 5, 6))
	assertExists(t, result, "addresses.postcode", positions(1, 4, 5, 6))
	assertExists(t, result, "addresses.numbers", positions(1, 5))
	assertExists(t, result, "tags", positions(1, 7, 8, 9))
	assertExists(t, result, "cars", positions(1, 10, 11, 12, 13, 14, 15, 16, 17))
	assertExists(t, result, "cars.make", positions(1, 10, 11, 12, 13, 14, 15, 16, 17))
	assertExists(t, result, "cars.tires", positions(1, 11, 12, 13, 14, 16))
	assertExists(t, result, "cars.tires.width", positions(1, 11, 12, 13, 14, 16))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 12, 13))
	assertExists(t, result, "cars.colors", positions(1, 17))

	// Anchors: 17 entries (10 distinct paths)
	require.Len(t, result.Anchors, 17)
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
	assertAnchor(t, result, "owner.nicknames", positions(1, 3))
	assertAnchor(t, result, "addresses", positions(1, 4, 6))
	assertAnchor(t, result, "addresses.numbers", positions(1, 5))
	assertAnchor(t, result, "tags", positions(1, 7, 8, 9))
	assertAnchor(t, result, "cars", positions(1, 10, 15))
	assertAnchor(t, result, "cars.tires", positions(1, 11, 14, 16))
	assertAnchor(t, result, "cars.tires.radiuses", positions(1, 12, 13))
	assertAnchor(t, result, "cars.colors", positions(1, 17))
}

// TestAssignPositions_Doc125Full tests complete doc125 (Anna) under
// per-element-anchor encoding: 13 leaves total (was 9). Owner has no
// nicknames so its element positions are just its self marker.
//
//	root (self) → l1
//	owner (self only) → l2
//	├─ firstname="Anna" → {l2}
//	└─ lastname="Wanna" → {l2}
//	addresses[0] (self+desc) → {l3, l4}
//	├─ city="Paris" → {l3, l4}
//	├─ postcode="75001" → {l3, l4}
//	└─ numbers[0]=125 → l4
//	tags[0]="electric" → l5
//	cars[0] (self+desc) → {l6..l13}
//	├─ make="Tesla" → {l6..l13}
//	├─ tires[0] (self+desc) → {l7..l10}
//	│  ├─ width=245 → {l7..l10}
//	│  ├─ radiuses[0]=18 → l8
//	│  ├─ radiuses[1]=19 → l9
//	│  └─ radiuses[2]=20 → l10
//	├─ accessories[0] (self only) → l11
//	│  └─ type="charger" → {l11}
//	├─ accessories[1] (self only) → l12
//	│  └─ type="mats" → {l12}
//	└─ colors[0]="yellow" → l13
//	name="subdoc_125" → {l1..l13}
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
	assertValue(t, result, "addresses.numbers", float64(125), positions(1, 4))
	assertValue(t, result, "addresses.city", "Paris", positions(1, 3, 4))
	assertValue(t, result, "addresses.postcode", "75001", positions(1, 3, 4))
	assertValue(t, result, "tags", "electric", positions(1, 5))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 8))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(20), positions(1, 10))
	assertValue(t, result, "cars.tires.width", float64(245), positions(1, 7, 8, 9, 10))
	assertValue(t, result, "cars.accessories.type", "charger", positions(1, 11))
	assertValue(t, result, "cars.accessories.type", "mats", positions(1, 12))
	assertValue(t, result, "cars.colors", "yellow", positions(1, 13))
	assertValue(t, result, "cars.make", "Tesla", positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertValue(t, result, "name", "subdoc_125", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

	// Idx: 13 entries (12 sub-array + 1 root; no owner.nicknames — Anna has no nicknames)
	require.Len(t, result.Idx, 13)
	assertIdx(t, result, "owner", 0, positions(1, 2))
	assertIdx(t, result, "addresses", 0, positions(1, 3, 4))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 4))
	assertIdx(t, result, "tags", 0, positions(1, 5))
	assertIdx(t, result, "cars", 0, positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertIdx(t, result, "cars.tires", 0, positions(1, 7, 8, 9, 10))
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 8))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 9))
	assertIdx(t, result, "cars.tires.radiuses", 2, positions(1, 10))
	assertIdx(t, result, "cars.accessories", 0, positions(1, 11))
	assertIdx(t, result, "cars.accessories", 1, positions(1, 12))
	assertIdx(t, result, "cars.colors", 0, positions(1, 13))
	// root-level _idx entry for arr[N] positional filtering
	assertIdx(t, result, "", 0, positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

	// Exists: 19 raw entries, 17 unique paths (no owner.nicknames)
	// accessories.type appears twice (one per leaf element)
	require.Len(t, result.Exists, 19)
	assertExists(t, result, "", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "name", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "owner", positions(1, 2))
	assertExists(t, result, "owner.firstname", positions(1, 2))
	assertExists(t, result, "owner.lastname", positions(1, 2))
	assertExists(t, result, "addresses", positions(1, 3, 4))
	assertExists(t, result, "addresses.city", positions(1, 3, 4))
	assertExists(t, result, "addresses.postcode", positions(1, 3, 4))
	assertExists(t, result, "addresses.numbers", positions(1, 4))
	assertExists(t, result, "tags", positions(1, 5))
	assertExists(t, result, "cars", positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "cars.make", positions(1, 6, 7, 8, 9, 10, 11, 12, 13))
	assertExists(t, result, "cars.tires", positions(1, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires.width", positions(1, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 8, 9, 10))
	assertExists(t, result, "cars.accessories", positions(1, 11, 12))
	assertExists(t, result, "cars.accessories.type", positions(1, 11, 12))
	assertExists(t, result, "cars.colors", positions(1, 13))

	// Anchors: 13 entries (10 distinct paths; no owner.nicknames anchor)
	require.Len(t, result.Anchors, 13)
	assertAnchor(t, result, "", positions(1, 1))
	assertAnchor(t, result, "owner", positions(1, 2))
	assertAnchor(t, result, "addresses", positions(1, 3))
	assertAnchor(t, result, "addresses.numbers", positions(1, 4))
	assertAnchor(t, result, "tags", positions(1, 5))
	assertAnchor(t, result, "cars", positions(1, 6))
	assertAnchor(t, result, "cars.tires", positions(1, 7))
	assertAnchor(t, result, "cars.tires.radiuses", positions(1, 8, 9, 10))
	assertAnchor(t, result, "cars.accessories", positions(1, 11, 12))
	assertAnchor(t, result, "cars.colors", positions(1, 13))
}

// TestAssignPositions_Doc999ObjectArray tests the object[] case under
// per-element-anchor encoding: two root elements (Justin=r1, Anna=r2).
// Each root has its own root_idx and an independent leaf-index space
// starting at l1. Justin has 17 leaves (mirrors Doc124), Anna has 13
// (mirrors Doc125). Roots only interact via _idx[K] / _exists / _anchor
// aggregation at shared paths.
//
// Root 1 (Justin) — r1, 17 leaves:
//
//	root (self) → r1|l1
//	owner (self+desc) → {r1|l2, r1|l3}
//	├─ firstname="Justin" → {r1|l2, r1|l3}
//	├─ lastname="Time" → {r1|l2, r1|l3}
//	└─ nicknames[0]="watch" → r1|l3
//	addresses[0] (self+desc) → {r1|l4, r1|l5}
//	├─ city="Madrid" → {r1|l4, r1|l5}
//	├─ postcode="28001" → {r1|l4, r1|l5}
//	└─ numbers[0]=124 → r1|l5
//	addresses[1] (self only) → r1|l6
//	├─ city="London" → {r1|l6}
//	└─ postcode="SW1" → {r1|l6}
//	tags[0]="german" → r1|l7
//	tags[1]="japanese" → r1|l8
//	tags[2]="sedan" → r1|l9
//	cars[0] (self+desc) → {r1|l10..l14}
//	├─ make="Audi" → {r1|l10..l14}
//	├─ tires[0] (self+desc) → {r1|l11, r1|l12, r1|l13}
//	│  ├─ width=205 → {r1|l11, r1|l12, r1|l13}
//	│  ├─ radiuses[0]=17 → r1|l12
//	│  └─ radiuses[1]=18 → r1|l13
//	└─ tires[1] (self only) → r1|l14
//	    └─ width=225 → {r1|l14}
//	cars[1] (self+desc) → {r1|l15, r1|l16, r1|l17}
//	├─ make="Kia" → {r1|l15, r1|l16, r1|l17}
//	├─ tires[0] (self only, radiuses=[]) → r1|l16
//	│  └─ width=195 → {r1|l16}
//	└─ colors[0]="white" → r1|l17
//	name="subdoc_124" → {r1|l1..l17}
//
// Root 2 (Anna) — r2, 13 leaves:
//
//	root (self) → r2|l1
//	owner (self only) → r2|l2
//	├─ firstname="Anna" → {r2|l2}
//	└─ lastname="Wanna" → {r2|l2}
//	addresses[0] (self+desc) → {r2|l3, r2|l4}
//	├─ city="Paris" → {r2|l3, r2|l4}
//	├─ postcode="75001" → {r2|l3, r2|l4}
//	└─ numbers[0]=125 → r2|l4
//	tags[0]="electric" → r2|l5
//	cars[0] (self+desc) → {r2|l6..l13}
//	├─ make="Tesla" → {r2|l6..l13}
//	├─ tires[0] (self+desc) → {r2|l7..l10}
//	│  ├─ width=245 → {r2|l7..l10}
//	│  ├─ radiuses[0]=18 → r2|l8
//	│  ├─ radiuses[1]=19 → r2|l9
//	│  └─ radiuses[2]=20 → r2|l10
//	├─ accessories[0] (self only) → r2|l11
//	│  └─ type="charger" → {r2|l11}
//	├─ accessories[1] (self only) → r2|l12
//	│  └─ type="mats" → {r2|l12}
//	└─ colors[0]="yellow" → r2|l13
//	name="subdoc_125" → {r2|l1..l13}
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
		// Root 1: Justin (same data as doc124)
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
		// Root 2: Anna (same data as doc125)
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

	// Values: 35 (20 from r1/Justin + 15 from r2/Anna)
	require.Len(t, result.Values, 35)
	// r1 (Justin) — mirrors Doc124 with r=1
	assertValue(t, result, "owner.nicknames", "watch", positions(1, 3))
	assertValue(t, result, "owner.firstname", "Justin", positions(1, 2, 3))
	assertValue(t, result, "owner.lastname", "Time", positions(1, 2, 3))
	assertValue(t, result, "addresses.numbers", float64(124), positions(1, 5))
	assertValue(t, result, "addresses.city", "Madrid", positions(1, 4, 5))
	assertValue(t, result, "addresses.postcode", "28001", positions(1, 4, 5))
	assertValue(t, result, "addresses.city", "London", positions(1, 6))
	assertValue(t, result, "addresses.postcode", "SW1", positions(1, 6))
	assertValue(t, result, "tags", "german", positions(1, 7))
	assertValue(t, result, "tags", "japanese", positions(1, 8))
	assertValue(t, result, "tags", "sedan", positions(1, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(17), positions(1, 12))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 13))
	assertValue(t, result, "cars.tires.width", float64(205), positions(1, 11, 12, 13))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 14))
	assertValue(t, result, "cars.tires.width", float64(195), positions(1, 16))
	assertValue(t, result, "cars.make", "Audi", positions(1, 10, 11, 12, 13, 14))
	assertValue(t, result, "cars.make", "Kia", positions(1, 15, 16, 17))
	assertValue(t, result, "cars.colors", "white", positions(1, 17))
	assertValue(t, result, "name", "subdoc_124", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	// r2 (Anna) — mirrors Doc125 with r=2
	assertValue(t, result, "owner.firstname", "Anna", positions(2, 2))
	assertValue(t, result, "owner.lastname", "Wanna", positions(2, 2))
	assertValue(t, result, "addresses.numbers", float64(125), positions(2, 4))
	assertValue(t, result, "addresses.city", "Paris", positions(2, 3, 4))
	assertValue(t, result, "addresses.postcode", "75001", positions(2, 3, 4))
	assertValue(t, result, "tags", "electric", positions(2, 5))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(2, 8))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(2, 9))
	assertValue(t, result, "cars.tires.radiuses", float64(20), positions(2, 10))
	assertValue(t, result, "cars.tires.width", float64(245), positions(2, 7, 8, 9, 10))
	assertValue(t, result, "cars.accessories.type", "charger", positions(2, 11))
	assertValue(t, result, "cars.accessories.type", "mats", positions(2, 12))
	assertValue(t, result, "cars.colors", "yellow", positions(2, 13))
	assertValue(t, result, "cars.make", "Tesla", positions(2, 6, 7, 8, 9, 10, 11, 12, 13))
	assertValue(t, result, "name", "subdoc_125", positions(2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

	// Idx: 30 (17 from r1/Justin + 13 from r2/Anna; numbers add up after
	// aggregation by (path, index) — see per-root counts)
	require.Len(t, result.Idx, 30)
	assertIdx(t, result, "owner", 0, append(positions(1, 2, 3), positions(2, 2)...))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 3))
	assertIdx(t, result, "addresses", 0, append(positions(1, 4, 5), positions(2, 3, 4)...))
	assertIdx(t, result, "addresses", 1, positions(1, 6))
	assertIdx(t, result, "addresses.numbers", 0, append(positions(1, 5), positions(2, 4)...))
	assertIdx(t, result, "tags", 0, append(positions(1, 7), positions(2, 5)...))
	assertIdx(t, result, "tags", 1, positions(1, 8))
	assertIdx(t, result, "tags", 2, positions(1, 9))
	assertIdx(t, result, "cars", 0, append(positions(1, 10, 11, 12, 13, 14), positions(2, 6, 7, 8, 9, 10, 11, 12, 13)...))
	assertIdx(t, result, "cars", 1, positions(1, 15, 16, 17))
	assertIdx(t, result, "cars.tires", 0, append(positions(1, 11, 12, 13, 16), positions(2, 7, 8, 9, 10)...))
	assertIdx(t, result, "cars.tires", 1, positions(1, 14))
	assertIdx(t, result, "cars.tires.radiuses", 0, append(positions(1, 12), positions(2, 8)...))
	assertIdx(t, result, "cars.tires.radiuses", 1, append(positions(1, 13), positions(2, 9)...))
	assertIdx(t, result, "cars.tires.radiuses", 2, positions(2, 10))
	assertIdx(t, result, "cars.accessories", 0, positions(2, 11))
	assertIdx(t, result, "cars.accessories", 1, positions(2, 12))
	assertIdx(t, result, "cars.colors", 0, append(positions(1, 17), positions(2, 13)...))
	// root-level _idx entries for arr[N] positional filtering
	assertIdx(t, result, "", 0, positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17))
	assertIdx(t, result, "", 1, positions(2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

	// Exists: 41 raw entries (23 from r1 + 17 from r2 + 1 root)
	require.Len(t, result.Exists, 41)
	allPositions := append(
		positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
		positions(2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)...,
	)
	assertExists(t, result, "", allPositions)
	assertExists(t, result, "name", allPositions)
	assertExists(t, result, "owner", append(positions(1, 2, 3), positions(2, 2)...))
	assertExists(t, result, "owner.firstname", append(positions(1, 2, 3), positions(2, 2)...))
	assertExists(t, result, "owner.lastname", append(positions(1, 2, 3), positions(2, 2)...))
	assertExists(t, result, "owner.nicknames", positions(1, 3))
	assertExists(t, result, "addresses", append(positions(1, 4, 5, 6), positions(2, 3, 4)...))
	assertExists(t, result, "addresses.city", append(positions(1, 4, 5, 6), positions(2, 3, 4)...))
	assertExists(t, result, "addresses.postcode", append(positions(1, 4, 5, 6), positions(2, 3, 4)...))
	assertExists(t, result, "addresses.numbers", append(positions(1, 5), positions(2, 4)...))
	assertExists(t, result, "tags", append(positions(1, 7, 8, 9), positions(2, 5)...))
	assertExists(t, result, "cars", append(positions(1, 10, 11, 12, 13, 14, 15, 16, 17), positions(2, 6, 7, 8, 9, 10, 11, 12, 13)...))
	assertExists(t, result, "cars.make", append(positions(1, 10, 11, 12, 13, 14, 15, 16, 17), positions(2, 6, 7, 8, 9, 10, 11, 12, 13)...))
	assertExists(t, result, "cars.tires", append(positions(1, 11, 12, 13, 14, 16), positions(2, 7, 8, 9, 10)...))
	assertExists(t, result, "cars.tires.width", append(positions(1, 11, 12, 13, 14, 16), positions(2, 7, 8, 9, 10)...))
	assertExists(t, result, "cars.tires.radiuses", append(positions(1, 12, 13), positions(2, 8, 9, 10)...))
	assertExists(t, result, "cars.accessories", positions(2, 11, 12))
	assertExists(t, result, "cars.accessories.type", positions(2, 11, 12))
	assertExists(t, result, "cars.colors", append(positions(1, 17), positions(2, 13)...))

	// Anchors: 30 entries (17 from r1 + 13 from r2)
	require.Len(t, result.Anchors, 30)
	assertAnchor(t, result, "", append(positions(1, 1), positions(2, 1)...))
	assertAnchor(t, result, "owner", append(positions(1, 2), positions(2, 2)...))
	assertAnchor(t, result, "owner.nicknames", positions(1, 3))
	assertAnchor(t, result, "addresses", append(positions(1, 4, 6), positions(2, 3)...))
	assertAnchor(t, result, "addresses.numbers", append(positions(1, 5), positions(2, 4)...))
	assertAnchor(t, result, "tags", append(positions(1, 7, 8, 9), positions(2, 5)...))
	assertAnchor(t, result, "cars", append(positions(1, 10, 15), positions(2, 6)...))
	assertAnchor(t, result, "cars.tires", append(positions(1, 11, 14, 16), positions(2, 7)...))
	assertAnchor(t, result, "cars.tires.radiuses", append(positions(1, 12, 13), positions(2, 8, 9, 10)...))
	assertAnchor(t, result, "cars.accessories", positions(2, 11, 12))
	assertAnchor(t, result, "cars.colors", append(positions(1, 17), positions(2, 13)...))
}
