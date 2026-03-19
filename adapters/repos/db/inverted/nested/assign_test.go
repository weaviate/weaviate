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

func TestAssignPositions_NilValue(t *testing.T) {
	prop := topLevelObject("nested", textProp("name"))
	result, err := AssignPositions(prop, nil)
	require.NoError(t, err)
	assert.Empty(t, result.Values)
	assert.Empty(t, result.Idx)
	assert.Empty(t, result.Exists)
}

func TestAssignPositions_EmptyObjectArray(t *testing.T) {
	prop := topLevelObjectArray("nested", textProp("name"))
	result, err := AssignPositions(prop, []any{})
	require.NoError(t, err)
	assert.Empty(t, result.Values)
}

func TestAssignPositions_SimpleScalar(t *testing.T) {
	// Object with a single scalar property → leaf node
	prop := topLevelObject("nested", textProp("name"))
	value := map[string]any{"name": "hello"}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// name inherits root element positions. Root element has no descendants → leaf → l1
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
}

func TestAssignPositions_ScalarArray(t *testing.T) {
	// Object with scalar array: tags=["german", "premium"]
	prop := topLevelObject("nested", textArrayProp("tags"))
	value := map[string]any{
		"tags": []any{"german", "premium"},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// Each scalar array element gets its own leaf
	tags := findValues(result, "tags")
	require.Len(t, tags, 2)
	assert.Equal(t, "german", tags[0].Value)
	assert.Equal(t, positions(1, 1), tags[0].Positions)
	assert.Equal(t, "premium", tags[1].Value)
	assert.Equal(t, positions(1, 2), tags[1].Positions)

	// _idx for each element
	idx0 := findIdx(result, "tags", 0)
	require.NotNil(t, idx0)
	assert.Equal(t, positions(1, 1), idx0.Positions)

	idx1 := findIdx(result, "tags", 1)
	require.NotNil(t, idx1)
	assert.Equal(t, positions(1, 2), idx1.Positions)

	// _exists.tags
	tagsExists := findExists(result, "tags")
	require.NotNil(t, tagsExists)
	assert.Equal(t, positions(1, 1, 2), tagsExists.Positions)
}

// TestAssignPositions_OwnerDoc123 tests position assignment matching the
// design doc's doc123 owner section:
//
//	owner (intermediate) → {l1, l2}
//	├─ firstname="Marsha" → {l1, l2}
//	├─ lastname="Mallow" → {l1, l2}
//	├─ nicknames[0]="Marshmallow" → l1
//	└─ nicknames[1]="M&M" → l2
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

	// nicknames[0]="Marshmallow" → l1, nicknames[1]="M&M" → l2
	nicknames := findValues(result, "owner.nicknames")
	require.Len(t, nicknames, 2)
	assert.Equal(t, "Marshmallow", nicknames[0].Value)
	assert.Equal(t, positions(1, 1), nicknames[0].Positions)
	assert.Equal(t, "M&M", nicknames[1].Value)
	assert.Equal(t, positions(1, 2), nicknames[1].Positions)

	// owner is intermediate → {l1, l2}
	// firstname="Marsha" inherits owner's positions → {l1, l2}
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, "Marsha", firstnames[0].Value)
	assert.Equal(t, positions(1, 1, 2), firstnames[0].Positions)

	lastnames := findValues(result, "owner.lastname")
	require.Len(t, lastnames, 1)
	assert.Equal(t, "Mallow", lastnames[0].Value)
	assert.Equal(t, positions(1, 1, 2), lastnames[0].Positions)

	// _exists.owner = {l1, l2}
	ownerExists := findExists(result, "owner")
	require.NotNil(t, ownerExists)
	assert.Equal(t, positions(1, 1, 2), ownerExists.Positions)

	// _exists.owner.nicknames = {l1, l2}
	nickExists := findExists(result, "owner.nicknames")
	require.NotNil(t, nickExists)
	assert.Equal(t, positions(1, 1, 2), nickExists.Positions)

	// _idx.owner.nicknames
	assert.Equal(t, positions(1, 1), findIdx(result, "owner.nicknames", 0).Positions)
	assert.Equal(t, positions(1, 2), findIdx(result, "owner.nicknames", 1).Positions)
}

// TestAssignPositions_OwnerDoc125LeafNode tests the case where owner has
// no nicknames → leaf node instead of intermediate.
//
//	owner (leaf, no nicknames) → l1
//	├─ firstname="Anna" → {l1}
//	└─ lastname="Wanna" → {l1}
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
			// no nicknames → owner is leaf
		},
	}

	result, err := AssignPositions(prop, value)
	require.NoError(t, err)

	// owner has no descendants → leaf → l1
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, positions(1, 1), firstnames[0].Positions)

	lastnames := findValues(result, "owner.lastname")
	require.Len(t, lastnames, 1)
	assert.Equal(t, positions(1, 1), lastnames[0].Positions)

	// _exists.owner = {l1}
	ownerExists := findExists(result, "owner")
	require.NotNil(t, ownerExists)
	assert.Equal(t, positions(1, 1), ownerExists.Positions)

	// No _exists.owner.nicknames (nicknames not present in data)
	assert.Nil(t, findExists(result, "owner.nicknames"))
}

// TestAssignPositions_Doc124Addresses tests mixed intermediate and leaf
// array elements, matching doc124 from the design:
//
//	addresses[0] (intermediate) → {l2}
//	├─ city="Madrid" → {l2}
//	├─ postcode="28001" → {l2}
//	└─ numbers[0]=124 → l2
//	addresses[1] (leaf, no numbers) → l3
//	├─ city="London" → {l3}
//	└─ postcode="SW1" → {l3}
//
// Note: owner with nicknames=["watch"] takes l1, so addresses start at l2.
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

	// owner.nicknames[0]="watch" → l1
	nicknames := findValues(result, "owner.nicknames")
	require.Len(t, nicknames, 1)
	assert.Equal(t, positions(1, 1), nicknames[0].Positions)

	// addresses[0].numbers[0]=124 → l2
	numbers := findValues(result, "addresses.numbers")
	require.Len(t, numbers, 1)
	assert.Equal(t, float64(124), numbers[0].Value)
	assert.Equal(t, positions(1, 2), numbers[0].Positions)

	// addresses[0] is intermediate → {l2}
	// addresses[0].city="Madrid" inherits {l2}
	cities := findValues(result, "addresses.city")
	require.Len(t, cities, 2)
	assert.Equal(t, "Madrid", cities[0].Value)
	assert.Equal(t, positions(1, 2), cities[0].Positions)

	// addresses[1] is leaf (no numbers) → l3
	// addresses[1].city="London" inherits {l3}
	assert.Equal(t, "London", cities[1].Value)
	assert.Equal(t, positions(1, 3), cities[1].Positions)

	postcodes := findValues(result, "addresses.postcode")
	require.Len(t, postcodes, 2)
	assert.Equal(t, "28001", postcodes[0].Value)
	assert.Equal(t, positions(1, 2), postcodes[0].Positions)
	assert.Equal(t, "SW1", postcodes[1].Value)
	assert.Equal(t, positions(1, 3), postcodes[1].Positions)

	// _idx.addresses[0] = {l2}, _idx.addresses[1] = {l3}
	assert.Equal(t, positions(1, 2), findIdx(result, "addresses", 0).Positions)
	assert.Equal(t, positions(1, 3), findIdx(result, "addresses", 1).Positions)

	// _exists.addresses = {l2, l3}
	addrExists := findExists(result, "addresses")
	require.NotNil(t, addrExists)
	assert.Equal(t, positions(1, 2, 3), addrExists.Positions)

	// _exists.addresses.numbers = {l2} (only addresses[0] has numbers)
	numExists := findExists(result, "addresses.numbers")
	require.NotNil(t, numExists)
	assert.Equal(t, positions(1, 2), numExists.Positions)
}

// TestAssignPositions_EmptyScalarArray tests that an empty scalar array
// produces no leaf positions (element is treated as if missing).
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

	// Empty nicknames → no descendants → owner is leaf → l1
	firstnames := findValues(result, "owner.firstname")
	require.Len(t, firstnames, 1)
	assert.Equal(t, positions(1, 1), firstnames[0].Positions)

	// No nicknames values
	assert.Empty(t, findValues(result, "owner.nicknames"))
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

// TestAssignPositions_Doc123Full tests complete doc123 (Marsha) from the design:
// 10 leaves total. Runs with both full shared schema and minimal doc123-only
// schema to verify that extra properties in the schema don't affect position
// assignment.
//
//	owner (intermediate) → {l1, l2}
//	├─ firstname="Marsha" → {l1, l2}
//	├─ lastname="Mallow" → {l1, l2}
//	├─ nicknames[0]="Marshmallow" → l1
//	└─ nicknames[1]="M&M" → l2
//	addresses[0] (intermediate) → {l3, l4}
//	├─ city="Berlin" → {l3, l4}
//	├─ postcode="10115" → {l3, l4}
//	├─ numbers[0]=123 → l3
//	└─ numbers[1]=1123 → l4
//	tags[0]="german" → l5
//	tags[1]="premium" → l6
//	cars[0] (intermediate) → {l7..l10}
//	├─ make="BMW" → {l7..l10}
//	├─ tires[0] (intermediate) → {l7, l8}
//	│  ├─ width=225 → {l7, l8}
//	│  ├─ radiuses[0]=18 → l7
//	│  └─ radiuses[1]=19 → l8
//	├─ colors[0]="black" → l9
//	└─ colors[1]="orange" → l10
//	name="subdoc_123" → {l1..l10}
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
	assertValue(t, result, "owner.nicknames", "Marshmallow", positions(1, 1))
	assertValue(t, result, "owner.nicknames", "M&M", positions(1, 2))
	assertValue(t, result, "owner.firstname", "Marsha", positions(1, 1, 2))
	assertValue(t, result, "owner.lastname", "Mallow", positions(1, 1, 2))
	assertValue(t, result, "addresses.numbers", float64(123), positions(1, 3))
	assertValue(t, result, "addresses.numbers", float64(1123), positions(1, 4))
	assertValue(t, result, "addresses.city", "Berlin", positions(1, 3, 4))
	assertValue(t, result, "addresses.postcode", "10115", positions(1, 3, 4))
	assertValue(t, result, "tags", "german", positions(1, 5))
	assertValue(t, result, "tags", "premium", positions(1, 6))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 7))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(1, 8))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 7, 8))
	assertValue(t, result, "cars.make", "BMW", positions(1, 7, 8, 9, 10))
	assertValue(t, result, "cars.colors", "black", positions(1, 9))
	assertValue(t, result, "cars.colors", "orange", positions(1, 10))
	assertValue(t, result, "name", "subdoc_123", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

	// Idx: 14 entries
	require.Len(t, result.Idx, 14)
	assertIdx(t, result, "owner", 0, positions(1, 1, 2))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 1))
	assertIdx(t, result, "owner.nicknames", 1, positions(1, 2))
	assertIdx(t, result, "addresses", 0, positions(1, 3, 4))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 3))
	assertIdx(t, result, "addresses.numbers", 1, positions(1, 4))
	assertIdx(t, result, "tags", 0, positions(1, 5))
	assertIdx(t, result, "tags", 1, positions(1, 6))
	assertIdx(t, result, "cars", 0, positions(1, 7, 8, 9, 10))
	assertIdx(t, result, "cars.tires", 0, positions(1, 7, 8))
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 7))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 8))
	assertIdx(t, result, "cars.colors", 0, positions(1, 9))
	assertIdx(t, result, "cars.colors", 1, positions(1, 10))

	// Exists: 17 entries
	require.Len(t, result.Exists, 17)
	assertExists(t, result, "", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
	assertExists(t, result, "name", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
	assertExists(t, result, "owner", positions(1, 1, 2))
	assertExists(t, result, "owner.firstname", positions(1, 1, 2))
	assertExists(t, result, "owner.lastname", positions(1, 1, 2))
	assertExists(t, result, "owner.nicknames", positions(1, 1, 2))
	assertExists(t, result, "addresses", positions(1, 3, 4))
	assertExists(t, result, "addresses.city", positions(1, 3, 4))
	assertExists(t, result, "addresses.postcode", positions(1, 3, 4))
	assertExists(t, result, "addresses.numbers", positions(1, 3, 4))
	assertExists(t, result, "tags", positions(1, 5, 6))
	assertExists(t, result, "cars", positions(1, 7, 8, 9, 10))
	assertExists(t, result, "cars.make", positions(1, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires", positions(1, 7, 8))
	assertExists(t, result, "cars.tires.width", positions(1, 7, 8))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 7, 8))
	assertExists(t, result, "cars.colors", positions(1, 9, 10))
}

// TestAssignPositions_Doc124Full tests complete doc124 (Justin) from the design:
// 11 leaves total. Key cases: tires[1] leaf (no radiuses), Kia tires[0] leaf
// (radiuses=[]), two cars with different structures.
//
//	owner (intermediate) → {l1}
//	├─ firstname="Justin" → {l1}
//	├─ lastname="Time" → {l1}
//	└─ nicknames[0]="watch" → l1
//	addresses[0] (intermediate) → {l2}
//	├─ city="Madrid" → {l2}
//	├─ postcode="28001" → {l2}
//	└─ numbers[0]=124 → l2
//	addresses[1] (leaf, no numbers) → l3
//	├─ city="London" → {l3}
//	└─ postcode="SW1" → {l3}
//	tags[0]="german" → l4
//	tags[1]="japanese" → l5
//	tags[2]="sedan" → l6
//	cars[0] (intermediate) → {l7..l9}
//	├─ make="Audi" → {l7..l9}
//	├─ tires[0] (intermediate) → {l7, l8}
//	│  ├─ width=205 → {l7, l8}
//	│  ├─ radiuses[0]=17 → l7
//	│  └─ radiuses[1]=18 → l8
//	└─ tires[1] (leaf, no radiuses) → l9
//	    └─ width=225 → {l9}
//	cars[1] (intermediate) → {l10, l11}
//	├─ make="Kia" → {l10, l11}
//	├─ tires[0] (leaf, radiuses=[]) → l10
//	│  └─ width=195 → {l10}
//	└─ colors[0]="white" → l11
//	name="subdoc_124" → {l1..l11}
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
	assertValue(t, result, "owner.nicknames", "watch", positions(1, 1))
	assertValue(t, result, "owner.firstname", "Justin", positions(1, 1))
	assertValue(t, result, "owner.lastname", "Time", positions(1, 1))
	assertValue(t, result, "addresses.numbers", float64(124), positions(1, 2))
	assertValue(t, result, "addresses.city", "Madrid", positions(1, 2))
	assertValue(t, result, "addresses.postcode", "28001", positions(1, 2))
	assertValue(t, result, "addresses.city", "London", positions(1, 3))
	assertValue(t, result, "addresses.postcode", "SW1", positions(1, 3))
	assertValue(t, result, "tags", "german", positions(1, 4))
	assertValue(t, result, "tags", "japanese", positions(1, 5))
	assertValue(t, result, "tags", "sedan", positions(1, 6))
	assertValue(t, result, "cars.tires.radiuses", float64(17), positions(1, 7))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 8))
	assertValue(t, result, "cars.tires.width", float64(205), positions(1, 7, 8))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 9))
	assertValue(t, result, "cars.tires.width", float64(195), positions(1, 10))
	assertValue(t, result, "cars.make", "Audi", positions(1, 7, 8, 9))
	assertValue(t, result, "cars.make", "Kia", positions(1, 10, 11))
	assertValue(t, result, "cars.colors", "white", positions(1, 11))
	assertValue(t, result, "name", "subdoc_124", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))

	// Idx: 16 entries (aggregated by path+index)
	require.Len(t, result.Idx, 16)
	assertIdx(t, result, "owner", 0, positions(1, 1))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 1))
	assertIdx(t, result, "addresses", 0, positions(1, 2))
	assertIdx(t, result, "addresses", 1, positions(1, 3))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 2))
	assertIdx(t, result, "tags", 0, positions(1, 4))
	assertIdx(t, result, "tags", 1, positions(1, 5))
	assertIdx(t, result, "tags", 2, positions(1, 6))
	assertIdx(t, result, "cars", 0, positions(1, 7, 8, 9))
	assertIdx(t, result, "cars", 1, positions(1, 10, 11))
	assertIdx(t, result, "cars.tires", 0, positions(1, 7, 8, 10))  // Audi t0 {l7,l8} + Kia t0 {l10}
	assertIdx(t, result, "cars.tires", 1, positions(1, 9))          // Audi t1 only
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 7))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 8))
	assertIdx(t, result, "cars.colors", 0, positions(1, 11))

	// Exists: 23 raw entries, 17 unique paths (aggregated)
	require.Len(t, result.Exists, 23)
	assertExists(t, result, "", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
	assertExists(t, result, "name", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
	assertExists(t, result, "owner", positions(1, 1))
	assertExists(t, result, "owner.firstname", positions(1, 1))
	assertExists(t, result, "owner.lastname", positions(1, 1))
	assertExists(t, result, "owner.nicknames", positions(1, 1))
	assertExists(t, result, "addresses", positions(1, 2, 3))
	assertExists(t, result, "addresses.city", positions(1, 2, 3))
	assertExists(t, result, "addresses.postcode", positions(1, 2, 3))
	assertExists(t, result, "addresses.numbers", positions(1, 2))
	assertExists(t, result, "tags", positions(1, 4, 5, 6))
	assertExists(t, result, "cars", positions(1, 7, 8, 9, 10, 11))
	assertExists(t, result, "cars.make", positions(1, 7, 8, 9, 10, 11))
	assertExists(t, result, "cars.tires", positions(1, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires.width", positions(1, 7, 8, 9, 10))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 7, 8))
	assertExists(t, result, "cars.colors", positions(1, 11))
}

// TestAssignPositions_Doc125Full tests complete doc125 (Anna) from the design:
// 9 leaves. Key cases: owner is leaf (no nicknames), accessories present.
//
//	owner (leaf, no nicknames) → l1
//	├─ firstname="Anna" → {l1}
//	└─ lastname="Wanna" → {l1}
//	addresses[0] (intermediate) → {l2}
//	├─ city="Paris" → {l2}
//	├─ postcode="75001" → {l2}
//	└─ numbers[0]=125 → l2
//	tags[0]="electric" → l3
//	cars[0] (intermediate) → {l4..l9}
//	├─ make="Tesla" → {l4..l9}
//	├─ tires[0] (intermediate) → {l4..l6}
//	│  ├─ width=245 → {l4..l6}
//	│  ├─ radiuses[0]=18 → l4
//	│  ├─ radiuses[1]=19 → l5
//	│  └─ radiuses[2]=20 → l6
//	├─ accessories[0] (leaf) → l7
//	│  └─ type="charger" → {l7}
//	├─ accessories[1] (leaf) → l8
//	│  └─ type="mats" → {l8}
//	└─ colors[0]="yellow" → l9
//	name="subdoc_125" → {l1..l9}
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
	assertValue(t, result, "owner.firstname", "Anna", positions(1, 1))
	assertValue(t, result, "owner.lastname", "Wanna", positions(1, 1))
	assertValue(t, result, "addresses.numbers", float64(125), positions(1, 2))
	assertValue(t, result, "addresses.city", "Paris", positions(1, 2))
	assertValue(t, result, "addresses.postcode", "75001", positions(1, 2))
	assertValue(t, result, "tags", "electric", positions(1, 3))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 4))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(1, 5))
	assertValue(t, result, "cars.tires.radiuses", float64(20), positions(1, 6))
	assertValue(t, result, "cars.tires.width", float64(245), positions(1, 4, 5, 6))
	assertValue(t, result, "cars.accessories.type", "charger", positions(1, 7))
	assertValue(t, result, "cars.accessories.type", "mats", positions(1, 8))
	assertValue(t, result, "cars.colors", "yellow", positions(1, 9))
	assertValue(t, result, "cars.make", "Tesla", positions(1, 4, 5, 6, 7, 8, 9))
	assertValue(t, result, "name", "subdoc_125", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9))

	// Idx: 12 entries (no owner.nicknames — Anna has no nicknames)
	require.Len(t, result.Idx, 12)
	assertIdx(t, result, "owner", 0, positions(1, 1))
	assertIdx(t, result, "addresses", 0, positions(1, 2))
	assertIdx(t, result, "addresses.numbers", 0, positions(1, 2))
	assertIdx(t, result, "tags", 0, positions(1, 3))
	assertIdx(t, result, "cars", 0, positions(1, 4, 5, 6, 7, 8, 9))
	assertIdx(t, result, "cars.tires", 0, positions(1, 4, 5, 6))
	assertIdx(t, result, "cars.tires.radiuses", 0, positions(1, 4))
	assertIdx(t, result, "cars.tires.radiuses", 1, positions(1, 5))
	assertIdx(t, result, "cars.tires.radiuses", 2, positions(1, 6))
	assertIdx(t, result, "cars.accessories", 0, positions(1, 7))
	assertIdx(t, result, "cars.accessories", 1, positions(1, 8))
	assertIdx(t, result, "cars.colors", 0, positions(1, 9))

	// Exists: 19 raw entries, 17 unique paths (no owner.nicknames)
	// accessories.type appears twice (one per leaf element)
	require.Len(t, result.Exists, 19)
	assertExists(t, result, "", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9))
	assertExists(t, result, "name", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9))
	assertExists(t, result, "owner", positions(1, 1))
	assertExists(t, result, "owner.firstname", positions(1, 1))
	assertExists(t, result, "owner.lastname", positions(1, 1))
	assertExists(t, result, "addresses", positions(1, 2))
	assertExists(t, result, "addresses.city", positions(1, 2))
	assertExists(t, result, "addresses.postcode", positions(1, 2))
	assertExists(t, result, "addresses.numbers", positions(1, 2))
	assertExists(t, result, "tags", positions(1, 3))
	assertExists(t, result, "cars", positions(1, 4, 5, 6, 7, 8, 9))
	assertExists(t, result, "cars.make", positions(1, 4, 5, 6, 7, 8, 9))
	assertExists(t, result, "cars.tires", positions(1, 4, 5, 6))
	assertExists(t, result, "cars.tires.width", positions(1, 4, 5, 6))
	assertExists(t, result, "cars.tires.radiuses", positions(1, 4, 5, 6))
	assertExists(t, result, "cars.accessories", positions(1, 7, 8))
	assertExists(t, result, "cars.accessories.type", positions(1, 7, 8))
	assertExists(t, result, "cars.colors", positions(1, 9))
}

// TestAssignPositions_Doc999ObjectArray tests the object[] case from the design:
// doc999 has nestedArray with two root elements (Justin=r1, Anna=r2).
// Justin has 11 leaves (same structure as doc124), Anna has 9 (same as doc125).
//
// Root 1 (Justin) — r1, 11 leaves:
//
//	owner (intermediate) → {r1|l1}
//	├─ firstname="Justin" → {r1|l1}
//	├─ lastname="Time" → {r1|l1}
//	└─ nicknames[0]="watch" → r1|l1
//	addresses[0] (intermediate) → {r1|l2}
//	├─ city="Madrid" → {r1|l2}
//	├─ postcode="28001" → {r1|l2}
//	└─ numbers[0]=124 → r1|l2
//	addresses[1] (leaf, no numbers) → r1|l3
//	├─ city="London" → {r1|l3}
//	└─ postcode="SW1" → {r1|l3}
//	tags[0]="german" → r1|l4
//	tags[1]="japanese" → r1|l5
//	tags[2]="sedan" → r1|l6
//	cars[0] (intermediate) → {r1|l7..l9}
//	├─ make="Audi" → {r1|l7..l9}
//	├─ tires[0] (intermediate) → {r1|l7, r1|l8}
//	│  ├─ width=205 → {r1|l7, r1|l8}
//	│  ├─ radiuses[0]=17 → r1|l7
//	│  └─ radiuses[1]=18 → r1|l8
//	└─ tires[1] (leaf, no radiuses) → r1|l9
//	    └─ width=225 → {r1|l9}
//	cars[1] (intermediate) → {r1|l10, r1|l11}
//	├─ make="Kia" → {r1|l10, r1|l11}
//	├─ tires[0] (leaf, radiuses=[]) → r1|l10
//	│  └─ width=195 → {r1|l10}
//	└─ colors[0]="white" → r1|l11
//	name="subdoc_124" → {r1|l1..l11}
//
// Root 2 (Anna) — r2, 9 leaves:
//
//	owner (leaf, no nicknames) → r2|l1
//	├─ firstname="Anna" → {r2|l1}
//	└─ lastname="Wanna" → {r2|l1}
//	addresses[0] (intermediate) → {r2|l2}
//	├─ city="Paris" → {r2|l2}
//	├─ postcode="75001" → {r2|l2}
//	└─ numbers[0]=125 → r2|l2
//	tags[0]="electric" → r2|l3
//	cars[0] (intermediate) → {r2|l4..l9}
//	├─ make="Tesla" → {r2|l4..l9}
//	├─ tires[0] (intermediate) → {r2|l4..l6}
//	│  ├─ width=245 → {r2|l4..l6}
//	│  ├─ radiuses[0]=18 → r2|l4
//	│  ├─ radiuses[1]=19 → r2|l5
//	│  └─ radiuses[2]=20 → r2|l6
//	├─ accessories[0] (leaf) → r2|l7
//	│  └─ type="charger" → {r2|l7}
//	├─ accessories[1] (leaf) → r2|l8
//	│  └─ type="mats" → {r2|l8}
//	└─ colors[0]="yellow" → r2|l9
//	name="subdoc_125" → {r2|l1..l9}
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
	// r1 (Justin)
	assertValue(t, result, "owner.nicknames", "watch", positions(1, 1))
	assertValue(t, result, "owner.firstname", "Justin", positions(1, 1))
	assertValue(t, result, "owner.lastname", "Time", positions(1, 1))
	assertValue(t, result, "addresses.numbers", float64(124), positions(1, 2))
	assertValue(t, result, "addresses.city", "Madrid", positions(1, 2))
	assertValue(t, result, "addresses.postcode", "28001", positions(1, 2))
	assertValue(t, result, "addresses.city", "London", positions(1, 3))
	assertValue(t, result, "addresses.postcode", "SW1", positions(1, 3))
	assertValue(t, result, "tags", "german", positions(1, 4))
	assertValue(t, result, "tags", "japanese", positions(1, 5))
	assertValue(t, result, "tags", "sedan", positions(1, 6))
	assertValue(t, result, "cars.tires.radiuses", float64(17), positions(1, 7))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(1, 8))
	assertValue(t, result, "cars.tires.width", float64(205), positions(1, 7, 8))
	assertValue(t, result, "cars.tires.width", float64(225), positions(1, 9))
	assertValue(t, result, "cars.tires.width", float64(195), positions(1, 10))
	assertValue(t, result, "cars.make", "Audi", positions(1, 7, 8, 9))
	assertValue(t, result, "cars.make", "Kia", positions(1, 10, 11))
	assertValue(t, result, "cars.colors", "white", positions(1, 11))
	assertValue(t, result, "name", "subdoc_124", positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
	// r2 (Anna)
	assertValue(t, result, "owner.firstname", "Anna", positions(2, 1))
	assertValue(t, result, "owner.lastname", "Wanna", positions(2, 1))
	assertValue(t, result, "addresses.numbers", float64(125), positions(2, 2))
	assertValue(t, result, "addresses.city", "Paris", positions(2, 2))
	assertValue(t, result, "addresses.postcode", "75001", positions(2, 2))
	assertValue(t, result, "tags", "electric", positions(2, 3))
	assertValue(t, result, "cars.tires.radiuses", float64(18), positions(2, 4))
	assertValue(t, result, "cars.tires.radiuses", float64(19), positions(2, 5))
	assertValue(t, result, "cars.tires.radiuses", float64(20), positions(2, 6))
	assertValue(t, result, "cars.tires.width", float64(245), positions(2, 4, 5, 6))
	assertValue(t, result, "cars.accessories.type", "charger", positions(2, 7))
	assertValue(t, result, "cars.accessories.type", "mats", positions(2, 8))
	assertValue(t, result, "cars.colors", "yellow", positions(2, 9))
	assertValue(t, result, "cars.make", "Tesla", positions(2, 4, 5, 6, 7, 8, 9))
	assertValue(t, result, "name", "subdoc_125", positions(2, 1, 2, 3, 4, 5, 6, 7, 8, 9))

	// Idx: 28 (16 from r1/Justin + 12 from r2/Anna)
	require.Len(t, result.Idx, 28)
	assertIdx(t, result, "owner", 0, append(positions(1, 1), positions(2, 1)...))
	assertIdx(t, result, "owner.nicknames", 0, positions(1, 1))
	assertIdx(t, result, "addresses", 0, append(positions(1, 2), positions(2, 2)...))
	assertIdx(t, result, "addresses", 1, positions(1, 3))
	assertIdx(t, result, "addresses.numbers", 0, append(positions(1, 2), positions(2, 2)...))
	assertIdx(t, result, "tags", 0, append(positions(1, 4), positions(2, 3)...))
	assertIdx(t, result, "tags", 1, positions(1, 5))
	assertIdx(t, result, "tags", 2, positions(1, 6))
	assertIdx(t, result, "cars", 0, append(positions(1, 7, 8, 9), positions(2, 4, 5, 6, 7, 8, 9)...))
	assertIdx(t, result, "cars", 1, positions(1, 10, 11))
	assertIdx(t, result, "cars.tires", 0, append(positions(1, 7, 8, 10), positions(2, 4, 5, 6)...))
	assertIdx(t, result, "cars.tires", 1, positions(1, 9))
	assertIdx(t, result, "cars.tires.radiuses", 0, append(positions(1, 7), positions(2, 4)...))
	assertIdx(t, result, "cars.tires.radiuses", 1, append(positions(1, 8), positions(2, 5)...))
	assertIdx(t, result, "cars.tires.radiuses", 2, positions(2, 6))
	assertIdx(t, result, "cars.accessories", 0, positions(2, 7))
	assertIdx(t, result, "cars.accessories", 1, positions(2, 8))
	assertIdx(t, result, "cars.colors", 0, append(positions(1, 11), positions(2, 9)...))

	// Exists: 40 raw entries (23 from r1 + 17 from r2)
	require.Len(t, result.Exists, 41) // 23 + 17 + 1 root
	allPositions := append(
		positions(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		positions(2, 1, 2, 3, 4, 5, 6, 7, 8, 9)...,
	)
	assertExists(t, result, "", allPositions)
	assertExists(t, result, "name", allPositions)
	assertExists(t, result, "owner", append(positions(1, 1), positions(2, 1)...))
	assertExists(t, result, "owner.firstname", append(positions(1, 1), positions(2, 1)...))
	assertExists(t, result, "owner.lastname", append(positions(1, 1), positions(2, 1)...))
	assertExists(t, result, "owner.nicknames", positions(1, 1))
	assertExists(t, result, "addresses", append(positions(1, 2, 3), positions(2, 2)...))
	assertExists(t, result, "addresses.city", append(positions(1, 2, 3), positions(2, 2)...))
	assertExists(t, result, "addresses.postcode", append(positions(1, 2, 3), positions(2, 2)...))
	assertExists(t, result, "addresses.numbers", append(positions(1, 2), positions(2, 2)...))
	assertExists(t, result, "tags", append(positions(1, 4, 5, 6), positions(2, 3)...))
	assertExists(t, result, "cars", append(positions(1, 7, 8, 9, 10, 11), positions(2, 4, 5, 6, 7, 8, 9)...))
	assertExists(t, result, "cars.make", append(positions(1, 7, 8, 9, 10, 11), positions(2, 4, 5, 6, 7, 8, 9)...))
	assertExists(t, result, "cars.tires", append(positions(1, 7, 8, 9, 10), positions(2, 4, 5, 6)...))
	assertExists(t, result, "cars.tires.width", append(positions(1, 7, 8, 9, 10), positions(2, 4, 5, 6)...))
	assertExists(t, result, "cars.tires.radiuses", append(positions(1, 7, 8), positions(2, 4, 5, 6)...))
	assertExists(t, result, "cars.accessories", positions(2, 7, 8))
	assertExists(t, result, "cars.accessories.type", positions(2, 7, 8))
	assertExists(t, result, "cars.colors", append(positions(1, 11), positions(2, 9)...))
}

// findAllIdx returns all IdxEntry instances for a given path and index
// (there can be multiple when the same relative path appears under different
// parent array elements, e.g. cars[0].tires and cars[1].tires both emit
// _idx entries for "cars.tires" with index 0).
// findAllIdx returns all IdxEntry instances for a given path and index
// (there can be multiple when the same relative path appears under different
// parent array elements, e.g. cars[0].tires and cars[1].tires both emit
// _idx entries for "cars.tires" with index 0).
func findAllIdx(result *AssignResult, path string, index int) []IdxEntry {
	var out []IdxEntry
	for _, idx := range result.Idx {
		if idx.Path == path && idx.Index == index {
			out = append(out, idx)
		}
	}
	return out
}
