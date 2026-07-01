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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nested2 "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested2"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// ownerProp builds the shared "nested" object property used across several
// tests: a root object containing an owner sub-object with firstname/lastname
// (text, filterable) and nicknames (text[], filterable). This mirrors the
// Doc123 fixture from nested2/assign_test.go.
func ownerProp() *models.Property {
	return &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{
				Name:     "owner",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
				},
			},
		},
	}
}

func ownerValue() map[string]any {
	return map[string]any{
		"owner": map[string]any{
			"firstname": "Marsha",
			"lastname":  "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
	}
}

// collectValues2 drains the Values iterator into a slice of ValueViews.
func collectValues2(np *NestedProperty2) []ValueView {
	var out []ValueView
	for v := range np.Values() {
		out = append(out, v)
	}
	return out
}

// collectIdxEntries2 drains the Idx iterator into a slice of IdxViews.
func collectIdxEntries2(np *NestedProperty2) []IdxView {
	var out []IdxView
	for idx := range np.Idx() {
		out = append(out, idx)
	}
	return out
}

// collectExistsPaths drains the Exists iterator into a slice of paths.
func collectExistsPaths(np *NestedProperty2) []string {
	var paths []string
	for e := range np.Exists() {
		paths = append(paths, e.Path)
	}
	return paths
}

// collectExists2 drains the Exists iterator into a slice of ExistsViews.
func collectExists2(np *NestedProperty2) []ExistsView {
	var out []ExistsView
	for e := range np.Exists() {
		out = append(out, e)
	}
	return out
}

// collectAnchorPaths drains the Anchors iterator into a slice of paths.
func collectAnchorPaths(np *NestedProperty2) []string {
	var paths []string
	for a := range np.Anchors() {
		paths = append(paths, a.Path)
	}
	return paths
}

// collectAnchors2 drains the Anchors iterator into a slice of AnchorViews.
func collectAnchors2(np *NestedProperty2) []AnchorView {
	var out []AnchorView
	for a := range np.Anchors() {
		out = append(out, a)
	}
	return out
}

func TestAnalyzeNestedProp2_NilValueReturnsNil(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := ownerProp()
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	result, err := analyzer.analyzeNestedProp2(ls, prop, nil)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestAnalyzeNestedProp2_EmptyObjectArrayReturnsNil(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObjectArray.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
		},
	}
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	result, err := analyzer.analyzeNestedProp2(ls, prop, []any{})
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestAnalyzeNestedProp2_AllIndexesDisabledReturnsNil(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")

	t.Run("object", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:              "city",
					DataType:          schema.DataTypeText.PropString(),
					IndexFilterable:   boolPtr(false),
					IndexSearchable:   boolPtr(false),
					IndexRangeFilters: boolPtr(false),
				},
			},
		}
		ls, err := nested2.BuildSchema(prop)
		require.NoError(t, err)

		result, err := analyzer.analyzeNestedProp2(ls, prop, map[string]any{"city": "Berlin"})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("object array", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:              "city",
					DataType:          schema.DataTypeText.PropString(),
					IndexFilterable:   boolPtr(false),
					IndexSearchable:   boolPtr(false),
					IndexRangeFilters: boolPtr(false),
				},
			},
		}
		ls, err := nested2.BuildSchema(prop)
		require.NoError(t, err)

		result, err := analyzer.analyzeNestedProp2(ls, prop, []any{
			map[string]any{"city": "Berlin"},
			map[string]any{"city": "Hamburg"},
		})
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

// TestAnalyzeNestedProp2_SimpleOwner exercises the full pipeline on the Doc123
// owner fixture. It verifies value count, iterator output, and predicates.
func TestAnalyzeNestedProp2_SimpleOwner(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := ownerProp()
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, ownerValue())
	require.NoError(t, err)
	require.NotNil(t, np)

	assert.Equal(t, "nested", np.Name)

	// Values: firstname(1) + lastname(1) + nicknames[0](1) + nicknames[1](1) = 4
	// "Marsha", "Mallow", "Marshmallow", "M&M" each produce 1 token with word
	// tokenization.
	vals := collectValues2(np)
	assert.Len(t, vals, 4)
	for _, v := range vals {
		assert.NotEmpty(t, v.Data, "value at %s should have analyzed bytes", v.Path)
		assert.True(t, v.HasFilterableIndex, "text leaves are filterable by default")
		assert.True(t, v.HasSearchableIndex, "text leaves are searchable by default")
		assert.False(t, v.HasRangeableIndex)
	}

	// Idx: nicknames[0](1) + nicknames[1](1) = scalar array elements.
	// owner[0](1) = walkNestedArray wraps DataTypeObject in a 1-element slice → Idx[0].
	// root ""[0](1) = root loop always appends one Idx per top-level element.
	// Total = 4.
	assert.Len(t, collectIdxEntries2(np), 4)

	// Exists: root(""), owner, owner.nicknames, owner.firstname, owner.lastname.
	// All leaves have hasAny()=true so the iterator yields every entry (gated == ungated).
	assert.Len(t, collectExists2(np), 5)
	existsPaths := collectExistsPaths(np)
	assert.Len(t, existsPaths, 5)
	assert.Contains(t, existsPaths, "")
	assert.Contains(t, existsPaths, "owner")
	assert.Contains(t, existsPaths, "owner.firstname")
	assert.Contains(t, existsPaths, "owner.lastname")
	assert.Contains(t, existsPaths, "owner.nicknames")

	// Anchors: root(""), owner, owner.nicknames (2 entries for nicknames[0/1]).
	// Total = 4.
	assert.Len(t, collectAnchors2(np), 4)
	anchorPaths := collectAnchorPaths(np)
	assert.Len(t, anchorPaths, 4)
	assert.Contains(t, anchorPaths, "")
	assert.Contains(t, anchorPaths, "owner")
	assert.Contains(t, anchorPaths, "owner.nicknames")

	// Predicates.
	assert.True(t, np.HasFilterableEntries())
	assert.True(t, np.HasMetaEntries())
	assert.False(t, np.HasSearchableEntries())
	assert.False(t, np.HasRangeableEntries())
	assert.True(t, np.HasFilterableIndex)
	assert.True(t, np.HasSearchableIndex)
	assert.False(t, np.HasRangeableIndex)
}

// TestAnalyzeNestedProp2_Gating verifies that a leaf with all indexes disabled
// is absent from Values and from the Exists/Anchors iterator output, but its
// Idx entries (it is a scalar array) are still present in Result.Idx (ungated).
func TestAnalyzeNestedProp2_Gating(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "indexed", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			// skipped_arr has all indexes off — its Value entries are dropped but
			// its Idx entries remain because Idx is always ungated.
			{
				Name:            "skipped_arr",
				DataType:        schema.DataTypeTextArray.PropString(),
				Tokenization:    "word",
				IndexFilterable: boolPtr(false),
				IndexSearchable: boolPtr(false),
			},
		},
	}
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	value := map[string]any{
		"indexed":     "hello",
		"skipped_arr": []any{"world"},
	}

	np, err := analyzer.analyzeNestedProp2(ls, prop, value)
	require.NoError(t, err)
	require.NotNil(t, np)

	// Only "indexed" produces a Value entry.
	gVals := collectValues2(np)
	require.Len(t, gVals, 1)
	assert.Equal(t, "indexed", gVals[0].Path)

	// Idx is ungated: skipped_arr[0] must appear alongside root "".
	idxEntries := collectIdxEntries2(np)
	idxPaths := make([]string, len(idxEntries))
	for i, idx := range idxEntries {
		idxPaths[i] = idx.Path
	}
	assert.Contains(t, idxPaths, "skipped_arr", "ungated Idx must include skipped leaf")
	assert.Contains(t, idxPaths, "", "root Idx entry must be present")

	// Exists iterator gates out skipped_arr but keeps root("") and indexed.
	existsPaths := collectExistsPaths(np)
	assert.NotContains(t, existsPaths, "skipped_arr", "disabled leaf must not appear in Exists iterator")
	assert.Contains(t, existsPaths, "", "root sentinel always kept")
	assert.Contains(t, existsPaths, "indexed", "enabled leaf always kept")

	// Anchors iterator gates out skipped_arr but keeps root("").
	anchorPaths := collectAnchorPaths(np)
	assert.NotContains(t, anchorPaths, "skipped_arr", "disabled leaf must not appear in Anchors iterator")
	assert.Contains(t, anchorPaths, "", "root anchor always kept")
}

// TestAnalyzeNestedProp2_HasFilterableEntriesFalse verifies that
// HasFilterableEntries returns false when all leaves are filterable=false even
// though meta entries (Idx/Exists/Anchors) are still present.
func TestAnalyzeNestedProp2_HasFilterableEntriesFalse(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{
				Name:            "city",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    "word",
				IndexFilterable: boolPtr(false),
				// searchable is on, so hasAny() is true → Values entry produced,
				// but HasFilterableIndex is false → numFilterable stays zero, so HasFilterableEntries() returns false.
			},
		},
	}
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, map[string]any{"city": "Berlin"})
	require.NoError(t, err)
	require.NotNil(t, np)

	// Values entry exists (searchable is true) but HasFilterableIndex=false.
	hfVals := collectValues2(np)
	require.Len(t, hfVals, 1)
	assert.False(t, hfVals[0].HasFilterableIndex)

	assert.False(t, np.HasFilterableEntries(), "no filterable values → HasFilterableEntries must be false")
	assert.True(t, np.HasMetaEntries(), "meta entries always present for non-empty doc")
	assert.False(t, np.HasSearchableEntries())
	assert.False(t, np.HasRangeableEntries())
}

// TestAnalyzeNestedProp2_IteratorEarlyStop verifies that the Exists/Anchors
// iterators honour a false return from the yield function (early termination).
func TestAnalyzeNestedProp2_IteratorEarlyStop(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "a", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "b", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "c", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
		},
	}
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, map[string]any{"a": "x", "b": "y", "c": "z"})
	require.NoError(t, err)
	require.NotNil(t, np)

	// Stop after the first yielded Exists entry.
	var existsCount int
	for range np.Exists() {
		existsCount++
		break
	}
	assert.Equal(t, 1, existsCount, "early-stop should see exactly 1 entry")

	// Same for Anchors.
	var anchorCount int
	for range np.Anchors() {
		anchorCount++
		break
	}
	assert.Equal(t, 1, anchorCount, "early-stop should see exactly 1 entry")
}

// TestAnalyzeNestedValue2_MultiTokenText verifies that a text value which
// tokenizes into N terms produces N NestedValue2 entries all carrying the same
// Pos — a 2×uint32 copy per entry with no new arena allocation.
func TestAnalyzeNestedValue2_MultiTokenText(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
		},
	}
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	// "hello world" with word tokenization produces two tokens.
	np, err := analyzer.analyzeNestedProp2(ls, prop, map[string]any{"title": "hello world"})
	require.NoError(t, err)
	require.NotNil(t, np)

	mttVals := collectValues2(np)
	require.Len(t, mttVals, 2, "two-word value should produce 2 analyzed entries")

	// Both entries must carry the identical PosRange — same arena slice, same extent.
	// We verify equivalence by checking that both resolve to the same positions.
	pos0 := mttVals[0].Positions
	pos1 := mttVals[1].Positions
	assert.Equal(t, pos0, pos1, "both tokens must share the same PosRange")

	// Confirm the two tokens differ only in Data (the analyzed term bytes).
	assert.NotEqual(t, mttVals[0].Data, mttVals[1].Data, "tokens should be distinct")
}

// TestAnalyzeNestedValue2_NilItems verifies that analyzeNestedValue2 returns
// nil when analyzeValue returns nil items (unrecognised DataType default case).
func TestAnalyzeNestedValue2_NilItems(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	// DataType "" is not handled by analyzeValue and falls to the default case
	// which returns (nil, nil).
	ve := nested2.ValueEntry{
		Path:     "x",
		PropName: "x",
		Value:    "ignored",
		DataType: schema.DataType(""),
	}
	cfg := nestedIndexConfig{filterable: true}
	result, err := analyzer.analyzeNestedValue2(ve, cfg)
	require.NoError(t, err)
	assert.Nil(t, result)
}

// TestAnalyzeNestedProp2_MixedLeafGating is a comprehensive gating-matrix test
// covering every leaf kind: indexed scalar, non-indexed scalar, indexed
// scalar-array, non-indexed scalar-array, and leaves inside an object-array
// intermediate (one indexed, one non-indexed). "Non-indexed" means all of
// IndexFilterable/IndexSearchable/IndexRangeFilters explicitly false.
//
// Mirrors v1's "per-leaf filterable gates values and exists across mixed leaf
// types" (objects_nested_test.go), adapted to NestedProperty2 accessors.
func TestAnalyzeNestedProp2_MixedLeafGating(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	off := boolPtr(false)
	prop := &models.Property{
		Name:     "cars",
		DataType: schema.DataTypeObjectArray.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: "field", IndexFilterable: boolPtr(true)},
			{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: "field", IndexFilterable: off, IndexSearchable: off},
			{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "field", IndexFilterable: boolPtr(true)},
			{Name: "repair_years", DataType: schema.DataTypeIntArray.PropString(), IndexFilterable: off, IndexRangeFilters: off},
			{
				Name:     "tires",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(true)},
					{Name: "brand", DataType: schema.DataTypeText.PropString(), Tokenization: "field", IndexFilterable: off, IndexSearchable: off},
				},
			},
		},
	}
	value := []any{
		map[string]any{
			"make":         "Toyota",
			"color":        "red",
			"tags":         []any{"family", "hybrid"},
			"repair_years": []any{float64(2020), float64(2021)},
			"tires": []any{
				map[string]any{"width": float64(215), "brand": "Michelin"},
			},
		},
	}

	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, value)
	require.NoError(t, err)
	require.NotNil(t, np)

	// Values: only indexed leaves contribute.
	// "field" tokenization produces exactly one token per string.
	valuePaths := map[string]int{}
	for v := range np.Values() {
		valuePaths[v.Path]++
	}
	assert.Equal(t, 1, valuePaths["make"], "indexed scalar leaf produces one value")
	assert.Equal(t, 2, valuePaths["tags"], "indexed scalar-array leaf produces one value per token")
	assert.Equal(t, 1, valuePaths["tires.width"], "indexed leaf inside object-array intermediate produces one value")
	assert.Zero(t, valuePaths["color"], "non-indexable scalar leaf must not appear in values")
	assert.Zero(t, valuePaths["repair_years"], "non-indexable scalar-array leaf must not appear in values")
	assert.Zero(t, valuePaths["tires.brand"], "non-indexable leaf inside object-array intermediate must not appear in values")

	// NumFilterable: make(1) + tags(2) + tires.width(1) = 4 filterable values.
	// Sizes the pre-allocated slice in nestedFilterableEntries2.
	assert.Equal(t, 4, np.NumFilterable())

	// Exists: same gating as values plus structural sentinels for IS NULL.
	existsPaths := map[string]bool{}
	for e := range np.Exists() {
		existsPaths[e.Path] = true
	}
	assert.True(t, existsPaths[""], "root sentinel _exists always kept")
	assert.True(t, existsPaths["tires"], "intermediate object-array _exists always kept")
	assert.True(t, existsPaths["make"], "indexed scalar leaf _exists kept")
	assert.True(t, existsPaths["tags"], "indexed scalar-array leaf _exists kept")
	assert.True(t, existsPaths["tires.width"], "indexed leaf inside intermediate _exists kept")
	assert.False(t, existsPaths["color"], "non-indexable scalar leaf _exists dropped")
	assert.False(t, existsPaths["repair_years"], "non-indexable scalar-array leaf _exists dropped")
	assert.False(t, existsPaths["tires.brand"], "non-indexable leaf inside intermediate _exists dropped")

	// Idx: ungated, one entry per array element. Object-array intermediates and
	// scalar-array elements both produce Idx entries. Only the two object-array
	// paths are asserted here (mirroring v1); scalar-array Idx entries also exist.
	idxPaths := map[string]bool{}
	for idx := range np.Idx() {
		idxPaths[idx.Path] = true
	}
	assert.True(t, idxPaths[""], "root cars[] idx kept")
	assert.True(t, idxPaths["tires"], "nested object-array idx kept")
}

// TestAnalyzeNestedProp2_AggregateFlags verifies that the aggregate Has*Index
// flags on NestedProperty2 reflect the union of per-leaf index types.
// A property with a searchable-only text leaf and a rangeable number leaf must
// report HasFilterableIndex=false, HasSearchableIndex=true, HasRangeableIndex=true.
//
// Mirrors v1's "aggregate flags reflect index types" (objects_nested_test.go).
func TestAnalyzeNestedProp2_AggregateFlags(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(false)},
			{Name: "price", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
		},
	}
	value := map[string]any{"title": "hello", "price": float64(9.99)}

	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, value)
	require.NoError(t, err)
	require.NotNil(t, np)

	assert.False(t, np.HasFilterableIndex, "no filterable paths")
	assert.True(t, np.HasSearchableIndex, "title is searchable by default")
	assert.True(t, np.HasRangeableIndex, "price is rangeable")
}

// TestAnalyzeNestedProp2_PerValueFlags verifies that the per-entry index flags
// on NestedValue2 reflect each path's config rather than the aggregate flags.
// A text leaf defaults to filterable+searchable; an int leaf with
// IndexRangeFilters=true is filterable+rangeable but not searchable.
//
// Mirrors v1's "per-value flags match path config" (objects_nested_test.go).
func TestAnalyzeNestedProp2_PerValueFlags(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
		},
	}
	value := map[string]any{"city": "Berlin", "count": float64(42)}

	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, value)
	require.NoError(t, err)
	require.NotNil(t, np)

	for v := range np.Values() {
		switch v.Path {
		case "city":
			// text: filterable+searchable by default, not rangeable
			assert.True(t, v.HasFilterableIndex, "city HasFilterableIndex")
			assert.True(t, v.HasSearchableIndex, "city HasSearchableIndex")
			assert.False(t, v.HasRangeableIndex, "city HasRangeableIndex")
		case "count":
			// int with IndexRangeFilters=true: filterable+rangeable, not searchable
			assert.True(t, v.HasFilterableIndex, "count HasFilterableIndex")
			assert.False(t, v.HasSearchableIndex, "count HasSearchableIndex")
			assert.True(t, v.HasRangeableIndex, "count HasRangeableIndex")
		default:
			t.Errorf("unexpected path %q", v.Path)
		}
	}
}

// TestAnalyzeNestedProp2_DatatypeBreadth exercises analyzeNestedProp2 with
// non-text scalar and scalar-array leaf types: number, int, boolean, date, uuid,
// and their array variants. For each, the test asserts that analysis succeeds,
// produces the expected number of values, and the per-value flags match the
// configured index types.
//
// DataType coverage: number/number[], int/int[], boolean/boolean[], date/date[],
// uuid/uuid[]. Text and text-array are covered by other tests in this file.
func TestAnalyzeNestedProp2_DatatypeBreadth(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			// Scalar types: filterable by default, not searchable, not rangeable.
			{Name: "score", DataType: schema.DataTypeNumber.PropString()},
			{Name: "rank", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			{Name: "active", DataType: schema.DataTypeBoolean.PropString()},
			{Name: "birthday", DataType: schema.DataTypeDate.PropString()},
			{Name: "uid", DataType: schema.DataTypeUUID.PropString()},
			// Array variants: each element produces one value entry.
			{Name: "scores", DataType: schema.DataTypeNumberArray.PropString()},
			{Name: "ranks", DataType: schema.DataTypeIntArray.PropString(), IndexRangeFilters: boolPtr(true)},
			{Name: "flags", DataType: schema.DataTypeBooleanArray.PropString()},
			{Name: "dates", DataType: schema.DataTypeDateArray.PropString()},
			{Name: "uids", DataType: schema.DataTypeUUIDArray.PropString()},
		},
	}
	value := map[string]any{
		"score":    float64(9.5),
		"rank":     float64(3),
		"active":   true,
		"birthday": "2000-01-01T00:00:00Z",
		"uid":      "550e8400-e29b-41d4-a716-446655440000",
		"scores":   []any{float64(1.0), float64(2.0)},
		"ranks":    []any{float64(10), float64(20)},
		"flags":    []any{true, false},
		"dates":    []any{"2000-01-01T00:00:00Z", "2001-01-01T00:00:00Z"},
		"uids":     []any{"550e8400-e29b-41d4-a716-446655440001", "550e8400-e29b-41d4-a716-446655440002"},
	}

	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	np, err := analyzer.analyzeNestedProp2(ls, prop, value)
	require.NoError(t, err)
	require.NotNil(t, np, "non-nil value with indexed leaves must produce a non-nil NestedProperty2")

	// Tally values per path: 5 scalars (1 each) + 5 arrays (2 each) = 15.
	valuePaths := map[string]int{}
	for v := range np.Values() {
		valuePaths[v.Path]++
	}
	assert.Equal(t, 1, valuePaths["score"], "number scalar")
	assert.Equal(t, 1, valuePaths["rank"], "int scalar")
	assert.Equal(t, 1, valuePaths["active"], "boolean scalar")
	assert.Equal(t, 1, valuePaths["birthday"], "date scalar")
	assert.Equal(t, 1, valuePaths["uid"], "uuid scalar")
	assert.Equal(t, 2, valuePaths["scores"], "number array (2 elements)")
	assert.Equal(t, 2, valuePaths["ranks"], "int array (2 elements)")
	assert.Equal(t, 2, valuePaths["flags"], "boolean array (2 elements)")
	assert.Equal(t, 2, valuePaths["dates"], "date array (2 elements)")
	assert.Equal(t, 2, valuePaths["uids"], "uuid array (2 elements)")

	// Per-value flag verification: all paths filterable; only rank/ranks rangeable;
	// none searchable (no text types in this fixture).
	for v := range np.Values() {
		assert.True(t, v.HasFilterableIndex, "path %s: filterable expected", v.Path)
		assert.False(t, v.HasSearchableIndex, "path %s: searchable must be false for non-text types", v.Path)
		switch v.Path {
		case "rank", "ranks":
			assert.True(t, v.HasRangeableIndex, "path %s: rangeable expected (IndexRangeFilters=true)", v.Path)
		default:
			assert.False(t, v.HasRangeableIndex, "path %s: rangeable expected false", v.Path)
		}
	}

	// HasMetaEntries() must be true for any non-nil result: root sentinels are
	// always appended to Exists by AssignPositionsFromSchema.
	assert.True(t, np.HasMetaEntries())
}

// TestAnalyzeNestedProp2_Doc124Addresses exercises the more complex Doc124
// fixture: owner (object) + addresses (object[]) with scalar subarrays.
// It verifies value counts and Idx/Exists/Anchors raw lengths.
func TestAnalyzeNestedProp2_Doc124Addresses(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")
	prop := &models.Property{
		Name:     "nested",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{
				Name:     "owner",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
				},
			},
			{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					{Name: "numbers", DataType: schema.DataTypeNumberArray.PropString()},
				},
			},
		},
	}
	ls, err := nested2.BuildSchema(prop)
	require.NoError(t, err)

	value := map[string]any{
		"owner": map[string]any{
			"firstname": "Justin",
			"lastname":  "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
			map[string]any{"city": "London", "postcode": "SW1"},
		},
	}

	np, err := analyzer.analyzeNestedProp2(ls, prop, value)
	require.NoError(t, err)
	require.NotNil(t, np)

	// Values: nickname "watch"(1) + firstname "Justin"(1) + lastname "Time"(1)
	//       + city "Madrid"(1) + postcode "28001"(1) + number 124(1)
	//       + city "London"(1) + postcode "SW1"(1) = 8.
	// (number values use the filterable path only and produce exactly 1 Countable)
	d124Vals := collectValues2(np)
	assert.Len(t, d124Vals, 8)

	// Idx entries: nicknames[0](1) + owner[0](1, DataTypeObject wrapped as 1-element)
	// + numbers[0](1) + addresses[0](1) + addresses[1](1) + root ""[0](1) = 6.
	assert.Len(t, collectIdxEntries2(np), 6)

	// All Value entries have HasFilterableIndex=true (defaults).
	for _, v := range d124Vals {
		assert.True(t, v.HasFilterableIndex, "path %s", v.Path)
	}

	assert.True(t, np.HasFilterableEntries())
	assert.True(t, np.HasMetaEntries())
	assert.False(t, np.HasSearchableEntries())
	assert.False(t, np.HasRangeableEntries())
}
