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

func TestParseSegmentIndex(t *testing.T) {
	tests := []struct {
		name      string
		seg       string
		wantClean string
		wantIdx   int
		wantHas   bool
	}{
		// valid cases
		{"index at end", "tags[2]", "tags", 2, true},
		{"zero index", "tags[0]", "tags", 0, true},
		{"multi-digit index", "cars[10]", "cars", 10, true},
		{"index only — empty name", "[3]", "", 3, true},
		// no index
		{"plain segment", "tags", "tags", 0, false},
		{"empty string", "", "", 0, false},
		// bracket not at the end — treated as no index
		{"bracket in the middle", "ta[2]gs", "ta[2]gs", 0, false},
		{"trailing chars after bracket", "tags[2]x", "tags[2]x", 0, false},
		// malformed
		{"empty index", "tags[]", "tags[]", 0, false},
		{"negative index", "tags[-1]", "tags[-1]", 0, false},
		{"non-numeric index", "tags[abc]", "tags[abc]", 0, false},
		{"unclosed bracket", "tags[", "tags[", 0, false},
		{"no opening bracket", "tags]", "tags]", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clean, idx, has := parseSegmentIndex(tt.seg)
			assert.Equal(t, tt.wantClean, clean)
			assert.Equal(t, tt.wantIdx, idx)
			assert.Equal(t, tt.wantHas, has)
		})
	}
}

func TestParseSegments(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []PathSegment
	}{
		{
			name: "single segment no index",
			path: "addresses",
			want: []PathSegment{{Name: "addresses"}},
		},
		{
			name: "single segment with index",
			path: "addresses[1]",
			want: []PathSegment{{Name: "addresses", HasIndex: true, Index: 1}},
		},
		{
			name: "two segments no index",
			path: "addresses.city",
			want: []PathSegment{{Name: "addresses"}, {Name: "city"}},
		},
		{
			name: "root index with sub-property",
			path: "addresses[1].city",
			want: []PathSegment{{Name: "addresses", HasIndex: true, Index: 1}, {Name: "city"}},
		},
		{
			name: "sub-property index",
			path: "cars.tags[2]",
			want: []PathSegment{{Name: "cars"}, {Name: "tags", HasIndex: true, Index: 2}},
		},
		{
			name: "multi-level indexes",
			path: "cars[1].tags[2]",
			want: []PathSegment{
				{Name: "cars", HasIndex: true, Index: 1},
				{Name: "tags", HasIndex: true, Index: 2},
			},
		},
		{
			name: "three segments no index",
			path: "cars.tires.width",
			want: []PathSegment{{Name: "cars"}, {Name: "tires"}, {Name: "width"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ParseSegments(tt.path))
		})
	}
}

func TestParseIndexedPath(t *testing.T) {
	tests := []struct {
		name             string
		path             string
		wantRelPath      string
		wantArrayIndices []ArrayIndex
	}{
		{
			name:        "no index — plain nested path unchanged",
			path:        "addresses.city",
			wantRelPath: "city",
		},
		{
			name:             "root-level index — addresses[1].city",
			path:             "addresses[1].city",
			wantRelPath:      "city",
			wantArrayIndices: []ArrayIndex{{RelPath: "", Index: 1}},
		},
		{
			name:             "leaf scalar array index — cars.tires.radiuses[2]",
			path:             "cars.tires.radiuses[2]",
			wantRelPath:      "tires.radiuses",
			wantArrayIndices: []ArrayIndex{{RelPath: "tires.radiuses", Index: 2}},
		},
		{
			name:             "multi-level index — cars[1].tags[2]",
			path:             "cars[1].tags[2]",
			wantRelPath:      "tags",
			wantArrayIndices: []ArrayIndex{{RelPath: "", Index: 1}, {RelPath: "tags", Index: 2}},
		},
		{
			name:             "root only with index — addresses[1]",
			path:             "addresses[1]",
			wantRelPath:      "",
			wantArrayIndices: []ArrayIndex{{RelPath: "", Index: 1}},
		},
		{
			name:             "zero index — tags[0]",
			path:             "cars.tags[0]",
			wantRelPath:      "tags",
			wantArrayIndices: []ArrayIndex{{RelPath: "tags", Index: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relPath, _, arrayIndices := ParseIndexedPath(tt.path)
			assert.Equal(t, tt.wantRelPath, relPath)
			assert.Equal(t, tt.wantArrayIndices, arrayIndices)
		})
	}
}

func TestRootPropName(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"addresses.city", "addresses"},
		{"addresses[1].city", "addresses"},
		{"addresses[1]", "addresses"},
		{"cars[0].tires.width", "cars"},
		{"name", "name"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, RootPropName(tt.path))
		})
	}
}

// testSchemaClass builds a class with a representative nested schema for
// ResolveLeaf tests:
//
//	carsCollection {
//	    cars       object[]    // root array
//	      .make    text        // scalar leaf
//	      .tires   object[]    // intermediate array
//	        .width int         // deep scalar leaf
//	        .brand text
//	    country    object      // root single-object
//	      .name    text        // scalar leaf
//	      .tags    text[]      // scalar array leaf
//	      .garages object[]    // intermediate array under single-object root
//	        .city  text
//	}
func testSchemaClass() *models.Class {
	tiresProps := []*models.NestedProperty{
		{Name: "width", DataType: schema.DataTypeInt.PropString()},
		{Name: "brand", DataType: schema.DataTypeText.PropString()},
	}
	carsProps := []*models.NestedProperty{
		{Name: "make", DataType: schema.DataTypeText.PropString()},
		{Name: "tires", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: tiresProps},
	}
	garagesProps := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString()},
	}
	countryProps := []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString()},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString()},
		{Name: "garages", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: garagesProps},
	}
	return &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "cars", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: carsProps},
			{Name: "country", DataType: schema.DataTypeObject.PropString(), NestedProperties: countryProps},
		},
	}
}

func TestIsNestedPath(t *testing.T) {
	// Class with a nested OBJECT_ARRAY (cars), a nested OBJECT (country),
	// and a flat text leaf at the top level (title).
	carsProps := []*models.NestedProperty{
		{Name: "make", DataType: schema.DataTypeText.PropString()},
	}
	countryProps := []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString()},
	}
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "cars", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: carsProps},
			{Name: "country", DataType: schema.DataTypeObject.PropString(), NestedProperties: countryProps},
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}

	cases := []struct {
		name string
		path string
		want bool
	}{
		{"dotted path on OBJECT_ARRAY root", "cars.make", true},
		{"dotted path on OBJECT root", "country.name", true},
		{"indexed dotted path on OBJECT_ARRAY root", "cars[0].make", true},
		{"deeper dotted path", "cars.tires.width", true},

		{"no dot — flat property name", "title", false},
		{"no dot — nested root only (legitimately not nested-path)", "cars", false},
		{"dotted path on flat text property — must not be classified as nested", "title.foo", false},
		{"dotted path with non-existent root — falls through to flat handling", "nonexistent.foo", false},
		{"empty string", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsNestedPath(class, tc.path))
		})
	}

	t.Run("nil class returns false", func(t *testing.T) {
		assert.False(t, IsNestedPath(nil, "cars.make"))
	})
}

func TestResolveLeaf(t *testing.T) {
	class := testSchemaClass()

	t.Run("happy paths return the expected leaf", func(t *testing.T) {
		cases := []struct {
			path     string
			leafName string
			leafDT   schema.DataType
		}{
			{"cars.make", "make", schema.DataTypeText},
			{"cars[0].make", "make", schema.DataTypeText},
			{"cars.tires.width", "width", schema.DataTypeInt},
			{"cars[0].tires[1].width", "width", schema.DataTypeInt},
			{"country.name", "name", schema.DataTypeText},
			{"country.tags", "tags", schema.DataTypeTextArray},
			{"country.garages.city", "city", schema.DataTypeText},
			// Object-typed leaves are returned without error — the rejection
			// rule for non-IsNull filters lives in the caller, not here.
			{"cars.tires", "tires", schema.DataTypeObjectArray},
			{"country.garages", "garages", schema.DataTypeObjectArray},
		}
		for _, tc := range cases {
			t.Run(tc.path, func(t *testing.T) {
				leaf, err := ResolveLeaf(class, tc.path)
				require.NoError(t, err)
				require.NotNil(t, leaf)
				assert.Equal(t, tc.leafName, leaf.Name)
				assert.Equal(t, string(tc.leafDT), leaf.DataType[0])
			})
		}
	})

	t.Run("root property does not exist on the class", func(t *testing.T) {
		_, err := ResolveLeaf(class, "missingroot.foo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missingroot")
	})

	t.Run("path has no sub-property segments", func(t *testing.T) {
		_, err := ResolveLeaf(class, "cars")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no sub-property segments")
	})

	t.Run("sub-property not found", func(t *testing.T) {
		_, err := ResolveLeaf(class, "cars.nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `nested path "cars.nonexistent"`)
		assert.Contains(t, err.Error(), `sub-property "nonexistent" not found`)
	})

	t.Run("path tries to descend past a scalar leaf", func(t *testing.T) {
		_, err := ResolveLeaf(class, "cars.make.foo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `nested path "cars.make.foo"`)
		assert.Contains(t, err.Error(), `sub-property "make" must be object or object[]`)
	})

	t.Run("[N] on root requires array type", func(t *testing.T) {
		// country is a single OBJECT, not OBJECT_ARRAY.
		_, err := ResolveLeaf(class, "country[0].name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `property "country"`)
		assert.Contains(t, err.Error(), "[N] indexing requires an array type")
	})

	t.Run("[N] on intermediate sub-property requires array type", func(t *testing.T) {
		// country.name is text — [N] not valid.
		_, err := ResolveLeaf(class, "country.name[0]")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `sub-property "name"`)
		assert.Contains(t, err.Error(), "[N] indexing requires an array type")
	})

	t.Run("[N] on intermediate object[] is valid", func(t *testing.T) {
		leaf, err := ResolveLeaf(class, "country.garages[0].city")
		require.NoError(t, err)
		assert.Equal(t, "city", leaf.Name)
	})
}

func TestResolveLeafFromRoot(t *testing.T) {
	class := testSchemaClass()
	carsProp := class.Properties[0]    // cars
	countryProp := class.Properties[1] // country

	t.Run("uses the supplied root prop without re-looking-up", func(t *testing.T) {
		leaf, err := ResolveLeafFromRoot(carsProp, "cars.make")
		require.NoError(t, err)
		assert.Equal(t, "make", leaf.Name)
	})

	t.Run("same path-shape checks apply", func(t *testing.T) {
		_, err := ResolveLeafFromRoot(carsProp, "cars.make.foo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be object or object[]")
	})

	t.Run("[N] on non-array root reported via supplied root prop", func(t *testing.T) {
		_, err := ResolveLeafFromRoot(countryProp, "country[0].name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `property "country"`)
		assert.Contains(t, err.Error(), "[N] indexing requires an array type")
	})

	t.Run("path with no sub-property segments", func(t *testing.T) {
		_, err := ResolveLeafFromRoot(carsProp, "cars")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no sub-property segments")
	})
}
