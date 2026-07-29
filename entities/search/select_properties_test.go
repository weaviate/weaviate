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

package search_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

// testProperty adapts models.Property to schema.PropertyInterface the same way
// the gRPC consumer does (adapters/handlers/grpc/v1/models.go), so that the
// direct tests of the exported generic mirror real API usage from outside the
// search package.
type testProperty struct {
	*models.Property
}

func (p *testProperty) GetName() string {
	return p.Name
}

func (p *testProperty) GetNestedProperties() []*models.NestedProperty {
	return p.NestedProperties
}

func TestAllNonRefNonBlobProperties(t *testing.T) {
	tests := []struct {
		name     string
		class    *models.Class
		expected []search.SelectProperty
	}{
		{
			name:     "class with no properties returns nil",
			class:    &models.Class{Class: "Empty"},
			expected: nil,
		},
		{
			name: "primitive properties preserve schema order",
			class: &models.Class{
				Class: "Primitives",
				Properties: []*models.Property{
					{Name: "title", DataType: schema.DataTypeText.PropString()},
					{Name: "count", DataType: schema.DataTypeInt.PropString()},
					{Name: "enabled", DataType: schema.DataTypeBoolean.PropString()},
					{Name: "score", DataType: schema.DataTypeNumber.PropString()},
					{Name: "createdAt", DataType: schema.DataTypeDate.PropString()},
					{Name: "identifier", DataType: schema.DataTypeUUID.PropString()},
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString()},
				},
			},
			expected: []search.SelectProperty{
				{Name: "title", IsPrimitive: true},
				{Name: "count", IsPrimitive: true},
				{Name: "enabled", IsPrimitive: true},
				{Name: "score", IsPrimitive: true},
				{Name: "createdAt", IsPrimitive: true},
				{Name: "identifier", IsPrimitive: true},
				{Name: "tags", IsPrimitive: true},
			},
		},
		{
			name: "refs blobs and blobHashes are skipped at class level",
			class: &models.Class{
				Class: "Skips",
				Properties: []*models.Property{
					{Name: "title", DataType: schema.DataTypeText.PropString()},
					{Name: "hasRef", DataType: []string{"RefTarget"}},
					{Name: "image", DataType: schema.DataTypeBlob.PropString()},
					{Name: "imageHash", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "count", DataType: schema.DataTypeInt.PropString()},
				},
			},
			expected: []search.SelectProperty{
				{Name: "title", IsPrimitive: true},
				{Name: "count", IsPrimitive: true},
			},
		},
		{
			name: "object property recurses and applies the skip set to nested children",
			class: &models.Class{
				Class: "WithObject",
				Properties: []*models.Property{
					{
						Name:     "meta",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "source", DataType: schema.DataTypeText.PropString()},
							{Name: "nestedRef", DataType: []string{"RefTarget"}},
							{Name: "raw", DataType: schema.DataTypeBlob.PropString()},
							{Name: "rawHash", DataType: schema.DataTypeBlobHash.PropString()},
							{Name: "weight", DataType: schema.DataTypeNumber.PropString()},
						},
					},
				},
			},
			expected: []search.SelectProperty{
				{
					Name:     "meta",
					IsObject: true,
					Props: []search.SelectProperty{
						{Name: "source", IsPrimitive: true},
						{Name: "weight", IsPrimitive: true},
					},
				},
			},
		},
		{
			name: "object array property recurses like object",
			class: &models.Class{
				Class: "WithObjectArray",
				Properties: []*models.Property{
					{
						Name:     "entries",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "label", DataType: schema.DataTypeText.PropString()},
							{Name: "thumbnail", DataType: schema.DataTypeBlob.PropString()},
						},
					},
				},
			},
			expected: []search.SelectProperty{
				{
					Name:     "entries",
					IsObject: true,
					Props: []search.SelectProperty{
						{Name: "label", IsPrimitive: true},
					},
				},
			},
		},
		{
			name: "multi-level nesting applies the skip set at depth",
			class: &models.Class{
				Class: "DeepNesting",
				Properties: []*models.Property{
					{
						Name:     "profile",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "name", DataType: schema.DataTypeText.PropString()},
							{
								Name:     "address",
								DataType: schema.DataTypeObject.PropString(),
								NestedProperties: []*models.NestedProperty{
									{Name: "street", DataType: schema.DataTypeText.PropString()},
									{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
									{Name: "photoHash", DataType: schema.DataTypeBlobHash.PropString()},
									{Name: "owner", DataType: []string{"RefTarget"}},
								},
							},
						},
					},
				},
			},
			expected: []search.SelectProperty{
				{
					Name:     "profile",
					IsObject: true,
					Props: []search.SelectProperty{
						{Name: "name", IsPrimitive: true},
						{
							Name:     "address",
							IsObject: true,
							Props: []search.SelectProperty{
								{Name: "street", IsPrimitive: true},
							},
						},
					},
				},
			},
		},
		{
			name: "mixed class with every property kind preserves order",
			class: &models.Class{
				Class: "Mixed",
				Properties: []*models.Property{
					{Name: "title", DataType: schema.DataTypeText.PropString()},
					{Name: "hasRef", DataType: []string{"RefTarget"}},
					{
						Name:     "meta",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "source", DataType: schema.DataTypeText.PropString()},
						},
					},
					{Name: "image", DataType: schema.DataTypeBlob.PropString()},
					{
						Name:     "entries",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "label", DataType: schema.DataTypeText.PropString()},
						},
					},
					{Name: "imageHash", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "count", DataType: schema.DataTypeInt.PropString()},
				},
			},
			expected: []search.SelectProperty{
				{Name: "title", IsPrimitive: true},
				{
					Name:     "meta",
					IsObject: true,
					Props: []search.SelectProperty{
						{Name: "source", IsPrimitive: true},
					},
				},
				{
					Name:     "entries",
					IsObject: true,
					Props: []search.SelectProperty{
						{Name: "label", IsPrimitive: true},
					},
				},
				{Name: "count", IsPrimitive: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := search.AllNonRefNonBlobProperties(tt.class)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestAllNonRefNonBlobProperties_AllNestedSkippedYieldsNilProps pins the
// nil-when-empty behavior: an object property whose nested children are all
// skipped gets Props == nil, not an empty slice. This matches what the gRPC
// path historically produced (parse_search_request.go builds SelectProperty
// values the same way), so downstream consumers must keep seeing nil here.
func TestAllNonRefNonBlobProperties_AllNestedSkippedYieldsNilProps(t *testing.T) {
	class := &models.Class{
		Class: "OnlySkippedNested",
		Properties: []*models.Property{
			{
				Name:     "attachments",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "data", DataType: schema.DataTypeBlob.PropString()},
					{Name: "dataHash", DataType: schema.DataTypeBlobHash.PropString()},
				},
			},
		},
	}

	got, err := search.AllNonRefNonBlobProperties(class)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "attachments", got[0].Name)
	assert.True(t, got[0].IsObject)
	assert.Nil(t, got[0].Props)
}

// TestAllNonRefNonBlobProperties_Errors covers the reachable failure modes.
// The property-name lookup inside the walk cannot fail (it iterates
// class.Properties itself), so the class-level error path is triggered via an
// invalid data-type string: a lowercase unknown type is not mistaken for a
// ref by schema.IsRefDataType and makes schema.GetValueDataTypeFromString
// fail. (An uppercase unknown type would be treated as a cross-reference and
// silently skipped instead.)
func TestAllNonRefNonBlobProperties_Errors(t *testing.T) {
	tests := []struct {
		name          string
		class         *models.Class
		wantWrap      string
		wantNotWrap   string
		wantCauseText string
	}{
		{
			name: "unknown lowercase data type on a class property",
			class: &models.Class{
				Class: "BadDataType",
				Properties: []*models.Property{
					{Name: "broken", DataType: []string{"unknownType"}},
				},
			},
			wantWrap:      "get property data type: ",
			wantCauseText: schema.ErrorNoSuchDatatype,
		},
		{
			name: "empty-string data type on a class property",
			class: &models.Class{
				Class: "EmptyDataType",
				Properties: []*models.Property{
					{Name: "broken", DataType: []string{""}},
				},
			},
			wantWrap:      "get property data type: ",
			wantCauseText: "invalid-dataType",
		},
		{
			name: "unknown data type inside a nested property carries the intermediate wrap",
			class: &models.Class{
				Class: "BadNestedDataType",
				Properties: []*models.Property{
					{
						Name:     "meta",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "broken", DataType: []string{"unknownType"}},
						},
					},
				},
			},
			// Faithful port: the recursive branch re-wraps, so the class-level
			// error carries the intermediate "get all non ref non blob nested
			// properties" prefix, byte-identical to the original gRPC message.
			wantWrap:      "get all non ref non blob nested properties: get nested property data type: ",
			wantNotWrap:   "get property data type: ",
			wantCauseText: schema.ErrorNoSuchDatatype,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := search.AllNonRefNonBlobProperties(tt.class)
			require.Error(t, err)
			// Faithful port: every error branch returns an empty (non-nil)
			// slice, matching the original gRPC implementation.
			assert.Equal(t, []search.SelectProperty{}, got)
			assert.NotNil(t, got)
			assert.ErrorContains(t, err, tt.wantWrap)
			if tt.wantNotWrap != "" {
				assert.NotContains(t, err.Error(), tt.wantNotWrap)
			}

			// %w wrapping must expose the leaf cause through the (possibly
			// multi-level) chain to errors.Unwrap/errors.Is.
			leaf := err
			for next := errors.Unwrap(leaf); next != nil; next = errors.Unwrap(leaf) {
				leaf = next
			}
			require.NotEqual(t, err, leaf)
			assert.EqualError(t, leaf, tt.wantCauseText)
			assert.ErrorIs(t, err, leaf)
		})
	}
}

// TestAllNonRefNonBlobNestedProperties_DirectAdapter exercises the exported
// generic through a locally-defined schema.PropertyInterface adapter, the way
// the gRPC parser calls it with its own Property adapter.
func TestAllNonRefNonBlobNestedProperties_DirectAdapter(t *testing.T) {
	tests := []struct {
		name     string
		prop     *models.Property
		expected []search.SelectProperty
	}{
		{
			name: "no nested properties returns nil",
			prop: &models.Property{
				Name:     "meta",
				DataType: schema.DataTypeObject.PropString(),
			},
			expected: nil,
		},
		{
			name: "nested primitives preserve order, skip set applies",
			prop: &models.Property{
				Name:     "meta",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "source", DataType: schema.DataTypeText.PropString()},
					{Name: "raw", DataType: schema.DataTypeBlob.PropString()},
					{Name: "rawHash", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "nestedRef", DataType: []string{"RefTarget"}},
					{Name: "weight", DataType: schema.DataTypeNumber.PropString()},
				},
			},
			expected: []search.SelectProperty{
				{Name: "source", IsPrimitive: true},
				{Name: "weight", IsPrimitive: true},
			},
		},
		{
			name: "all nested children skipped returns nil, not empty slice",
			prop: &models.Property{
				Name:     "attachments",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "data", DataType: schema.DataTypeBlob.PropString()},
					{Name: "dataHash", DataType: schema.DataTypeBlobHash.PropString()},
				},
			},
			expected: nil,
		},
		{
			name: "recursion into nested object children",
			prop: &models.Property{
				Name:     "profile",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
					{
						Name:     "address",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "street", DataType: schema.DataTypeText.PropString()},
							{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
						},
					},
				},
			},
			expected: []search.SelectProperty{
				{Name: "name", IsPrimitive: true},
				{
					Name:     "address",
					IsObject: true,
					Props: []search.SelectProperty{
						{Name: "street", IsPrimitive: true},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := search.AllNonRefNonBlobNestedProperties(&testProperty{tt.prop})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestAllNonRefNonBlobNestedProperties_DirectAdapter_Error(t *testing.T) {
	prop := &models.Property{
		Name:     "meta",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "broken", DataType: []string{"unknownType"}},
		},
	}

	got, err := search.AllNonRefNonBlobNestedProperties(&testProperty{prop})
	require.Error(t, err)
	// Faithful port: error branches return an empty (non-nil) slice.
	assert.Equal(t, []search.SelectProperty{}, got)
	assert.NotNil(t, got)
	assert.ErrorContains(t, err, "get nested property data type: ")

	cause := errors.Unwrap(err)
	require.NotNil(t, cause)
	assert.EqualError(t, cause, schema.ErrorNoSuchDatatype)
	assert.ErrorIs(t, err, cause)
}
