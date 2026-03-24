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

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// nestedClass builds the shared test class used across nested filter tests:
//
//	Article {
//	  nested: object {
//	    name: text
//	    count: int
//	    tags: text[]
//	    owner: object {
//	      firstname: text
//	      age: int
//	    }
//	    addresses: object[] {
//	      city: text
//	      postcode: text
//	    }
//	  }
//	}
func nestedClass() *models.Class {
	boolTrue := true
	return &models.Class{
		Class: "Article",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{
				Name:     "nested",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "name", DataType: schema.DataTypeText.PropString(), IndexFilterable: &boolTrue},
					{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &boolTrue},
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &boolTrue},
					{
						Name:     "owner",
						DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "firstname", DataType: schema.DataTypeText.PropString(), IndexFilterable: &boolTrue},
							{Name: "age", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &boolTrue},
						},
					},
					{
						Name:     "addresses",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: &boolTrue},
							{Name: "postcode", DataType: schema.DataTypeText.PropString(), IndexFilterable: &boolTrue},
						},
					},
				},
			},
		},
	}
}

func nestedGetClass(name string) (*models.Class, error) {
	return nestedClass(), nil
}

func makeNestedClause(propName string, valueType schema.DataType, value any) *Clause {
	return &Clause{
		Operator: OperatorEqual,
		Value:    &Value{Value: value, Type: valueType},
		On:       &Path{Class: "Article", Property: schema.PropertyName(propName)},
	}
}

func TestValidateNestedPathClause(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		valueType schema.DataType
		value     any
		wantErr   string
	}{
		{
			name:      "one-level text",
			propName:  "nested.name",
			valueType: schema.DataTypeText,
			value:     "hello",
		},
		{
			name:      "one-level int",
			propName:  "nested.count",
			valueType: schema.DataTypeInt,
			value:     42,
		},
		{
			name:      "one-level text array",
			propName:  "nested.tags",
			valueType: schema.DataTypeText,
			value:     "tag1",
		},
		{
			name:      "two-level text",
			propName:  "nested.owner.firstname",
			valueType: schema.DataTypeText,
			value:     "Alice",
		},
		{
			name:      "two-level int",
			propName:  "nested.owner.age",
			valueType: schema.DataTypeInt,
			value:     30,
		},
		{
			name:      "object array leaf text",
			propName:  "nested.addresses.city",
			valueType: schema.DataTypeText,
			value:     "Berlin",
		},
		{
			name:      "wrong value type for text prop",
			propName:  "nested.name",
			valueType: schema.DataTypeInt,
			value:     42,
			wantErr:   `nested path "nested.name": data type filter cannot use "valueInt" on type "text"`,
		},
		{
			name:      "wrong value type for int prop",
			propName:  "nested.count",
			valueType: schema.DataTypeText,
			value:     "hello",
			wantErr:   `nested path "nested.count": data type filter cannot use "valueText" on type "int"`,
		},
		{
			name:      "non-existent leaf",
			propName:  "nested.missing",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `nested path "nested.missing": sub-property "missing" not found`,
		},
		{
			name:      "non-existent intermediate",
			propName:  "nested.missing.city",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `nested path "nested.missing.city": sub-property "missing" not found`,
		},
		{
			name:      "intermediate is not object",
			propName:  "nested.name.something",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `nested path "nested.name.something": sub-property "name" must be object or object[]`,
		},
		{
			name:      "leaf is object type",
			propName:  "nested.owner",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `nested path "nested.owner": sub-property "owner" is of type "object"`,
		},
		{
			name:      "leaf is object array type",
			propName:  "nested.addresses",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `nested path "nested.addresses": sub-property "addresses" is of type "object[]"`,
		},
		{
			name:      "filter directly on object prop without dot",
			propName:  "nested",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `property "nested" is of type "object"; use dot notation to filter on a sub-property`,
		},
		{
			name:      "filter directly on object array prop without dot",
			propName:  "nested.addresses",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `nested path "nested.addresses": sub-property "addresses" is of type "object[]"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := makeNestedClause(tt.propName, tt.valueType, tt.value)
			err := validateClause(nestedGetClass, newClauseWrapper(cl))
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateNestedLengthFilter(t *testing.T) {
	tests := []struct {
		name     string
		propName string
	}{
		{name: "len on top-level nested prop", propName: "len(nested)"},
		{name: "len on nested path", propName: "len(nested.name)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := &Clause{
				Operator: OperatorEqual,
				Value:    &Value{Value: 5, Type: schema.DataTypeInt},
				On:       &Path{Class: "Article", Property: schema.PropertyName(tt.propName)},
			}
			err := validateClause(nestedGetClass, newClauseWrapper(cl))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "property length filtering is not supported for nested properties")
		})
	}
}

func TestValidateNestedIsNull(t *testing.T) {
	// IsNull is validated by the existing check before the nested path
	// delegation, so it passes for any property type including nested.
	cl := &Clause{
		Operator: OperatorIsNull,
		Value:    &Value{Value: true, Type: schema.DataTypeBoolean},
		On:       &Path{Class: "Article", Property: "nested.name"},
	}
	err := validateClause(nestedGetClass, newClauseWrapper(cl))
	require.NoError(t, err)
}
