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
			{Name: "tagsArray", DataType: schema.DataTypeTextArray.PropString()},
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
			{
				// object[] at the top level — valid target for root [N] indexing
				Name:     "items",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "value", DataType: schema.DataTypeText.PropString(), IndexFilterable: &boolTrue},
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), IndexFilterable: &boolTrue},
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
		// --- indexed paths: [N] stripped before schema lookup ---
		{
			// nested is DataTypeObject (not object[]) — use items which is object[]
			name:      "root index on object[] — items[1].value",
			propName:  "items[1].value",
			valueType: schema.DataTypeText,
			value:     "hello",
		},
		{
			name:      "sub-property index — nested.tags[1]",
			propName:  "nested.tags[1]",
			valueType: schema.DataTypeText,
			value:     "tag",
		},
		{
			// items[1].tags[0]: root object[] index + scalar array sub-property index
			name:      "multi-level indexes — items[1].tags[0]",
			propName:  "items[1].tags[0]",
			valueType: schema.DataTypeText,
			value:     "tag",
		},
		{
			name:      "object array sub-property with index — nested.addresses[0].city",
			propName:  "nested.addresses[0].city",
			valueType: schema.DataTypeText,
			value:     "Berlin",
		},
		{
			name:      "non-existent sub-property with index still rejected",
			propName:  "nested.missing[1]",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `sub-property "missing" not found`,
		},
		// --- index on non-array types is rejected ---
		{
			name:      "root object (not object[]) with index rejected",
			propName:  "nested[0].city",
			valueType: schema.DataTypeText,
			value:     "Berlin",
			wantErr:   `"nested" is of type "object" — [N] indexing requires an array type`,
		},
		{
			name:      "scalar sub-property with index rejected",
			propName:  "nested.name[1]",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `sub-property "name" is of type "text" — [N] indexing requires an array type`,
		},
		{
			name:      "int sub-property with index rejected",
			propName:  "nested.count[0]",
			valueType: schema.DataTypeInt,
			value:     42,
			wantErr:   `sub-property "count" is of type "int" — [N] indexing requires an array type`,
		},
		{
			name:      "object (not object[]) sub-property with index rejected",
			propName:  "nested.owner[0].firstname",
			valueType: schema.DataTypeText,
			value:     "Alice",
			wantErr:   `sub-property "owner" is of type "object" — [N] indexing requires an array type`,
		},
		// --- [N] on flat (non-nested) properties is rejected, so positional
		// intent is never silently dropped ---
		{
			name:      "[N] rejected on flat array property",
			propName:  "tagsArray[0]",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `[N] indexing is only supported on nested object[] properties`,
		},
		{
			name:      "[N] rejected on flat scalar property",
			propName:  "title[0]",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   `[N] indexing is only supported on nested object[] properties`,
		},
		{
			// A dotted path on a flat property must not be misreported as a
			// "[N] indexing" error — the path contains no [N], so the
			// [N]-rejection branch must not fire. The current code falls
			// through and allows the filter (treating "title.foo" as a filter
			// on the flat "title" prop); whether that's correct is a separate
			// concern. This test asserts only that the [N] branch is gated
			// by an actual [N], not by a stripped dot.
			name:      "dotted path on flat property does not produce [N] error",
			propName:  "title.foo",
			valueType: schema.DataTypeText,
			value:     "x",
			wantErr:   "",
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
	isNullClause := func(propName string) *Clause {
		return &Clause{
			Operator: OperatorIsNull,
			Value:    &Value{Value: true, Type: schema.DataTypeBoolean},
			On:       &Path{Class: "Article", Property: schema.PropertyName(propName)},
		}
	}

	tests := []struct {
		name     string
		propName string
		wantErr  string
	}{
		// root-level IsNull (no dot notation) — checks object existence
		{
			name:     "root object IsNull — no dot, valid",
			propName: "nested",
		},
		// sub-property IsNull on text — bool value accepted regardless of leaf type
		{
			name:     "sub-property text IsNull",
			propName: "nested.name",
		},
		// sub-property IsNull on int — bool value accepted regardless of leaf type
		{
			name:     "sub-property int IsNull",
			propName: "nested.count",
		},
		// sub-property IsNull on text[] — bool value accepted regardless of leaf type
		{
			name:     "sub-property textArray IsNull",
			propName: "nested.tags",
		},
		// sub-property inside nested object[]
		{
			name:     "nested object[] sub-property IsNull",
			propName: "nested.addresses.city",
		},
		// nested object sub-property IsNull — leaf is object, valid for IsNull
		{
			name:     "nested object sub-property (nested.owner) IsNull",
			propName: "nested.owner",
		},
		// nested object[] sub-property IsNull — leaf is object[], valid for IsNull
		{
			name:     "nested object[] sub-property (nested.addresses) IsNull",
			propName: "nested.addresses",
		},
		// two-level path through nested object, int leaf
		{
			name:     "two-level path nested.owner.age IsNull",
			propName: "nested.owner.age",
		},
		// non-existent sub-property is now rejected — path is validated
		{
			name:     "non-existent sub-property rejected",
			propName: "nested.missing",
			wantErr:  `"missing" not found`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := isNullClause(tt.propName)
			err := validateClause(nestedGetClass, newClauseWrapper(cl))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
