//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/test_utils"
)

func Test_MergeRecursivelyNestedProperties(t *testing.T) {
	vFalse := false
	vTrue := true

	emptyProps := []*models.NestedProperty{}
	nestedProps1 := []*models.NestedProperty{
		{
			Name:            "nested_int",
			DataType:        schema.DataTypeInt.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "nested_text",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			Tokenization:    models.PropertyTokenizationWord,
		},
		{
			Name:            "nested_objects",
			DataType:        schema.DataTypeObjectArray.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "nested_bool_lvl2",
					DataType:        schema.DataTypeBoolean.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
				{
					Name:            "nested_numbers_lvl2",
					DataType:        schema.DataTypeNumberArray.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
			},
		},
	}
	nestedProps2 := []*models.NestedProperty{
		{
			Name:            "nested_number",
			DataType:        schema.DataTypeNumber.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "nested_text",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			Tokenization:    models.PropertyTokenizationField, // different setting than (1)
		},
		{
			Name:            "nested_objects",
			DataType:        schema.DataTypeObjectArray.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "nested_date_lvl2",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
				{
					Name:            "nested_numbers_lvl2",
					DataType:        schema.DataTypeNumberArray.PropString(),
					IndexFilterable: &vFalse, // different setting than (1)
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
			},
		},
	}

	mergedProps_1_2 := []*models.NestedProperty{
		{
			Name:            "nested_int",
			DataType:        schema.DataTypeInt.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "nested_number",
			DataType:        schema.DataTypeNumber.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "nested_text",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			Tokenization:    models.PropertyTokenizationWord, // from (1)
		},
		{
			Name:            "nested_objects",
			DataType:        schema.DataTypeObjectArray.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "nested_bool_lvl2",
					DataType:        schema.DataTypeBoolean.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
				{
					Name:            "nested_date_lvl2",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
				{
					Name:            "nested_numbers_lvl2",
					DataType:        schema.DataTypeNumberArray.PropString(),
					IndexFilterable: &vTrue, // from (1)
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
			},
		},
	}

	mergedProps_2_1 := []*models.NestedProperty{
		{
			Name:            "nested_int",
			DataType:        schema.DataTypeInt.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "nested_number",
			DataType:        schema.DataTypeNumber.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "nested_text",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			Tokenization:    models.PropertyTokenizationField, // from (2)
		},
		{
			Name:            "nested_objects",
			DataType:        schema.DataTypeObjectArray.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "nested_bool_lvl2",
					DataType:        schema.DataTypeBoolean.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
				{
					Name:            "nested_date_lvl2",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
				{
					Name:            "nested_numbers_lvl2",
					DataType:        schema.DataTypeNumberArray.PropString(),
					IndexFilterable: &vFalse, // from (2)
					IndexSearchable: &vFalse,
					Tokenization:    "",
				},
			},
		},
	}

	t.Run("empty + nested", func(t *testing.T) {
		nestedProps, merged := schema.MergeRecursivelyNestedProperties(emptyProps, nestedProps1)

		assert.True(t, merged)
		assert.Equal(t, nestedProps1, nestedProps)
	})

	t.Run("nested + empty", func(t *testing.T) {
		nestedProps, merged := schema.MergeRecursivelyNestedProperties(nestedProps1, emptyProps)

		assert.False(t, merged)
		assert.Equal(t, nestedProps1, nestedProps)
	})

	t.Run("2 x nested", func(t *testing.T) {
		nestedProps, merged := schema.MergeRecursivelyNestedProperties(nestedProps1, nestedProps1)

		assert.False(t, merged)
		assert.Equal(t, nestedProps1, nestedProps)
	})

	t.Run("nested1 + nested2", func(t *testing.T) {
		nestedProps, merged := schema.MergeRecursivelyNestedProperties(nestedProps1, nestedProps2)

		assert.True(t, merged)
		assert.NotEqual(t, nestedProps1, nestedProps)
		assert.NotEqual(t, nestedProps2, nestedProps)
		test_utils.AssertNestedPropsMatch(t, mergedProps_1_2, nestedProps)
	})

	t.Run("nested2 + nested1", func(t *testing.T) {
		nestedProps, merged := schema.MergeRecursivelyNestedProperties(nestedProps2, nestedProps1)

		assert.True(t, merged)
		assert.NotEqual(t, nestedProps1, nestedProps)
		assert.NotEqual(t, nestedProps2, nestedProps)
		test_utils.AssertNestedPropsMatch(t, mergedProps_2_1, nestedProps)
	})
}
