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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/test_utils"
)

func Test_DedupProperties(t *testing.T) {
	vFalse := false
	vTrue := true

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

	props1 := []*models.Property{
		{
			Name:            "text",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			Tokenization:    models.PropertyTokenizationWord,
		},
		{
			Name:            "number",
			DataType:        schema.DataTypeNumber.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:             "both_diff_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
		{
			Name:             "both_diff_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
		{
			Name:             "both_same_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
		{
			Name:             "both_same_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
		{
			Name:             "one_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
		{
			Name:             "one_dup_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
		{
			Name:             "one_dup_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
	}
	props2 := []*models.Property{
		{
			Name:            "text",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    models.PropertyTokenizationWord,
		},
		{
			Name:            "int",
			DataType:        schema.DataTypeInt.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:            "bool",
			DataType:        schema.DataTypeBoolean.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
		},
		{
			Name:             "both_diff_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
		{
			Name:             "both_diff_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
		{
			Name:             "both_same_json1",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
		{
			Name:             "both_same_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
		{
			Name:             "one_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
		{
			Name:             "one_dup_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps2,
		},
		{
			Name:             "one_dup_json2",
			DataType:         schema.DataTypeObject.PropString(),
			IndexFilterable:  &vFalse,
			IndexSearchable:  &vFalse,
			Tokenization:     "",
			NestedProperties: nestedProps1,
		},
	}

	t.Run("props1 + props2", func(t *testing.T) {
		props := schema.DedupProperties(props1, props2)

		test_utils.AssertPropsMatch(t, props, []*models.Property{
			{
				Name:            "int",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "bool",
				DataType:        schema.DataTypeBoolean.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "both_diff_json1",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_number",
						DataType:        schema.DataTypeNumber.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
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
						},
					},
				},
			},
			{
				Name:            "both_diff_json2",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_int",
						DataType:        schema.DataTypeInt.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
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
						},
					},
				},
			},
			{
				Name:            "one_json2",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
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
				},
			},
			{
				Name:            "one_dup_json2",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
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
				},
			},
		})
	})

	t.Run("props2 + props1", func(t *testing.T) {
		props := schema.DedupProperties(props2, props1)

		test_utils.AssertPropsMatch(t, props, []*models.Property{
			{
				Name:            "number",
				DataType:        schema.DataTypeNumber.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
				Tokenization:    "",
			},
			{
				Name:            "both_diff_json1",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_int",
						DataType:        schema.DataTypeInt.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
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
						},
					},
				},
			},
			{
				Name:            "both_diff_json2",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "nested_number",
						DataType:        schema.DataTypeNumber.PropString(),
						IndexFilterable: &vTrue,
						IndexSearchable: &vFalse,
						Tokenization:    "",
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
						},
					},
				},
			},
			{
				Name:            "one_json1",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
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
				},
			},
			{
				Name:            "one_dup_json1",
				DataType:        schema.DataTypeObject.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
				Tokenization:    "",
				NestedProperties: []*models.NestedProperty{
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
				},
			},
		})
	})
}
