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

package fixtures

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var (
	AllPropertiesClassName = "AllProperties"
	AllPropertiesID1       = "00000000-0000-0000-0000-000000000001"
	vTrue                  = true
	vFalse                 = false
)

var AllPropertiesClass = &models.Class{
	Class: AllPropertiesClassName,
	Properties: []*models.Property{
		{
			Name:            "objectProperty",
			DataType:        schema.DataTypeObject.PropString(),
			IndexFilterable: &vTrue,
			IndexSearchable: &vFalse,
			Tokenization:    "",
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "text",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "texts",
					DataType: schema.DataTypeTextArray.PropString(),
				},
				{
					Name:     "number",
					DataType: schema.DataTypeNumber.PropString(),
				},
				{
					Name:     "numbers",
					DataType: schema.DataTypeNumberArray.PropString(),
				},
				{
					Name:     "int",
					DataType: schema.DataTypeInt.PropString(),
				},
				{
					Name:     "ints",
					DataType: schema.DataTypeIntArray.PropString(),
				},
				{
					Name:     "date",
					DataType: schema.DataTypeDate.PropString(),
				},
				{
					Name:     "dates",
					DataType: schema.DataTypeDateArray.PropString(),
				},
				{
					Name:     "bool",
					DataType: schema.DataTypeBoolean.PropString(),
				},
				{
					Name:     "bools",
					DataType: schema.DataTypeBooleanArray.PropString(),
				},
				{
					Name:     "uuid",
					DataType: schema.DataTypeUUID.PropString(),
				},
				{
					Name:     "uuids",
					DataType: schema.DataTypeUUIDArray.PropString(),
				},
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
					Tokenization:    models.PropertyTokenizationWord,
				},
				{
					Name:            "nested_objects",
					DataType:        schema.DataTypeObject.PropString(),
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
				{
					Name:            "nested_array_objects",
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
	},
}

var AllPropertiesProperties = map[string]interface{}{
	"text":          "red",
	"texts":         []string{"red", "blue"},
	"number":        float64(1.1),
	"numbers":       []float64{1.1, 2.2},
	"int":           int64(1),
	"ints":          []int64{1, 2},
	"uuid":          AllPropertiesID1,
	"uuids":         []string{AllPropertiesID1},
	"date":          "2009-11-01T23:00:00Z",
	"dates":         []string{"2009-11-01T23:00:00Z"},
	"bool":          true,
	"bools":         []bool{true, false},
	"nested_int":    int64(11),
	"nested_number": float64(11.11),
	"nested_text":   "nested text",
	"nested_objects": map[string]interface{}{
		"nested_bool_lvl2":    true,
		"nested_numbers_lvl2": []float64{11.1, 22.1},
	},
	"nested_array_objects": []interface{}{
		map[string]interface{}{
			"nested_bool_lvl2":    true,
			"nested_numbers_lvl2": []float64{111.1, 222.1},
		},
		map[string]interface{}{
			"nested_bool_lvl2":    false,
			"nested_numbers_lvl2": []float64{112.1, 222.1},
		},
	},
}
