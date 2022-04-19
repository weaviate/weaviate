//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func getMyFavoriteClassSchemaForTests() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "MyFavoriteClass",
					Properties: []*models.Property{
						{
							Name:     "stringProp",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "textProp",
							DataType: []string{string(schema.DataTypeText)},
						},
						{
							Name:     "stringPropArray",
							DataType: []string{string(schema.DataTypeStringArray)},
						},
						{
							Name:     "textPropArray",
							DataType: []string{string(schema.DataTypeTextArray)},
						},
						{
							Name:     "intProp",
							DataType: []string{string(schema.DataTypeInt)},
						},
						{
							Name:     "numberProp",
							DataType: []string{string(schema.DataTypeNumber)},
						},
						{
							Name:     "intPropArray",
							DataType: []string{string(schema.DataTypeIntArray)},
						},
						{
							Name:     "numberPropArray",
							DataType: []string{string(schema.DataTypeNumberArray)},
						},
						{
							Name:     "boolProp",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
						{
							Name:     "boolPropArray",
							DataType: []string{string(schema.DataTypeBooleanArray)},
						},
						{
							Name:     "dateProp",
							DataType: []string{string(schema.DataTypeDate)},
						},
						{
							Name:     "datePropArray",
							DataType: []string{string(schema.DataTypeDateArray)},
						},
						{
							Name:     "phoneProp",
							DataType: []string{string(schema.DataTypePhoneNumber)},
						},
						{
							Name:     "geoProp",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
						{
							Name:     "emptyStringProp",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "crefProp",
							DataType: []string{string(schema.DataTypeCRef)},
						},
					},
				},
			},
		},
	}
}
