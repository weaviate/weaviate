/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package common_filters provides the filters for the graphql endpoint for Weaviate

package common_filters

import (
	"github.com/graphql-go/graphql"
)

// The filters common to Local->Get and Local->GetMeta queries.

func GetGetAndGetMetaWhereFilters() graphql.InputObjectConfigFieldMap {
	commonFilters := graphql.InputObjectConfigFieldMap{
		"operator": &graphql.InputObjectFieldConfig{
			Type: graphql.NewEnum(graphql.EnumConfig{
				Name: "WhereOperatorEnum",
				Values: graphql.EnumValueConfigMap{
					"And":              &graphql.EnumValueConfig{},
					"Or":               &graphql.EnumValueConfig{},
					"Equal":            &graphql.EnumValueConfig{},
					"Not":              &graphql.EnumValueConfig{},
					"NotEqual":         &graphql.EnumValueConfig{},
					"GreaterThan":      &graphql.EnumValueConfig{},
					"GreaterThanEqual": &graphql.EnumValueConfig{},
					"LessThan":         &graphql.EnumValueConfig{},
					"LessThanEqual":    &graphql.EnumValueConfig{},
				},
				Description: "Enumeration object for the 'where' filter",
			}),
			Description: "Operator in the 'where' filter field, value is one of the 'WhereOperatorEnum' object",
		},
		"path": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.String),
			Description: "Path of from 'Things' or 'Actions' to the property name through the classes",
		},
		"valueInt": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: "Integer value that the property at the provided path will be compared to by an operator",
		},
		"valueNumber": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: "Number value that the property at the provided path will be compared to by an operator",
		},
		"valueBoolean": &graphql.InputObjectFieldConfig{
			Type:        graphql.Boolean,
			Description: "Boolean value that the property at the provided path will be compared to by an operator",
		},
		"valueString": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: "String value that the property at the provided path will be compared to by an operator",
		},
		"valueDate": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: "String value that the property at the provided path will be compared to by an operator",
		},
	}

	// Recurse into the same time.
	commonFilters["operands"] = &graphql.InputObjectFieldConfig{
		Description: "Operands in the 'where' filter field, is a list of objects",
		Type: graphql.NewList(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "WhereOperandsInpObj",
				Description: "Operands in the 'where' filter field, is a list of objects",
				Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
					return commonFilters
				}),
			},
		)),
	}

	return commonFilters
}
//package common_filters
//
//import (
//	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
//	"github.com/graphql-go/graphql"
//	"sync"
//)
//
//var sharedGetAndGetMetaWhereFilters graphql.InputObjectConfigFieldMap
//var sharedOperatorEnum *graphql.Enum
//var initFilter sync.Once
//var initEnum sync.Once
//
//// The filters common to Network->Get and Network->GetMeta queries.
//func GetGetAndGetMetaWhereFilters() graphql.InputObjectConfigFieldMap {
//	initFilter.Do(func() {
//		sharedGetAndGetMetaWhereFilters = BuildNewGetAndGetMetaFilters()
//	})
//
//	return sharedGetAndGetMetaWhereFilters
//}
//
//func BuildNewGetAndGetMetaFilters() graphql.InputObjectConfigFieldMap {
//	commonFilters := graphql.InputObjectConfigFieldMap{
//		"operator": &graphql.InputObjectFieldConfig{
//			Type:        GetOperatorEnum(),
//			Description: descriptions.WhereOperatorDesc,
//		},
//		"path": &graphql.InputObjectFieldConfig{
//			Type:        graphql.NewList(graphql.String),
//			Description: descriptions.WherePathDesc,
//		},
//		"valueInt": &graphql.InputObjectFieldConfig{
//			Type:        graphql.Int,
//			Description: descriptions.WhereValueIntDesc,
//		},
//		"valueNumber": &graphql.InputObjectFieldConfig{
//			Type:        graphql.Float,
//			Description: descriptions.WhereValueNumberDesc,
//		},
//		"valueBoolean": &graphql.InputObjectFieldConfig{
//			Type:        graphql.Boolean,
//			Description: descriptions.WhereValueBooleanDesc,
//		},
//		"valueString": &graphql.InputObjectFieldConfig{
//			Type:        graphql.String,
//			Description: descriptions.WhereValueStringDesc,
//		},
//		"valueText": &graphql.InputObjectFieldConfig{
//			Type:        graphql.String,
//			Description: descriptions.WhereValueTextDesc,
//		},
//		"valueDate": &graphql.InputObjectFieldConfig{
//			Type:        graphql.String,
//			Description: descriptions.WhereValueStringDesc,
//		},
//	}
//
//	// Recurse into the same time.
//	commonFilters["operands"] = &graphql.InputObjectFieldConfig{
//		Description: descriptions.WhereOperandsDesc,
//		Type: graphql.NewList(graphql.NewInputObject(
//			graphql.InputObjectConfig{
//				Name:        "WhereOperandsInpObj",
//				Description: descriptions.WhereOperandsInpObjDesc,
//				Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
//					return commonFilters
//				}),
//			},
//		)),
//	}
//
//	return commonFilters
//}
//
//// The common operator enum object.
//func GetOperatorEnum() *graphql.Enum {
//	initEnum.Do(func() {
//		sharedOperatorEnum = BuildNewOperatorEnum()
//	})
//
//	return sharedOperatorEnum
//}
//
//func BuildNewOperatorEnum() *graphql.Enum {
//	operatorEnum := graphql.NewEnum(graphql.EnumConfig{
//		Name: "WhereOperatorEnum",
//		Values: graphql.EnumValueConfigMap{
//			"And":              &graphql.EnumValueConfig{},
//			"Or":               &graphql.EnumValueConfig{},
//			"Equal":            &graphql.EnumValueConfig{},
//			"Not":              &graphql.EnumValueConfig{},
//			"NotEqual":         &graphql.EnumValueConfig{},
//			"GreaterThan":      &graphql.EnumValueConfig{},
//			"GreaterThanEqual": &graphql.EnumValueConfig{},
//			"LessThan":         &graphql.EnumValueConfig{},
//			"LessThanEqual":    &graphql.EnumValueConfig{},
//		},
//		Description: descriptions.WhereOperatorEnumDesc,
//	})
//
//	return operatorEnum
//}
