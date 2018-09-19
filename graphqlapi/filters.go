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

// Package graphqlapi provides the graphql endpoint for Weaviate
package graphqlapi

import (
	"github.com/graphql-go/graphql"
)

func genFilterFields(filterOptions map[string]*graphql.InputObject) graphql.InputObjectConfigFieldMap {
	staticFilterElements := genStaticWhereFilterElements()

	filterFields := graphql.InputObjectConfigFieldMap{
		"operands": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(genOperandsObject(filterOptions, staticFilterElements)),
			Description: "Operands in the 'where' filter field, is a list of objects",
		},
	}

	for key, value := range staticFilterElements {
		filterFields[key] = value
	}

	return filterFields
}

// generate these elements once
func genStaticWhereFilterElements() graphql.InputObjectConfigFieldMap {
	staticFilterElements := graphql.InputObjectConfigFieldMap{
		"operator": &graphql.InputObjectFieldConfig{
			Type:        genOperatorObject(),
			Description: "Operator in the 'where' filter field, value is one of the 'WhereOperatorEnum' object",
		},
		"path": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.String),
			Description: "Path of from 'Things' or 'Actions' to the property name through the classes",
		},
		"int": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: "Integer value that the property at the provided path will be compared to by an operator",
		},
		"number": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: "Float value that the property at the provided path will be compared to by an operator",
		},
		"boolean": &graphql.InputObjectFieldConfig{
			Type:        graphql.Boolean,
			Description: "Boolean value that the property at the provided path will be compared to by an operator",
		},
		"string": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: "String value that the property at the provided path will be compared to by an operator",
		},
		"date": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: "Date value that the property at the provided path will be compared to by an operator",
		},
	}

	return staticFilterElements
}

func genOperatorObject() *graphql.Enum {
	enumFilterOptionsMap := graphql.EnumValueConfigMap{
		"And":              &graphql.EnumValueConfig{},
		"Or":               &graphql.EnumValueConfig{},
		"Equal":            &graphql.EnumValueConfig{},
		"Not":              &graphql.EnumValueConfig{},
		"NotEqual":         &graphql.EnumValueConfig{},
		"GreaterThan":      &graphql.EnumValueConfig{},
		"GreaterThanEqual": &graphql.EnumValueConfig{},
		"LessThan":         &graphql.EnumValueConfig{},
		"LessThanEqual":    &graphql.EnumValueConfig{},
	}

	enumFilterOptionsConf := graphql.EnumConfig{
		Name:        "WhereOperatorEnum",
		Values:      enumFilterOptionsMap,
		Description: "Enumeration object for the 'where' filter",
	}

	return graphql.NewEnum(enumFilterOptionsConf)
}

// use a thunk to avoid a cyclical relationship (filters refer to filters refer to .... ad infinitum)
func genOperandsObject(filterOptions map[string]*graphql.InputObject, staticFilterElements graphql.InputObjectConfigFieldMap) *graphql.InputObject {
	outputObject := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "WhereOperandsInpObj",
			Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
				filterFields := genOperandsObjectFields(filterOptions, staticFilterElements)
				return filterFields
			}),
			Description: "Operands in the 'where' filter field, is a list of objects",
		},
	)

	filterOptions["operands"] = outputObject

	return outputObject
}

func genOperandsObjectFields(filterOptions map[string]*graphql.InputObject, staticFilterElements graphql.InputObjectConfigFieldMap) graphql.InputObjectConfigFieldMap {
	outputFieldConfigMap := staticFilterElements

	outputFieldConfigMap["operands"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(filterOptions["operands"]),
		Description: "Operands in the 'where' filter field, is a list of objects",
	}

	return outputFieldConfigMap
}
