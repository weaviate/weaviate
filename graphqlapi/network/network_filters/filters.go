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

// Package network_filters provides the filters for the network graphql endpoint for Weaviate
package network_filters

import (
	"github.com/graphql-go/graphql"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"sync"
)

var sharedNetworkGetAndGetMetaWhereFilters graphql.InputObjectConfigFieldMap
var sharedNetworkOperatorEnum *graphql.Enum
var initFilter sync.Once
var initEnum sync.Once

// The filters common to Network->Get and Network->GetMeta queries.
func GetNetworkGetAndGetMetaWhereFilters() graphql.InputObjectConfigFieldMap {
	initFilter.Do(func() {
    sharedNetworkGetAndGetMetaWhereFilters = BuildNewNetworkGetAndGetMetaFilters()
  })

  return sharedNetworkGetAndGetMetaWhereFilters
}

func BuildNewNetworkGetAndGetMetaFilters() graphql.InputObjectConfigFieldMap {
    commonFilters := graphql.InputObjectConfigFieldMap{
		"operator": &graphql.InputObjectFieldConfig{
			Type: GetNetworkOperatorEnum(),
			Description: descriptions.WhereOperatorDesc,
		},
		"path": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.String),
			Description: descriptions.WherePathDesc,
		},
		"valueInt": &graphql.InputObjectFieldConfig{
			Type:        graphql.Int,
			Description: descriptions.WhereValueIntDesc,
		},
		"valueNumber": &graphql.InputObjectFieldConfig{
			Type:        graphql.Float,
			Description: descriptions.WhereValueNumberDesc,
		},
		"valueBoolean": &graphql.InputObjectFieldConfig{
			Type:        graphql.Boolean,
			Description: descriptions.WhereValueBooleanDesc,
		},
		"valueString": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereValueStringDesc,
		},
		"valueDate": &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: descriptions.WhereValueStringDesc,
		},
	}

	// Recurse into the same time.
	commonFilters["operands"] = &graphql.InputObjectFieldConfig{
		Description: descriptions.WhereOperandsDesc,
		Type: graphql.NewList(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        "NetworkWhereOperandsInpObj",
				Description: descriptions.WhereOperandsInpObjDesc,
				Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
					return commonFilters
				}),
			},
		)),
	}

	return commonFilters
}

// The operator enum object common to Network queries.
func GetNetworkOperatorEnum() *graphql.Enum {
	initEnum.Do(func() {
    sharedNetworkOperatorEnum = BuildNewNetworkOperatorEnum()
  })

  return sharedNetworkOperatorEnum
}

func BuildNewNetworkOperatorEnum() *graphql.Enum{
	operatorEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "NetworkWhereOperatorEnum",
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
		Description: descriptions.WhereOperatorEnumDesc,
	})
		
	return operatorEnum
}		


