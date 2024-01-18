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

// Package common_filters provides the filters for the graphql endpoint for Weaviate
package common_filters

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

// The filters common to Local->Get and Local->Meta queries.
func BuildNew(path string) graphql.InputObjectConfigFieldMap {
	commonFilters := graphql.InputObjectConfigFieldMap{
		"operator": &graphql.InputObjectFieldConfig{
			Type: graphql.NewEnum(graphql.EnumConfig{
				Name: fmt.Sprintf("%sWhereOperatorEnum", path),
				Values: graphql.EnumValueConfigMap{
					"And":              &graphql.EnumValueConfig{},
					"Like":             &graphql.EnumValueConfig{},
					"Or":               &graphql.EnumValueConfig{},
					"Equal":            &graphql.EnumValueConfig{},
					"Not":              &graphql.EnumValueConfig{},
					"NotEqual":         &graphql.EnumValueConfig{},
					"GreaterThan":      &graphql.EnumValueConfig{},
					"GreaterThanEqual": &graphql.EnumValueConfig{},
					"LessThan":         &graphql.EnumValueConfig{},
					"LessThanEqual":    &graphql.EnumValueConfig{},
					"WithinGeoRange":   &graphql.EnumValueConfig{},
					"IsNull":           &graphql.EnumValueConfig{},
					"ContainsAny":      &graphql.EnumValueConfig{},
					"ContainsAll":      &graphql.EnumValueConfig{},
				},
				Description: descriptions.WhereOperatorEnum,
			}),
			Description: descriptions.WhereOperator,
		},
		"path": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.String),
			Description: descriptions.WherePath,
		},
		"valueInt": &graphql.InputObjectFieldConfig{
			Type:        newValueIntType(path),
			Description: descriptions.WhereValueInt,
		},
		"valueNumber": &graphql.InputObjectFieldConfig{
			Type:        newValueNumberType(path),
			Description: descriptions.WhereValueNumber,
		},
		"valueBoolean": &graphql.InputObjectFieldConfig{
			Type:        newValueBooleanType(path),
			Description: descriptions.WhereValueBoolean,
		},
		"valueString": &graphql.InputObjectFieldConfig{
			Type:        newValueStringType(path),
			Description: descriptions.WhereValueString,
		},
		"valueText": &graphql.InputObjectFieldConfig{
			Type:        newValueTextType(path),
			Description: descriptions.WhereValueText,
		},
		"valueDate": &graphql.InputObjectFieldConfig{
			Type:        newValueDateType(path),
			Description: descriptions.WhereValueString,
		},
		"valueGeoRange": &graphql.InputObjectFieldConfig{
			Type:        newGeoRangeInputObject(path),
			Description: descriptions.WhereValueRange,
		},
	}

	// Recurse into the same time.
	commonFilters["operands"] = &graphql.InputObjectFieldConfig{
		Description: descriptions.WhereOperands,
		Type: graphql.NewList(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sWhereOperandsInpObj", path),
				Description: descriptions.WhereOperandsInpObj,
				Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
					return commonFilters
				}),
			},
		)),
	}

	return commonFilters
}

func newGeoRangeInputObject(path string) *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: fmt.Sprintf("%sWhereGeoRangeInpObj", path),
		Fields: graphql.InputObjectConfigFieldMap{
			"geoCoordinates": &graphql.InputObjectFieldConfig{
				Type:        graphql.NewNonNull(newGeoRangeGeoCoordinatesInputObject(path)),
				Description: descriptions.WhereValueRangeGeoCoordinates,
			},
			"distance": &graphql.InputObjectFieldConfig{
				Type:        graphql.NewNonNull(newGeoRangeDistanceInputObject(path)),
				Description: descriptions.WhereValueRangeDistance,
			},
		},
	})
}

func newGeoRangeGeoCoordinatesInputObject(path string) *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: fmt.Sprintf("%sWhereGeoRangeGeoCoordinatesInpObj", path),
		Fields: graphql.InputObjectConfigFieldMap{
			"latitude": &graphql.InputObjectFieldConfig{
				Type:        graphql.NewNonNull(graphql.Float),
				Description: descriptions.WhereValueRangeGeoCoordinatesLatitude,
			},
			"longitude": &graphql.InputObjectFieldConfig{
				Type:        graphql.NewNonNull(graphql.Float),
				Description: descriptions.WhereValueRangeGeoCoordinatesLongitude,
			},
		},
	})
}

func newGeoRangeDistanceInputObject(path string) *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: fmt.Sprintf("%sWhereGeoRangeDistanceInpObj", path),
		Fields: graphql.InputObjectConfigFieldMap{
			"max": &graphql.InputObjectFieldConfig{
				Type:        graphql.NewNonNull(graphql.Float),
				Description: descriptions.WhereValueRangeDistanceMax,
			},
		},
	})
}
