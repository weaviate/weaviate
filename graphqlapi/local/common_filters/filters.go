package common_filters

import (
	"github.com/graphql-go/graphql"
	"sync"
)

var sharedCommonFilters graphql.InputObjectConfigFieldMap
var initFilter sync.Once

// The filters common to Local->Get and Local->GetMeta queries.
func Get() graphql.InputObjectConfigFieldMap {
	initFilter.Do(func() {
    sharedCommonFilters = BuildNew()
  })

  return sharedCommonFilters
}

func BuildNew() graphql.InputObjectConfigFieldMap {
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
