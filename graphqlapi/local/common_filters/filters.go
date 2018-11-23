package common_filters

import (
	"github.com/graphql-go/graphql"
)

// The filters common to Local->Get and Local->GetMeta queries.
var CommonFilters graphql.InputObjectConfigFieldMap = graphql.InputObjectConfigFieldMap{
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

// We need to bootstrap the operands back to the commonfilters
func init() {
	var operands *graphql.InputObject

	operands = graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "WhereOperandsInpObj",
			Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
				CommonFilters["operands"] = &graphql.InputObjectFieldConfig{
					Type:        graphql.NewList(operands),
					Description: "Operands in the 'where' filter field, is a list of objects",
				}
				return CommonFilters
			}),
			Description: "Operands in the 'where' filter field, is a list of objects",
		},
	)

	CommonFilters["operands"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(operands),
		Description: "Operands in the 'where' filter field, is a list of objects",
	}
}
