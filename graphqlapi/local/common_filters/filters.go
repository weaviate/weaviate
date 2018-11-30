package common_filters

import (
	"github.com/graphql-go/graphql"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
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
				Description: descriptions.WhereOperatorEnumDesc,
			}),
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
				Name:        "WhereOperandsInpObj",
				Description: descriptions.WhereOperandsInpObjDesc,
				Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
					return commonFilters
				}),
			},
		)),
	}

	return commonFilters
}
