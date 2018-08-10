package graphqlapi

import (
	"fmt"
	"github.com/graphql-go/graphql"
)

func genFilterFields(filterOptions map[string]*graphql.InputObject) *graphql.InputObject {
	inputType := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "WeaviateLocalConvertedFetchFilterInpObj",
			Fields: graphql.InputObjectConfigFieldMap{
				"AND": &graphql.InputObjectFieldConfig{
					Type: makeObjectAndFillMap(filterOptions),
				},
			},
			Description: "Filter options for the converted fetch search, to convert the data to the filter input",
		},
	)
	return inputType
}

func makeObjectAndFillMap(filterOptions map[string]*graphql.InputObject) *graphql.InputObject { // hier geef je de map mee als arg en vul je hem
	outputObject := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: "FetchFilterANDInpObj",
			Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
				filterFields := genFilterObjectFields(filterOptions)
				return filterFields
			}),
		},
	)
	filterOptions[outputObject.Name()] = outputObject
	return outputObject
}

func genFilterObjectFields(filterOptions map[string]*graphql.InputObject) graphql.InputObjectConfigFieldMap {
	outputFieldConfigMap := graphql.InputObjectConfigFieldMap{}

	for optionName, optionObject := range filterOptions {
		outputFieldConfigMap[optionName] = &graphql.InputObjectFieldConfig{
			Type:        optionObject,
			Description: "bladiebla",
		}
	}
	return outputFieldConfigMap
}
