package graphqlapi

import (
	"github.com/graphql-go/graphql"
)

type filterDescriptorsContainer struct {
	filterNames             map[string]string
	filterDescriptions      map[string]string
	filterFetchNames        map[string]string
	filterFetchDescriptions map[string]string
}

// TODO: store string values in constants
// TODO: think of a better name for this struct
func initializeFilterDescriptor() *filterDescriptorsContainer {

	filterDescriptor := filterDescriptorsContainer{

		filterNames: map[string]string{
			"AND": "FetchFilterANDInpObj",
			"OR":  "FetchFilterORInpObj",
			"EQ":  "FetchFilterEQInpObj",
			"NEQ": "FetchFilterNEQInpObj",
			"IE":  "FetchFilterIEInpObj",
		},

		filterDescriptions: map[string]string{
			"AND": "Filter options for the converted fetch search, to convert the data to the filter input",
			"OR":  "Filter options for the converted fetch search, to convert the data to the filter input",
			"EQ":  "filter where the path end should be equal to the value",
			"NEQ": "filter where the path end should NOT be equal to the value",
			"IE":  "filter where the path end should be inequal to the value",
		},

		filterFetchNames: map[string]string{
			"AND": "FetchFilterFieldANDInpObj",
			"OR":  "FetchFilterFieldORInpObj",
		},

		filterFetchDescriptions: map[string]string{
			"AND": "Filter options for the converted fetch search, to convert the data to the filter input",
			"OR":  "Filter options for the converted fetch search, to convert the data to the filter input",
		},
	}
	return &filterDescriptor
}

// generate the AND/OR/EQ/NEQ/IE filter fields for the ConvertedFetch and MetaFetch fields
func genFilterFields(filterOptions map[string]*graphql.InputObject, filterFetchOptions map[string]*graphql.InputObject) graphql.InputObjectConfigFieldMap {

	filterDescriptor := initializeFilterDescriptor()

	genSingleFilterFetchField(filterFetchOptions, "AND", filterDescriptor)
	genSingleFilterFetchField(filterFetchOptions, "OR", filterDescriptor)

	inputType := graphql.InputObjectConfigFieldMap{
		"AND": &graphql.InputObjectFieldConfig{
			Type:        genSingleFilterField(filterOptions, filterFetchOptions, "AND", filterDescriptor),
			Description: filterDescriptor.filterDescriptions["AND"],
		},
		"OR": &graphql.InputObjectFieldConfig{
			Type:        genSingleFilterField(filterOptions, filterFetchOptions, "OR", filterDescriptor),
			Description: filterDescriptor.filterDescriptions["OR"],
		},
		"EQ": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(genSingleFilterField(filterOptions, filterFetchOptions, "EQ", filterDescriptor)),
			Description: filterDescriptor.filterDescriptions["EQ"],
		},
		"NEQ": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(genSingleFilterField(filterOptions, filterFetchOptions, "NEQ", filterDescriptor)),
			Description: filterDescriptor.filterDescriptions["NEQ"],
		},
		"IE": &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(genSingleFilterField(filterOptions, filterFetchOptions, "IE", filterDescriptor)),
			Description: filterDescriptor.filterDescriptions["IE"],
		},
	}
	return inputType
}

// use a thunk to avoid a cyclical relationship (filters refer to filters refer to .... ad infinitum)
func genSingleFilterField(filterOptions map[string]*graphql.InputObject, filterFetchOptions map[string]*graphql.InputObject,
	filterOptionName string, filterDescriptor *filterDescriptorsContainer) *graphql.InputObject {

	outputObject := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: filterDescriptor.filterNames[filterOptionName],
			Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
				filterFields := genFilterObjectFields(filterOptions, filterFetchOptions, filterOptionName, filterDescriptor)
				return filterFields
			}),
		},
	)
	filterOptions[filterOptionName] = outputObject
	return outputObject
}

/*
AND_OR = [AND, OR, EQ, NEQ, IE]
OTHER = [AND2, OR2, path, value]

AND and OR filters only have AND_OR options (cyclical referral)
EQ, NEQ and IE (and AND2 and OR2) filters only have OTHER options (cyclical referral)

This function determines what type the current filter is and generates the according subset of filters
*/
func genFilterObjectFields(filterOptions map[string]*graphql.InputObject, filterFetchOptions map[string]*graphql.InputObject,
	filterOptionName string, filterDescriptor *filterDescriptorsContainer) graphql.InputObjectConfigFieldMap {

	outputFieldConfigMap := graphql.InputObjectConfigFieldMap{}

	if filterOptionName == "AND" || filterOptionName == "OR" {
		for optionName, optionObject := range filterOptions {
			if optionName == "AND" || optionName == "OR" {
				outputFieldConfigMap[optionName] = &graphql.InputObjectFieldConfig{
					Type:        optionObject,
					Description: filterDescriptor.filterDescriptions[filterOptionName],
				}
			} else {
				outputFieldConfigMap[optionName] = &graphql.InputObjectFieldConfig{
					Type:        graphql.NewList(optionObject),
					Description: filterDescriptor.filterDescriptions[filterOptionName],
				}
			}

		}
	} else {
		for optionName, optionObject := range filterFetchOptions {
			outputFieldConfigMap[optionName] = &graphql.InputObjectFieldConfig{
				Type:        graphql.NewList(optionObject),
				Description: filterDescriptor.filterDescriptions[filterOptionName],
			}
		}
		outputFieldConfigMap["path"] = &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.String),
			Description: "path from the root Thing or Action until class property",
		}
		outputFieldConfigMap["value"] = &graphql.InputObjectFieldConfig{
			Type:        graphql.String,
			Description: "the value to class property should be filtered at",
		}
	}
	return outputFieldConfigMap
}

// gen the filter field subset available to the EQ, NEQ and IE filters
func genSingleFilterFetchField(filterFetchOptions map[string]*graphql.InputObject, filterOptionName string,
	filterDescriptor *filterDescriptorsContainer) *graphql.InputObject {

	outputObject := graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: filterDescriptor.filterFetchNames[filterOptionName],
			Fields: (graphql.InputObjectConfigFieldMapThunk)(func() graphql.InputObjectConfigFieldMap {
				filterFields := genFilterFetchObjectFields(filterFetchOptions, filterOptionName, filterDescriptor)
				return filterFields
			}),
		},
	)
	filterFetchOptions[filterOptionName] = outputObject
	return outputObject
}

func genFilterFetchObjectFields(filterFetchOptions map[string]*graphql.InputObject, filterOptionName string,
	filterDescriptor *filterDescriptorsContainer) graphql.InputObjectConfigFieldMap {

	outputFieldConfigMap := graphql.InputObjectConfigFieldMap{}

	for optionName, optionObject := range filterFetchOptions {
		outputFieldConfigMap[optionName] = &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(optionObject),
			Description: filterDescriptor.filterFetchDescriptions[filterOptionName],
		}
	}
	outputFieldConfigMap["path"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.NewList(graphql.String),
		Description: "path from the root Thing or Action until class property",
	}
	outputFieldConfigMap["value"] = &graphql.InputObjectFieldConfig{
		Type:        graphql.String,
		Description: "the value to class property should be filtered at",
	}
	return outputFieldConfigMap
}
