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
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated Get Actions part of the schema
func genActionClassFieldsFromSchema(g *GraphQL, getActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {
	actionClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {
		singleActionClassField, singleActionClassObject := genSingleActionClassField(class, getActionsAndThings)
		actionClassFields[class.Class] = singleActionClassField
		// this line assigns the created class to a Hashmap which is used in thunks to handle cyclical relationships (Classes with other Classes as properties)
		(*getActionsAndThings)[class.Class] = singleActionClassObject
	}

	localGetActions := graphql.ObjectConfig{
		Name:        "WeaviateLocalGetActionsObj",
		Fields:      actionClassFields,
		Description: "Type of Actions i.e. Actions classes to Get on the Local Weaviate",
	}

	return graphql.NewObject(localGetActions), nil
}

func genSingleActionClassField(class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object) {
	singleActionClassPropertyFields := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleActionClassPropertyFields, err := genSingleActionClassPropertyFields(class, getActionsAndThings)

			if err != nil {
				panic("Failed to generate single Action Class property fields")
			}

			return singleActionClassPropertyFields
		}),
		Description: class.Description,
	}

	singleActionClassPropertyFieldsObj := graphql.NewObject(singleActionClassPropertyFields)

	singleActionClassPropertyFieldsField := &graphql.Field{
		Type:        graphql.NewList(singleActionClassPropertyFieldsObj),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: "Pagination option, show the first x results",
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: "Pagination option, show the results after the first x results",
				Type:        graphql.Int,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			result, err := dbConnector.GetGraph(p)
			return result, err
		},
	}
	return singleActionClassPropertyFieldsField, singleActionClassPropertyFieldsObj
}

func genSingleActionClassPropertyFields(class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object) (graphql.Fields, error) {
	singleActionClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)

		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			capitalizedPropertyName := strings.Title(property.Name)
			numberOfDataTypes := len(property.AtDataType)
			dataTypeClasses := make([]*graphql.Object, numberOfDataTypes)

			for index, dataType := range property.AtDataType {
				thingOrActionType, ok := (*getActionsAndThings)[dataType]

				if !ok {
					return nil, fmt.Errorf("no such thing/action class '%s'", property.AtDataType[index])
				}

				dataTypeClasses[index] = thingOrActionType
			}

			dataTypeUnionConf := graphql.UnionConfig{
				Name:  fmt.Sprintf("%s%s%s", class.Class, capitalizedPropertyName, "Obj"),
				Types: dataTypeClasses,
				ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
					return nil
				},
				Description: property.Description,
			}

			multipleClassDataTypesUnion := graphql.NewUnion(dataTypeUnionConf)

			singleActionClassPropertyFields[capitalizedPropertyName] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					result, err := dbConnector.GetGraph(p)
					return result, err
				},
			}
		} else {
			convertedDataType, err := handleGetNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}

			singleActionClassPropertyFields[property.Name] = convertedDataType
		}
	}

	singleActionClassPropertyFields["uuid"] = &graphql.Field{
		Description: "UUID of the thing or action given by the local Weaviate instance",
		Type:        graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			result, err := dbConnector.GetGraph(p)
			return result, err
		},
	}

	return singleActionClassPropertyFields, nil
}

// Build the dynamically generated Get Things part of the schema
func genThingClassFieldsFromSchema(g *GraphQL, getActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {
	thingClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ThingSchema.Schema.Classes {
		singleThingClassField, singleThingClassObject := genSingleThingClassField(g, class, getActionsAndThings)
		thingClassFields[class.Class] = singleThingClassField
		// this line assigns the created class to a Hashmap which is used in thunks to handle cyclical relationships (Classes with other Classes as properties)
		(*getActionsAndThings)[class.Class] = singleThingClassObject
	}

	localGetThings := graphql.ObjectConfig{
		Name:        "WeaviateLocalGetThingsObj",
		Fields:      thingClassFields,
		Description: "Type of Things i.e. Things classes to Get on the Local Weaviate",
	}

	return graphql.NewObject(localGetThings), nil
}

func genSingleThingClassField(g *GraphQL, class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object) {

	singleThingClassPropertyFieldsObj := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleThingClassPropertyFields, err := genSingleThingClassPropertyFields(class, getActionsAndThings)
			if err != nil {
				panic(fmt.Errorf("failed to assemble single Thing Class field for Class %s", class.Class))
			}
			return singleThingClassPropertyFields
		}),
		Description: class.Description,
	}

	thingClassPropertyFieldsObject := graphql.NewObject(singleThingClassPropertyFieldsObj)
	thingClassPropertyFieldsField := &graphql.Field{
		Type:        graphql.NewList(thingClassPropertyFieldsObject),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: "Pagination option, show the first x results",
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: "Pagination option, show the results after the first x results",
				Type:        graphql.Int,
			},
		},
		Resolve: g.dbConnector.GraphqlListThings,
	}
	return thingClassPropertyFieldsField, thingClassPropertyFieldsObject
}

func genSingleThingClassPropertyFields(class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object) (graphql.Fields, error) {
	singleThingClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {

		propertyType, err := schema.GetPropertyDataType(class, property.Name)

		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			capitalizedPropertyName := strings.Title(property.Name)
			numberOfDataTypes := len(property.AtDataType)
			dataTypeClasses := make([]*graphql.Object, numberOfDataTypes)

			for index, dataType := range property.AtDataType {
				thingOrActionType, ok := (*getActionsAndThings)[dataType]

				if !ok {
					return nil, fmt.Errorf("no such thing/action class '%s'", property.AtDataType[index])
				}

				dataTypeClasses[index] = thingOrActionType
			}

			dataTypeUnionConf := graphql.UnionConfig{
				Name:  fmt.Sprintf("%s%s%s", class.Class, capitalizedPropertyName, "Obj"),
				Types: dataTypeClasses,
				ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
					return nil
				},
				Description: property.Description,
			}

			multipleClassDataTypesUnion := graphql.NewUnion(dataTypeUnionConf)

			singleThingClassPropertyFields[capitalizedPropertyName] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					result, err := dbConnector.GetGraph(p)
					return result, err
				},
			}
		} else {
			convertedDataType, err := handleGetNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}

			singleThingClassPropertyFields[property.Name] = convertedDataType
		}
	}

	singleThingClassPropertyFields["uuid"] = &graphql.Field{
		Description: "UUID of the thing or action given by the local Weaviate instance",
		Type:        graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			result, err := dbConnector.GetGraph(p)
			return result, err
		},
	}

	return singleThingClassPropertyFields, nil
}

func resolveAnythingOnAMap(p graphql.ResolveParams) (interface{}, error) {
	sourceAsMap, ok := p.Source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf(
			"the only supported type to resolve fields on is a map[string]interface{} for now, "+
				"but we got %t", p.Source)
	}

	return sourceAsMap[p.Info.FieldName], nil
}

func handleGetNonObjectDataTypes(dataType schema.DataType, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {
	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
			Resolve:     resolveAnythingOnAMap,
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
			Resolve:     resolveAnythingOnAMap,
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Float,
			Resolve:     resolveAnythingOnAMap,
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Boolean,
			Resolve:     resolveAnythingOnAMap,
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String, // String since no graphql date datatype exists
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("date is not supported")
			},
		}, nil

	default:
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}
