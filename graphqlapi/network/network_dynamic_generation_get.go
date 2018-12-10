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

// Package network provides the network graphql endpoint for Weaviate
package network

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	network_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated Get Actions part of the schema
func genNetworkActionClassFieldsFromSchema(dbSchema *schema.Schema, networkGetActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Object, error) {
	actionClassFields := graphql.Fields{}

	for _, class := range dbSchema.Actions.Classes {
		singleActionClassField, singleActionClassObject := genSingleNetworkActionClassField(class, networkGetActionsAndThings, weaviate)
		actionClassFields[class.Class] = singleActionClassField
		// this line assigns the created class to a Hashmap which is used in thunks to handle cyclical relationships (Classes with other Classes as properties)
		(*networkGetActionsAndThings)[class.Class] = singleActionClassObject
	}

	networkGetActions := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGet", weaviate, "ActionsObj"),
		Fields:      actionClassFields,
		Description: descriptions.NetworkGetWeaviateActionsObjDesc,
	}

	return graphql.NewObject(networkGetActions), nil
}

func genSingleNetworkActionClassField(class *models.SemanticSchemaClass, networkGetActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Field, *graphql.Object) {
	singleNetworkActionClassPropertyFields := graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%s", weaviate, class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleActionClassPropertyFields, err := genSingleNetworkActionClassPropertyFields(class, networkGetActionsAndThings, weaviate)

			if err != nil {
				panic("Failed to generate single Network Action Class property fields")
			}

			return singleActionClassPropertyFields
		}),
		Description: class.Description,
	}

	singleNetworkActionClassPropertyFieldsObj := graphql.NewObject(singleNetworkActionClassPropertyFields)

	singleNetworkActionClassPropertyFieldsField := &graphql.Field{
		Type:        graphql.NewList(singleNetworkActionClassPropertyFieldsObj),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"first": &graphql.ArgumentConfig{
				Description: descriptions.FirstDesc,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.AfterDesc,
				Type:        graphql.Int,
			},
		},
		Resolve: network_get.ResolveAction,
	}
	return singleNetworkActionClassPropertyFieldsField, singleNetworkActionClassPropertyFieldsObj
}

func genSingleNetworkActionClassPropertyFields(class *models.SemanticSchemaClass, networkGetActionsAndThings *map[string]*graphql.Object, weaviate string) (graphql.Fields, error) {
	singleNetworkActionClassPropertyFields := graphql.Fields{}

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
				thingOrActionType, ok := (*networkGetActionsAndThings)[dataType]

				if !ok {
					return nil, fmt.Errorf("no such thing/action class '%s'", property.AtDataType[index])
				}

				dataTypeClasses[index] = thingOrActionType
			}

			dataTypeUnionConf := graphql.UnionConfig{
				Name:  fmt.Sprintf("%s%s%s%s", weaviate, class.Class, capitalizedPropertyName, "Obj"),
				Types: dataTypeClasses,
				ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
					return nil
				},
				Description: property.Description,
			}

			multipleClassDataTypesUnion := graphql.NewUnion(dataTypeUnionConf)

			singleNetworkActionClassPropertyFields[capitalizedPropertyName] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("not supported")
				},
			}
		} else {
			convertedDataType, err := handleNetworkGetNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}

			singleNetworkActionClassPropertyFields[property.Name] = convertedDataType
		}
	}

	singleNetworkActionClassPropertyFields["uuid"] = &graphql.Field{
		Description: descriptions.NetworkGetClassUUIDDesc,
		Type:        graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return singleNetworkActionClassPropertyFields, nil
}

// Build the dynamically generated Get Things part of the schema
func genNetworkThingClassFieldsFromSchema(dbSchema *schema.Schema, getNetworkActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Object, error) {
	thingClassFields := graphql.Fields{}

	for _, class := range dbSchema.Things.Classes {
		singleThingClassField, singleThingClassObject := genSingleNetworkThingClassField(class, getNetworkActionsAndThings, weaviate)
		thingClassFields[class.Class] = singleThingClassField
		// this line assigns the created class to a Hashmap which is used in thunks to handle cyclical relationships (Classes with other Classes as properties)
		(*getNetworkActionsAndThings)[class.Class] = singleThingClassObject
	}

	networkGetThings := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGet", weaviate, "ThingsObj"),
		Fields:      thingClassFields,
		Description: descriptions.NetworkGetWeaviateThingsObjDesc,
	}

	return graphql.NewObject(networkGetThings), nil
}

func genSingleNetworkThingClassField(class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Field, *graphql.Object) {
	singleThingClassPropertyFieldsObj := graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%s", weaviate, class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleThingClassPropertyFields, err := genSingleNetworkThingClassPropertyFields(class, getActionsAndThings, weaviate)
			if err != nil {
				panic(fmt.Errorf("failed to assemble single Network Thing Class field for Class %s", class.Class))
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
				Description: descriptions.FirstDesc,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.AfterDesc,
				Type:        graphql.Int,
			},
		},
		Resolve: network_get.ResolveThing,
	}
	return thingClassPropertyFieldsField, thingClassPropertyFieldsObject
}

func genSingleNetworkThingClassPropertyFields(class *models.SemanticSchemaClass, getNetworkActionsAndThings *map[string]*graphql.Object, weaviate string) (graphql.Fields, error) {
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
				thingOrActionType, ok := (*getNetworkActionsAndThings)[dataType]

				if !ok {
					return nil, fmt.Errorf("no such thing/action class '%s'", property.AtDataType[index])
				}

				dataTypeClasses[index] = thingOrActionType
			}

			dataTypeUnionConf := graphql.UnionConfig{
				Name:  fmt.Sprintf("%s%s%s%s", weaviate, class.Class, capitalizedPropertyName, "Obj"),
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
					return nil, fmt.Errorf("resolving single network thing class property field not supported")
				},
			}
		} else {
			convertedDataType, err := handleNetworkGetNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}

			singleThingClassPropertyFields[property.Name] = convertedDataType
		}
	}

	singleThingClassPropertyFields["uuid"] = &graphql.Field{
		Description: descriptions.NetworkGetClassUUIDDesc,
		Type:        graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return singleThingClassPropertyFields, nil
}

func handleNetworkGetNonObjectDataTypes(dataType schema.DataType, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {

	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
		}, nil

	case schema.DataTypeText:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Float,
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Boolean,
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
		}, nil

	default:
		return nil, fmt.Errorf("%s", schema.ErrorNoSuchDatatype)
	}
}
