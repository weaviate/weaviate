//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package network_get provides the network get graphql endpoint for Weaviate
package network_get

import (
	"fmt"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// Build the dynamically generated Get Actions part of the schema
func ActionClassFieldsFromSchema(dbSchema *schema.Schema, getActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Object, error) {
	actionClassFields := graphql.Fields{}

	for _, class := range dbSchema.Actions.Classes {
		singleActionClassField, singleActionClassObject := actionClassField(class, getActionsAndThings, weaviate)
		actionClassFields[class.Class] = singleActionClassField
		// this line assigns the created class to a Hashmap which is used in thunks to handle cyclical relationships (Classes with other Classes as properties)
		(*getActionsAndThings)[class.Class] = singleActionClassObject
	}

	getActions := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGet", weaviate, "ActionsObj"),
		Fields:      actionClassFields,
		Description: descriptions.NetworkGetActionsObj,
	}

	return graphql.NewObject(getActions), nil
}

func actionClassField(class *models.Class, getActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Field, *graphql.Object) {
	actionClassPropertyFields := graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%s", weaviate, class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleactionClassPropertyFields, err := actionClassPropertyFields(class, getActionsAndThings, weaviate)

			if err != nil {
				panic("Failed to generate single Network Action Class property fields")
			}

			return singleactionClassPropertyFields
		}),
		Description: class.Description,
	}

	actionClassPropertyFieldsObj := graphql.NewObject(actionClassPropertyFields)

	actionClassPropertyFieldsField := &graphql.Field{
		Type:        graphql.NewList(actionClassPropertyFieldsObj),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"limit": &graphql.ArgumentConfig{
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.NetworkGetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateNetworkGet%sActions%sWhereInpObj", weaviate, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkGet%sActions%s", weaviate, class.Class)),
						Description: descriptions.NetworkGetWhereInpObj,
					},
				),
			},
		},
		Resolve: ResolveAction,
	}
	return actionClassPropertyFieldsField, actionClassPropertyFieldsObj
}

func actionClassPropertyFields(class *models.Class, getActionsAndThings *map[string]*graphql.Object, weaviate string) (graphql.Fields, error) {
	actionClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {
		propertyType, err := schema.GetPropertyDataType(class, property.Name)

		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			capitalizedPropertyName := strings.Title(property.Name)
			numberOfDataTypes := len(property.DataType)
			dataTypeClasses := make([]*graphql.Object, numberOfDataTypes)

			for index, dataType := range property.DataType {
				thingOrActionType, ok := (*getActionsAndThings)[dataType]

				if !ok {
					return nil, fmt.Errorf("no such thing/action class '%s'", property.DataType[index])
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

			actionClassPropertyFields[capitalizedPropertyName] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("not supported")
				},
			}
		} else {
			prefix := fmt.Sprintf("%s%s", weaviate, class.Class)
			convertedDataType, err := handleNetworkGetNonObjectDataTypes(*propertyType, property, prefix)

			if err != nil {
				return nil, err
			}

			actionClassPropertyFields[property.Name] = convertedDataType
		}
	}

	actionClassPropertyFields["uuid"] = &graphql.Field{
		Description: descriptions.NetworkGetClassUUID,
		Type:        graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return actionClassPropertyFields, nil
}

// Build the dynamically generated Get Things part of the schema
func ThingClassFieldsFromSchema(dbSchema *schema.Schema, actionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Object, error) {
	thingClassFields := graphql.Fields{}

	for _, class := range dbSchema.Things.Classes {
		singleThingClassField, singleThingClassObject := thingClassField(class, actionsAndThings, weaviate)
		thingClassFields[class.Class] = singleThingClassField
		// this line assigns the created class to a Hashmap which is used in thunks to handle cyclical relationships (Classes with other Classes as properties)
		(*actionsAndThings)[class.Class] = singleThingClassObject
	}

	getThings := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%s", "WeaviateNetworkGet", weaviate, "ThingsObj"),
		Fields:      thingClassFields,
		Description: descriptions.NetworkGetThingsObj,
	}

	return graphql.NewObject(getThings), nil
}

func thingClassField(class *models.Class, getActionsAndThings *map[string]*graphql.Object, weaviate string) (*graphql.Field, *graphql.Object) {
	singleThingClassPropertyFieldsObj := graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%s", weaviate, class.Class),
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleThingClassPropertyFields, err := thingClassPropertyFields(class, getActionsAndThings, weaviate)
			if err != nil {
				panic(fmt.Errorf("failed to assemble single Network Thing Class field for Class %s: %s", class.Class, err))
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
				Description: descriptions.First,
				Type:        graphql.Int,
			},
			"after": &graphql.ArgumentConfig{
				Description: descriptions.After,
				Type:        graphql.Int,
			},
			"where": &graphql.ArgumentConfig{
				Description: descriptions.NetworkGetWhere,
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        fmt.Sprintf("WeaviateNetworkGet%sThings%sWhereInpObj", weaviate, class.Class),
						Fields:      common_filters.BuildNew(fmt.Sprintf("WeaviateNetworkGet%sThings%s", weaviate, class.Class)),
						Description: descriptions.NetworkGetWhereInpObj,
					},
				),
			},
		},
		Resolve: ResolveThing,
	}
	return thingClassPropertyFieldsField, thingClassPropertyFieldsObject
}

func thingClassPropertyFields(class *models.Class, actionsAndThings *map[string]*graphql.Object, weaviate string) (graphql.Fields, error) {
	fields := graphql.Fields{}

	for _, property := range class.Properties {

		propertyType, err := schema.GetPropertyDataType(class, property.Name)

		if err != nil {
			return nil, err
		}

		if *propertyType == schema.DataTypeCRef {
			capitalizedPropertyName := strings.Title(property.Name)
			numberOfDataTypes := len(property.DataType)
			dataTypeClasses := make([]*graphql.Object, numberOfDataTypes)

			for index, dataType := range property.DataType {
				thingOrActionType, ok := (*actionsAndThings)[dataType]

				if !ok {
					return nil, fmt.Errorf("no such thing/action class '%s'", property.DataType[index])
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

			fields[capitalizedPropertyName] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("resolving single network thing class property field not supported")
				},
			}
		} else {
			prefix := fmt.Sprintf("%s%s", weaviate, class.Class)
			convertedDataType, err := handleNetworkGetNonObjectDataTypes(*propertyType, property, prefix)

			if err != nil {
				return nil, err
			}

			fields[property.Name] = convertedDataType
		}
	}

	fields["uuid"] = &graphql.Field{
		Description: descriptions.NetworkGetClassUUID,
		Type:        graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("not supported")
		},
	}

	return fields, nil
}

func handleNetworkGetNonObjectDataTypes(dataType schema.DataType,
	property *models.Property, prefix string) (*graphql.Field, error) {

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
	case schema.DataTypeGeoCoordinates:
		obj := newGeoCoordinatesObject(prefix)

		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        obj,
		}, nil

	default:
		return nil, fmt.Errorf("%s: %s", schema.ErrorNoSuchDatatype, dataType)
	}
}

func newGeoCoordinatesObject(prefix string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Description: "GeoCoordinates as latitude and longitude in decimal form",
		Name:        fmt.Sprintf("%sGeoCoordinatesObj", prefix),
		Fields: graphql.Fields{
			"latitude": &graphql.Field{
				Name:        "Latitude",
				Description: "The Latitude of the point in decimal form.",
				Type:        graphql.Float,
			},
			"longitude": &graphql.Field{
				Name:        "Longitude",
				Description: "The Longitude of the point in decimal form.",
				Type:        graphql.Float,
			},
		},
	})
}
