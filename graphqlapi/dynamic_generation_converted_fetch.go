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

package graphqlapi

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/graphql-go/graphql"
)

// Build the dynamically generated ConvertedFetch Actions part of the schema
func (g *GraphQL) genActionClassFieldsFromSchema(convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {

	actionClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {

		singleActionClassField, singleActionClassObject, err := genSingleActionClassField(class, convertedFetchActionsAndThings)

		if err != nil {
			return nil, err
		}
		actionClassFields[class.Class] = singleActionClassField

		(*convertedFetchActionsAndThings)[class.Class] = singleActionClassObject
	}

	localConvertedFetchActions := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchActionsObj",
		Fields:      actionClassFields,
		Description: "Fetch Actions on the internal Weaviate",
	}

	return graphql.NewObject(localConvertedFetchActions), nil
}

func genSingleActionClassField(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object, error) {
	singleActionClassPropertyFields := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleActionClassPropertyFields, err := genSingleActionClassPropertyFields(class, convertedFetchActionsAndThings)
			if err != nil {
				panic("oops")
			}
			return singleActionClassPropertyFields
		}),
		Description: "Type of fetch on the internal Weaviate",
	}

	singleActionClassPropertyFieldsObj := graphql.NewObject(singleActionClassPropertyFields)
	singleActionClassPropertyFieldsField := &graphql.Field{
		Type:        singleActionClassPropertyFieldsObj,
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	return singleActionClassPropertyFieldsField, singleActionClassPropertyFieldsObj, nil
}

func genSingleActionClassPropertyFields(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (graphql.Fields, error) {

	singleActionClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {

		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, err
		}
		if *propertyType == schema.DataTypeCRef {
			numberOfDataTypes := len(property.AtDataType)

			dataTypeClasses := make([]*graphql.Object, numberOfDataTypes)

			for index, dataType := range property.AtDataType {

				thingOrActionType, ok := (*convertedFetchActionsAndThings)[dataType]
				if !ok {
					panic(fmt.Errorf("No such thing/action class '%s'", property.AtDataType[index]))
				}

				dataTypeClasses[index] = thingOrActionType
			}
			dataTypeUnionConf := graphql.UnionConfig{
				Name:  mergeStrings(class.Class, property.Name),
				Types: dataTypeClasses,
				ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
					return nil
				},
				Description: property.Description,
			}
			multipleClassDataTypesUnion := graphql.NewUnion(dataTypeUnionConf)

			singleActionClassPropertyFields[property.Name] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("Not supported")
				},
			}
		} else {
			convertedDataType, err := handleConvertedFetchNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}
			singleActionClassPropertyFields[property.Name] = convertedDataType
		}
	}
	return singleActionClassPropertyFields, nil
}

// build the dynamically generated ConvertedFetch Things part of the schema
func (g *GraphQL) genThingClassFieldsFromSchema(convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {

	thingClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ThingSchema.Schema.Classes {
		SingleThingClassField, SingleThingClassObject, err := genSingleThingClassField(class, convertedFetchActionsAndThings)

		if err != nil {
			return nil, err
		}
		thingClassFields[class.Class] = SingleThingClassField
		(*convertedFetchActionsAndThings)[class.Class] = SingleThingClassObject
	}
	localConvertedFetchThings := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchThingsObj",
		Fields:      thingClassFields,
		Description: "Fetch Things on the internal Weaviate",
	}

	return graphql.NewObject(localConvertedFetchThings), nil
}

func genSingleThingClassField(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object, error) {

	singleThingClassPropertyFieldsObj := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleThingClassPropertyFields, err := genSingleThingClassPropertyFields(class, convertedFetchActionsAndThings)
			if err != nil {
				panic("oops")
			}
			return singleThingClassPropertyFields
		}),
		Description: "Type of fetch on the internal Weaviate",
	}

	thingClassPropertyFieldsObject := graphql.NewObject(singleThingClassPropertyFieldsObj)
	thingClassPropertyFieldsField := &graphql.Field{
		Type:        thingClassPropertyFieldsObject,
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	return thingClassPropertyFieldsField, thingClassPropertyFieldsObject, nil
}

func genSingleThingClassPropertyFields(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (graphql.Fields, error) {

	singleThingClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {

		propertyType, err := schema.GetPropertyDataType(class, property.Name)
		if err != nil {
			return nil, err
		}
		if *propertyType == schema.DataTypeCRef {
			numberOfDataTypes := len(property.AtDataType)

			dataTypeClasses := make([]*graphql.Object, numberOfDataTypes)

			for index, dataType := range property.AtDataType {

				thingOrActionType, ok := (*convertedFetchActionsAndThings)[dataType]
				if !ok {
					panic(fmt.Errorf("No such thing/action class '%s'", property.AtDataType[index]))
				}

				dataTypeClasses[index] = thingOrActionType
			}

			dataTypeUnionConf := graphql.UnionConfig{
				Name:  mergeStrings(class.Class, property.Name),
				Types: dataTypeClasses,
				ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
					return nil
				},
				Description: property.Description,
			}

			multipleClassDataTypesUnion := graphql.NewUnion(dataTypeUnionConf)

			singleThingClassPropertyFields[property.Name] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return nil, fmt.Errorf("Not supported")
				},
			}
		} else {
			convertedDataType, err := handleConvertedFetchNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}
			singleThingClassPropertyFields[property.Name] = convertedDataType
		}
	}
	return singleThingClassPropertyFields, nil
}

func handleConvertedFetchNonObjectDataTypes(dataType schema.DataType, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {

	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Boolean,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String, // String since no graphql date datatype exists
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	default:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
		}, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}
