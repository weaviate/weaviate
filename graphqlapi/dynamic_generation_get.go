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
	"errors"
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	graphql_ast "github.com/graphql-go/graphql/language/ast"
)

// Build the dynamically generated Get Actions part of the schema
func (g *graphQL) genActionClassFieldsFromSchema(getActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {
	actionClassFields := graphql.Fields{}

	if len(g.databaseSchema.ActionSchema.Schema.Classes) == 0 {
		return nil, nil
	}

	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {
		singleActionClassField, singleActionClassObject := g.genSingleActionClassField(class, getActionsAndThings)
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

func (g *graphQL) genSingleActionClassField(class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object) {
	singleActionClassPropertyFields := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleActionClassPropertyFields, err := genSingleActionClassPropertyFields(class, getActionsAndThings)

			if err != nil {
				panic(fmt.Sprintf("Failed to generate single Action Class property fields; %#v", err))
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
			fmt.Printf("- singleActionClassPropertyFieldsObj (extract pagination)\n")
			filters := p.Source.(*localGetFilters)

			pagination, err := extractPaginationFromArgs(p.Args)
			if err != nil {
				return nil, err
			}

			properties, err := extractPropertiesFromFieldASTs(p.Info.FieldASTs)
			if err != nil {
				return nil, err
			}
			getAction := &getClassParams{
				filters:    filters,
				pagination: pagination,
				kind:       kind.ACTION_KIND,
				className:  class.Class,
				properties: properties,
			}

			result, err := g.resolver.ResolveGetClass(getAction)

			if err != nil {
				return nil, err
			} else {
				fmt.Printf("RESLVER PROMISE: %#v\n", result)
				return func() (interface{}, error) {
					return "jep", nil
				}, nil
			}
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
				//ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
				//	fmt.Printf("Resolver: WHOOPTYDOO\n")
				//	return nil
				//},
				Description: property.Description,
			}

			fmt.Printf("genSingleActionClassPropertyFields union: %s %s\n", class.Class, capitalizedPropertyName)

			multipleClassDataTypesUnion := graphql.NewUnion(dataTypeUnionConf)

			singleActionClassPropertyFields[capitalizedPropertyName] = &graphql.Field{
				Type:        multipleClassDataTypesUnion,
				Description: property.Description,
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					fmt.Printf("- Resolve action property field (ref?)\n")
					fmt.Printf("WHOOPTYDOO2\n")
					return nil, nil
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
			fmt.Printf("WHOOPTYDOO uuid\n")
			return "uuid", nil
		},
	}

	return singleActionClassPropertyFields, nil
}

// Build the dynamically generated Get Things part of the schema
func (g *graphQL) genThingClassFieldsFromSchema(getActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {
	thingClassFields := graphql.Fields{}

	if len(g.databaseSchema.ThingSchema.Schema.Classes) == 0 {
		return nil, nil
	}

	for _, class := range g.databaseSchema.ThingSchema.Schema.Classes {
		singleThingClassField, singleThingClassObject := genSingleThingClassField(class, getActionsAndThings)
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

func genSingleThingClassField(class *models.SemanticSchemaClass, getActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object) {
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
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			fmt.Printf("- thing class (supposed to extract pagination, now return nil)\n")
			return nil, nil
		},
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
					fmt.Printf("- thing class resolve blurgh (ret nil)\n")
					return nil, nil
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
			fmt.Printf("- thing class get uuid (return 'foo')\n")
			return "foo", nil
		},
	}

	return singleThingClassPropertyFields, nil
}

func handleGetNonObjectDataTypes(dataType schema.DataType, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {
	switch dataType {

	case schema.DataTypeString:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("GET PRIMITIVE PROP: string\n")
				return "primitive string", nil
			},
		}, nil

	case schema.DataTypeInt:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("GET PRIMITIVE PROP: int\n")
				return nil, nil
			},
		}, nil

	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("GET PRIMITIVE PROP: float\n")
				return 4.2, nil
			},
		}, nil

	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Boolean,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("GET PRIMITIVE PROP: bool\n")
				return true, nil
			},
		}, nil

	case schema.DataTypeDate:
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String, // String since no graphql date datatype exists
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				fmt.Printf("GET PRIMITIVE PROP: date\n")
				return "somedate", nil
			},
		}, nil

	default:
		fmt.Printf("ASDLKJASL:DJASKL:DJSKL:ADJ %#v\n", dataType)
		return nil, fmt.Errorf(schema.ErrorNoSuchDatatype)
	}
}

func extractPaginationFromArgs(args map[string]interface{}) (*pagination, error) {
	afterVal, afterOk := args["after"]
	firstVal, firstOk := args["first"]

	if firstOk && afterOk {
		after := afterVal.(int)
		first := firstVal.(int)
		return &pagination{
			after: after,
			first: first,
		}, nil
	}

	if firstOk || afterOk {
		return nil, errors.New("after and first must both be specified")
	}

	return nil, nil
}

func extractPropertiesFromFieldASTs(fieldASTs []*graphql_ast.Field) ([]property, error) {
	var properties []property

	for _, fieldAST := range fieldASTs {
		selections := fieldAST.SelectionSet.Selections
		if len(selections) != 1 {
			panic("unspected length")
		}
		selection := selections[0].(*graphql_ast.Field)
		name := selection.Name.Value
		properties = append(properties, property{name: name})
	}

	return properties, nil
}
