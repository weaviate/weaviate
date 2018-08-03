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
	"bytes"
	"fmt"
	//"reflect"
	//"strconv"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/graphql-go/graphql"
	//"strings"
)

// Build the GraphQL schema based on
// 1) the static query structure (e.g. LocalFetch)
// 2) the (dynamic) database schema from Weaviate

func (g *GraphQL) buildGraphqlSchema() error {

	rootFieldsObject, err := g.assembleFullSchema()

	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}

	schemaObject := graphql.ObjectConfig{
		Name:        "WeaviateObj",
		Fields:      rootFieldsObject,
		Description: "Location of the root query",
	}

	// Run grahql.NewSchema in a sub-closure, so that we can recover from panics.
	// We need to use panics to return errors deep inside the dynamic generation of the GraphQL schema,
	// inside the FieldThunks. There is _no_ way to bubble up an error besides panicking.
	func() {
		defer func() {
			if r := recover(); r != nil {
				var ok bool
				err, ok = r.(error) // can't shadow err here; we need the err from outside the function closure.
				if !ok {
					err = fmt.Errorf("%v", err)
				}
			}
		}()

		g.weaviateGraphQLSchema, err = graphql.NewSchema(graphql.SchemaConfig{
			Query: graphql.NewObject(schemaObject),
		})
	}()

	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	} else {
		return nil
	}
}

// check: regel class refs voor meerdere objecten als datatype (union)
// check: maak dit ook voor Things
// check: refactor naar objects returnen ipv object configs
// check: check all Things strings
// TODO: confirm output of dynamic schema generation; classes as properties in lists y/n?
// TODO: implement metafetch
// TODO: implement filters

func (g *GraphQL) assembleFullSchema() (graphql.Fields, error) {

	// This map is used to store all the Thing and Action ObjectConfigs, so that we can use them in references.
	convertedFetchActionsAndThings := make(map[string]*graphql.Object)

	localConvertedFetchActions, err := g.buildActionClassFieldsFromSchema(&convertedFetchActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local convertedfetch because: %v", err)
	}

	localConvertedFetchThings, err := g.buildThingClassFieldsFromSchema(&convertedFetchActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local convertedfetch because: %v", err)
	}

	localConvertedFetchObject, err := g.genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj(localConvertedFetchActions, localConvertedFetchThings)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate things and action fields for local convertedfetch because: %v", err)
	}

	localMetaFetchObject, err := g.genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj()
	if err != nil {
		return nil, fmt.Errorf("Failed to generate things and action fields for local metafetch because: %v", err)
	}

	localMetaGenericsObject, err := g.genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate generics field for local metafetch because: %v", err)
	}

	localMetaAndConvertedFetchObject, err := g.genConvertedFetchAndMetaGenericsFields(localConvertedFetchObject, localMetaGenericsObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate meta and convertedfetch fields for local weaviateobject because: %v", err)
	}

	localObject, err := g.buildLocalField(localMetaAndConvertedFetchObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate local field for local weaviateobject because: %v", err)
	}

	rootFieldsObject, err := g.genRootQueryFields(localObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate root query because: %v", err)
	}

	return rootFieldsObject, nil
}

// Build the dynamically generated Actions part of the schema
func (g *GraphQL) buildActionClassFieldsFromSchema(convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {

	actionClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {

		field, obj, err := buildSingleActionClassField(class, convertedFetchActionsAndThings)

		if err != nil {
			return nil, err
		}
		actionClassFields[class.Class] = field

		(*convertedFetchActionsAndThings)[class.Class] = obj
	}

	localConvertedFetchActions := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchActionsObj",
		Fields:      actionClassFields,
		Description: "Fetch Actions on the internal Weaviate",
	}

	return graphql.NewObject(localConvertedFetchActions), nil
}

func buildSingleActionClassField(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object, error) {
	singleActionClassPropertyFieldsObj := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleActionClassPropertyFields, err := buildSingleActionClassPropertyFields(class, convertedFetchActionsAndThings)
			if err != nil {
				panic("oops")
			}
			return singleActionClassPropertyFields
		}),
		Description: "Type of fetch on the internal Weaviate",
	}

	obj := graphql.NewObject(singleActionClassPropertyFieldsObj)
	field := &graphql.Field{
		Type:        obj,
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	return field, obj, nil
}

func buildSingleActionClassPropertyFields(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (graphql.Fields, error) {

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
				Name:  genClassPropertyClassName(class, property),
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
			convertedDataType, err := handleNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}
			singleActionClassPropertyFields[property.Name] = convertedDataType
		}
	}
	return singleActionClassPropertyFields, nil
}

func genClassPropertyClassName(class *models.SemanticSchemaClass, property *models.SemanticSchemaClassProperty) string {

	var buffer bytes.Buffer

	buffer.WriteString(class.Class)
	buffer.WriteString(property.Name)
	buffer.WriteString("Obj")

	return buffer.String()
}

// Build the dynamically generated Things part of the schema
func (g *GraphQL) buildThingClassFieldsFromSchema(convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Object, error) {

	thingClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ThingSchema.Schema.Classes {
		field, obj, err := buildSingleThingClassField(class, convertedFetchActionsAndThings)

		if err != nil {
			return nil, err
		}
		thingClassFields[class.Class] = field
		(*convertedFetchActionsAndThings)[class.Class] = obj
	}
	localConvertedFetchThings := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchThingsObj",
		Fields:      thingClassFields,
		Description: "Fetch Things on the internal Weaviate",
	}

	return graphql.NewObject(localConvertedFetchThings), nil
}

func buildSingleThingClassField(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (*graphql.Field, *graphql.Object, error) {

	singleThingClassPropertyFieldsObj := graphql.ObjectConfig{
		Name: class.Class,
		Fields: (graphql.FieldsThunk)(func() graphql.Fields {
			singleThingClassPropertyFields, err := buildSingleThingClassPropertyFields(class, convertedFetchActionsAndThings)
			if err != nil {
				panic("oops")
			}
			return singleThingClassPropertyFields
		}),
		Description: "Type of fetch on the internal Weaviate",
	}

	obj := graphql.NewObject(singleThingClassPropertyFieldsObj)
	field := &graphql.Field{
		Type:        obj,
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	return field, obj, nil
}

func buildSingleThingClassPropertyFields(class *models.SemanticSchemaClass, convertedFetchActionsAndThings *map[string]*graphql.Object) (graphql.Fields, error) {

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
				Name:  genClassPropertyClassName(class, property),
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
			convertedDataType, err := handleNonObjectDataTypes(*propertyType, property)

			if err != nil {
				return nil, err
			}
			singleThingClassPropertyFields[property.Name] = convertedDataType
		}
	}
	return singleThingClassPropertyFields, nil
}

func handleNonObjectDataTypes(dataType schema.DataType, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {

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

func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj(localConvertedFetchActions *graphql.Object,
	localConvertedFetchThings *graphql.Object) (*graphql.Object, error) {

	convertedFetchThingsAndActionFields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetchActions",
			Description: "Locate Actions on the local Weaviate",
			Type:        localConvertedFetchActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"Things": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetchThings",
			Description: "Locate Things on the local Weaviate",
			Type:        localConvertedFetchThings,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	convertedFetchThingsAndActionFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchObj",
		Fields:      convertedFetchThingsAndActionFields,
		Description: "Fetch things or actions on the internal Weaviate",
	}
	return graphql.NewObject(convertedFetchThingsAndActionFieldsObject), nil
}

func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj() (*graphql.Object, error) {

	metaFetchGenericsThingsAndActionFields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsActions",
			Description: "Action to fetch for meta generic fetch",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"Things": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsThings",
			Description: "Thing to fetch for meta generic fetch",
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchGenericsThingsAndActionFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalMetaFetchGenericsObj",
		Fields:      metaFetchGenericsThingsAndActionFields,
		Description: "Object type to fetch",
	}
	return graphql.NewObject(metaFetchGenericsThingsAndActionFieldsObject), nil
}

func (g *GraphQL) genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject *graphql.Object) (*graphql.Object, error) {

	metaFetchGenericsField := graphql.Fields{
		"Generics": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsObj",
			Description: "Fetch generic meta information based on the type",
			Type:        localMetaFetchObject,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	metaFetchGenericsFieldObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalMetaFetchObj",
		Fields:      metaFetchGenericsField,
		Description: "Fetch things or actions on the internal Weaviate",
	}
	return graphql.NewObject(metaFetchGenericsFieldObject), nil
}

func (g *GraphQL) genConvertedFetchAndMetaGenericsFields(
	localConvertedFetchObject *graphql.Object,
	localMetaGenericsObject *graphql.Object) (*graphql.Object, error) {

	convertedAndMetaFetchFields := graphql.Fields{
		"ConvertedFetch": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetch",
			Type:        localConvertedFetchObject,
			Description: "Do a converted fetch to search Things or Actions on the local weaviate",
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"MetaFetch": &graphql.Field{
			Name:        "WeaviateLocalMetaFetch",
			Type:        localMetaGenericsObject,
			Description: "Fetch meta information about Things or Actions on the local weaviate",
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}
	weaviateLocalObject := &graphql.ObjectConfig{
		Name:        "WeaviateLocalObj",
		Fields:      convertedAndMetaFetchFields,
		Description: "Type of fetch on the internal Weaviate",
	}
	return graphql.NewObject(*weaviateLocalObject), nil
}

func (g *GraphQL) buildLocalField(localMetaAndConvertedFetchObject *graphql.Object) (*graphql.Field, error) {

	field := graphql.Field{
		Type:        localMetaAndConvertedFetchObject,
		Description: "Locate on the local Weaviate",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}
	return &field, nil
}

func (g *GraphQL) genRootQueryFields(localField *graphql.Field) (graphql.Fields, error) {

	var rootQueryFields = graphql.Fields{
		"Local":   localField,
		"Network": nil,
	}
	return rootQueryFields, nil
}
