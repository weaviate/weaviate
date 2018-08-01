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
	//"bytes"
	"fmt"
	//"reflect"
	//"strconv"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	"strings"
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

	g.weaviateGraphQLSchema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(schemaObject),
	})

	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	} else {
		return nil
	}
}

// TODO: check all Things strings

func (g *GraphQL) assembleFullSchema() (graphql.Fields, error) {

	localConvertedFetchActions, err := g.buildActionClassFieldsFromSchema()
	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local convertedfetch because: %v", err)
	}

	localConvertedFetchThings, err := g.buildThingClassFieldsFromSchema()
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

func (g *GraphQL) buildActionClassFieldsFromSchema() (*graphql.ObjectConfig, error) {

	actionClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {
		field, err := buildSingleActionClassField(class)

		if err != nil {
			return nil, err
		}
		actionClassFields[class.Class] = field
	}
	localConvertedFetchActions := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchActionsObj",
		Fields:      actionClassFields,
		Description: "Fetch Actions on the internal Weaviate",
	}
	return &localConvertedFetchActions, nil
}

//THING
func (g *GraphQL) buildThingClassFieldsFromSchema() (*graphql.ObjectConfig, error) {

	thingClassFields := graphql.Fields{}

	for _, class := range g.databaseSchema.ThingSchema.Schema.Classes {
		field, err := buildSingleThingClassField(class)

		if err != nil {
			return nil, err
		}
		thingClassFields[class.Class] = field
	}
	localConvertedFetchThings := graphql.ObjectConfig{
		Name:        "WeaviateLocalConvertedFetchThingsObj",
		Fields:      thingClassFields,
		Description: "Fetch Things on the internal Weaviate",
	}
	return &localConvertedFetchThings, nil
}

func buildSingleActionClassField(class *models.SemanticSchemaClass) (*graphql.Field, error) {

	singleActionClassPropertyFields, err := buildSingleActionClassPropertyFields(class)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse properties from action:", err)
	}
	singleActionClassPropertyFieldsObj := graphql.ObjectConfig{
		Name:        class.Class,
		Fields:      singleActionClassPropertyFields,
		Description: "Type of fetch on the internal Weaviate", // check string TODO
	}
	return &graphql.Field{
		Type:        graphql.NewObject(singleActionClassPropertyFieldsObj),
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}, nil
}

//THING
func buildSingleThingClassField(class *models.SemanticSchemaClass) (*graphql.Field, error) {

	singleThingClassPropertyFields, err := buildSingleThingClassPropertyFields(class)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse properties from thing:", err)
	}
	singleThingClassPropertyFieldsObj := graphql.ObjectConfig{
		Name:        class.Class,
		Fields:      singleThingClassPropertyFields,
		Description: "Type of fetch on the internal Weaviate", // todo
	}
	return &graphql.Field{
		Type:        graphql.NewObject(singleThingClassPropertyFieldsObj),
		Description: class.Description,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}, nil
}

func buildSingleActionClassPropertyFields(class *models.SemanticSchemaClass) (graphql.Fields, error) {

	singleActionClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {

		if propertyDataTypeIsClass(property) {
			//			handleObjectDataTypes(property, fields) // TODO: implement data type object handling
		} else {
			convertedDataType, err := handleNonObjectDataTypes(property.AtDataType[0], singleActionClassPropertyFields, property)

			if err != nil {
				return nil, err
			}
			singleActionClassPropertyFields[property.Name] = convertedDataType
		}
	}
	return singleActionClassPropertyFields, nil
}

// THING
func buildSingleThingClassPropertyFields(class *models.SemanticSchemaClass) (graphql.Fields, error) {

	singleThingClassPropertyFields := graphql.Fields{}

	for _, property := range class.Properties {

		if propertyDataTypeIsClass(property) {
			//			handleObjectDataTypes(property, fields) // TODO: implement data type object handling
		} else {
			convertedDataType, err := handleNonObjectDataTypes(property.AtDataType[0], singleThingClassPropertyFields, property)

			if err != nil {
				return nil, err
			}
			singleThingClassPropertyFields[property.Name] = convertedDataType
		}
	}
	return singleThingClassPropertyFields, nil
}

func propertyDataTypeIsClass(property *models.SemanticSchemaClassProperty) bool {

	firstChar := string([]rune(property.AtDataType[0])[0]) // get first char from first element using utf-8

	if firstChar == strings.ToUpper(firstChar) {
		return true
	}
	return false
}

//// TODO make gql.union, either from conf or []string property.AtDataType
//func handleObjectDataTypes(property *models.SemanticSchemaClassProperty, fields graphql.Fields) error {
//	for _, dataType := range property.AtDataType {
//		// TODO: attempting to handle objects as datatypes.
//	}
//	unionConf := &graphql.UnionConfig{
//		Name:        property.Name,
//		Types:       property.AtDataType,
//		Description: property.Description,
//	}
//	argh := &graphql.NewUnion(unionConf)
//	//fields[property.Name] = &graphql.NewUnion(unionConf)
//	return nil
//}

// TODO fields is not required?
func handleNonObjectDataTypes(dataType string, fields graphql.Fields, property *models.SemanticSchemaClassProperty) (*graphql.Field, error) {

	switch dataType {

	case "string":
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case "int":
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Int,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case "number":
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Float,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case "boolean":
		return &graphql.Field{
			Description: property.Description,
			Type:        graphql.Boolean,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		}, nil

	case "date":
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
		}, fmt.Errorf("I DON'T KNOW THIS VALUE!")
	}
}

// TODO: return Field, error and parse to objectconf -> object on a higher level
func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj(localConvertedFetchActions *graphql.ObjectConfig,
	localConvertedFetchThings *graphql.ObjectConfig) (*graphql.ObjectConfig, error) {

	convertedFetchThingsAndActionFields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetchActions",
			Description: "Locate Actions on the local Weaviate",
			Type:        graphql.NewObject(*localConvertedFetchActions),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"Things": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetchThings",
			Description: "Locate Things on the local Weaviate",
			Type:        graphql.NewObject(*localConvertedFetchThings),
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
	return &convertedFetchThingsAndActionFieldsObject, nil
}

func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj() (*graphql.ObjectConfig, error) {

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
	return &metaFetchGenericsThingsAndActionFieldsObject, nil
}

func (g *GraphQL) genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject *graphql.ObjectConfig) (*graphql.ObjectConfig, error) {

	metaFetchGenericsField := graphql.Fields{
		"Generics": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsObj",
			Description: "Fetch generic meta information based on the type",
			Type:        graphql.NewObject(*localMetaFetchObject),
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
	return &metaFetchGenericsFieldObject, nil
}

func (g *GraphQL) genConvertedFetchAndMetaGenericsFields(
	localConvertedFetchObject *graphql.ObjectConfig,
	localMetaGenericsObject *graphql.ObjectConfig) (*graphql.ObjectConfig, error) {

	convertedAndMetaFetchFields := graphql.Fields{
		"ConvertedFetch": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetch",
			Type:        graphql.NewObject(*localConvertedFetchObject),
			Description: "Do a converted fetch to search Things or Actions on the local weaviate",
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"MetaFetch": &graphql.Field{
			Name:        "WeaviateLocalMetaFetch",
			Type:        graphql.NewObject(*localMetaGenericsObject),
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
	return weaviateLocalObject, nil
}

func (g *GraphQL) buildLocalField(localMetaAndConvertedFetchObject *graphql.ObjectConfig) (*graphql.Field, error) {

	field := graphql.Field{
		Type:        graphql.NewObject(*localMetaAndConvertedFetchObject),
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
