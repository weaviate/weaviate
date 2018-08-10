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
	"github.com/graphql-go/graphql"
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

func (g *GraphQL) assembleFullSchema() (graphql.Fields, error) {

	// This map is used to store all the Thing and Action Objects, so that we can use them in references.
	convertedFetchActionsAndThings := make(map[string]*graphql.Object)
	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterOptions := make(map[string]*graphql.InputObject)
	filterFetchOptions := make(map[string]*graphql.InputObject)

	localConvertedFetchActions, err := g.genActionClassFieldsFromSchema(&convertedFetchActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local convertedfetch because: %v", err)
	}

	localConvertedFetchThings, err := g.genThingClassFieldsFromSchema(&convertedFetchActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate thing fields from schema for local convertedfetch because: %v", err)
	}

	localMetaFetchActions, err := g.genMetaClassFieldsFromSchema(g.databaseSchema.ActionSchema.Schema.Classes, true)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local metafetch because: %v", err)
	}

	localMetaFetchThings, err := g.genMetaClassFieldsFromSchema(g.databaseSchema.ThingSchema.Schema.Classes, false)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate thing fields from schema for local metafetch because: %v", err)
	}

	localConvertedFetchObject, err := g.genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj(localConvertedFetchActions, localConvertedFetchThings)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate things and action fields for local convertedfetch because: %v", err)
	}

	localMetaFetchObject, err := g.genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj(localMetaFetchActions, localMetaFetchThings)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate things and action fields for local metafetch because: %v", err)
	}

	localMetaGenericsObject, err := g.genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate generics field for local metafetch because: %v", err)
	}

	localMetaAndConvertedFetchObject, err := g.genConvertedFetchAndMetaGenericsFields(localConvertedFetchObject, localMetaGenericsObject, filterOptions, filterFetchOptions)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate meta and convertedfetch fields for local weaviateobject because: %v", err)
	}

	localObject, err := g.genLocalField(localMetaAndConvertedFetchObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate local field for local weaviateobject because: %v", err)
	}

	rootFieldsObject, err := g.genRootQueryFields(localObject)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate root query because: %v", err)
	}

	return rootFieldsObject, nil
}

// generate the static parts of the schema
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

func (g *GraphQL) genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj(localMetaFetchActions *graphql.Object, localMetaFetchThings *graphql.Object) (*graphql.Object, error) {

	metaFetchGenericsThingsAndActionFields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsActions",
			Description: "Action to fetch for meta generic fetch",
			Type:        localMetaFetchActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"Things": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsThings",
			Description: "Thing to fetch for meta generic fetch",
			Type:        localMetaFetchThings,
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
	localMetaGenericsObject *graphql.Object,
	filterOptions map[string]*graphql.InputObject,
	filterFetchOptions map[string]*graphql.InputObject) (*graphql.Object, error) {

	filterFields := genFilterFields(filterOptions, filterFetchOptions)

	convertedAndMetaFetchFields := graphql.Fields{
		"ConvertedFetch": &graphql.Field{
			Name:        "WeaviateLocalConvertedFetch",
			Type:        localConvertedFetchObject,
			Description: "Do a converted fetch to search Things or Actions on the local weaviate",
			Args: graphql.FieldConfigArgument{
				"_filter": &graphql.ArgumentConfig{
					Description: "Filter options for the converted fetch search, to convert the data to the filter input",
					Type: graphql.NewInputObject(
						graphql.InputObjectConfig{
							Name:        "WeaviateLocalConvertedFetchFilterInpObj",
							Fields:      filterFields,
							Description: "Filter options for the converted fetch search, to convert the data to the filter input",
						},
					),
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"MetaFetch": &graphql.Field{
			Name:        "WeaviateLocalMetaFetch",
			Type:        localMetaGenericsObject,
			Description: "Fetch meta information about Things or Actions on the local weaviate",
			Args: graphql.FieldConfigArgument{
				"_filter": &graphql.ArgumentConfig{
					Description: "Filter options for the converted fetch search, to convert the data to the filter input",
					Type: graphql.NewInputObject(
						graphql.InputObjectConfig{
							Name:        "WeaviateLocalMetaFetchFilterInpObj",
							Fields:      filterFields,
							Description: "Filter options for the meta fetch search, to convert the data to the filter input",
						},
					),
				},
			},
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

func (g *GraphQL) genLocalField(localMetaAndConvertedFetchObject *graphql.Object) (*graphql.Field, error) {

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

func mergeStrings(stringParts ...string) string {

	var buffer bytes.Buffer
	for _, stringPart := range stringParts {
		buffer.WriteString(stringPart)
	}

	return buffer.String()
}
