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
	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/graphql-go/graphql"
)

// Build the GraphQL schema based on
// 1) the static query structure (e.g. LocalFetch)
// 2) the (dynamic) database schema from Weaviate

func (g *GraphQL) buildGraphqlSchema() error {
	rootFieldsObject, err := assembleFullSchema(g)

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

func assembleFullSchema(g *GraphQL) (graphql.Fields, error) {
	// This map is used to store all the Thing and Action Objects, so that we can use them in references.
	convertedFetchActionsAndThings := make(map[string]*graphql.Object)
	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterOptions := make(map[string]*graphql.InputObject)
	filterFetchOptions := make(map[string]*graphql.InputObject)

	localConvertedFetchActions, err := genActionClassFieldsFromSchema(g, &convertedFetchActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local convertedfetch because: %v", err)
	}

	localConvertedFetchThings, err := genThingClassFieldsFromSchema(g, &convertedFetchActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate thing fields from schema for local convertedfetch because: %v", err)
	}

	classParentTypeIsAction := true
	localMetaFetchActions, err := genMetaClassFieldsFromSchema(g.databaseSchema.ActionSchema.Schema.Classes, classParentTypeIsAction)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate action fields from schema for local metafetch because: %v", err)
	}

	classParentTypeIsAction = false
	localMetaFetchThings, err := genMetaClassFieldsFromSchema(g.databaseSchema.ThingSchema.Schema.Classes, classParentTypeIsAction)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate thing fields from schema for local metafetch because: %v", err)
	}

	localConvertedFetchObject := genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj(localConvertedFetchActions, localConvertedFetchThings)

	localMetaFetchObject := genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj(localMetaFetchActions, localMetaFetchThings)

	localMetaGenericsObject := genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject)

	localMetaAndConvertedFetchObject := genConvertedFetchAndMetaGenericsFields(localConvertedFetchObject, localMetaGenericsObject, filterOptions, filterFetchOptions)

	localField := &graphql.Field{
		Type:        localMetaAndConvertedFetchObject,
		Description: "Locate on the local Weaviate",
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return g.dbConnector, nil
		},
	}

	rootFields := graphql.Fields{
		"Local":   localField,
		"Network": nil,
	}

	return rootFields, nil
}

// generate the static parts of the schema
func genThingsAndActionsFieldsForWeaviateLocalConvertedFetchObj(localConvertedFetchActions *graphql.Object, localConvertedFetchThings *graphql.Object) *graphql.Object {
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

	return graphql.NewObject(convertedFetchThingsAndActionFieldsObject)
}

func genThingsAndActionsFieldsForWeaviateLocalMetaFetchGenericsObj(localMetaFetchActions *graphql.Object, localMetaFetchThings *graphql.Object) *graphql.Object {
	metaFetchGenericsThingsAndActionFields := graphql.Fields{

		"Actions": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsActions",
			Description: "Actions to fetch for meta generic fetch",
			Type:        localMetaFetchActions,
			Args: graphql.FieldConfigArgument{
				"_maxArraySize": &graphql.ArgumentConfig{
					Description: "If there are arrays in the result, limit them to this size",
					Type:        graphql.Int,
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},

		"Things": &graphql.Field{
			Name:        "WeaviateLocalMetaFetchGenericsThings",
			Description: "Things to fetch for meta generic fetch",
			Type:        localMetaFetchThings,
			Args: graphql.FieldConfigArgument{
				"_maxArraySize": &graphql.ArgumentConfig{
					Description: "If there are arrays in the result, limit them to this size",
					Type:        graphql.Int,
				},
			},
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

	return graphql.NewObject(metaFetchGenericsThingsAndActionFieldsObject)
}

func genGenericsFieldForWeaviateLocalMetaFetchObj(localMetaFetchObject *graphql.Object) *graphql.Object {
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

	return graphql.NewObject(metaFetchGenericsFieldObject)
}

func genConvertedFetchAndMetaGenericsFields(localConvertedFetchObject *graphql.Object, localMetaGenericsObject *graphql.Object, filterOptions map[string]*graphql.InputObject, filterFetchOptions map[string]*graphql.InputObject) *graphql.Object {
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

	return graphql.NewObject(*weaviateLocalObject)
}
