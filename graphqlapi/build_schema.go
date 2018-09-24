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

	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/graphql-go/graphql"
)

var dbConnector dbconnector.DatabaseConnector

// Build the GraphQL schema based on
// 1) the static query structure (e.g. Get)
// 2) the (dynamic) database schema from Weaviate

func (g *GraphQL) buildGraphqlSchema() error {
	rootFieldsObject, err := g.assembleFullSchema()

	if err != nil {
		return fmt.Errorf("could not build GraphQL schema, because: %v", err)
	}

	schemaObject := graphql.ObjectConfig{
		Name:        "WeaviateObj",
		Fields:      rootFieldsObject,
		Description: "Location of the root query",
	}

	// Run grahpql.NewSchema in a sub-closure, so that we can recover from panics.
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
		return fmt.Errorf("could not build GraphQL schema, because: %v", err)
	}
	return nil
}

func (g *GraphQL) assembleFullSchema() (graphql.Fields, error) {
	// This map is used to store all the Thing and Action Objects, so that we can use them in references.

	getActionsAndThings := make(map[string]*graphql.Object)
	// this map is used to store all the Filter InputObjects, so that we can use them in references.
	filterOptions := make(map[string]*graphql.InputObject)

	localGetActions, err := genActionClassFieldsFromSchema(g, &getActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("failed to generate action fields from schema for local Get because: %v", err)
	}

	localGetThings, err := genThingClassFieldsFromSchema(g, &getActionsAndThings)

	if err != nil {
		return nil, fmt.Errorf("failed to generate thing fields from schema for local Get because: %v", err)
	}

	classParentTypeIsAction := true
	localGetMetaActions, err := genMetaClassFieldsFromSchema(g.databaseSchema.ActionSchema.Schema.Classes, classParentTypeIsAction)

	if err != nil {
		return nil, fmt.Errorf("failed to generate action fields from schema for local MetaGet because: %v", err)
	}

	classParentTypeIsAction = false
	localGetMetaThings, err := genMetaClassFieldsFromSchema(g.databaseSchema.ThingSchema.Schema.Classes, classParentTypeIsAction)

	if err != nil {
		return nil, fmt.Errorf("failed to generate thing fields from schema for local MetaGet because: %v", err)
	}

	localGetObject := genThingsAndActionsFieldsForWeaviateLocalGetObj(localGetActions, localGetThings)

	localGetMetaObject := genThingsAndActionsFieldsForWeaviateLocalGetMetaObj(localGetMetaActions, localGetMetaThings)

	localGetAndGetMetaObject := genGetAndGetMetaFields(localGetObject, localGetMetaObject, filterOptions)

	localField := &graphql.Field{
		Type:        localGetAndGetMetaObject,
		Description: "Query a local Weaviate instance",
		Resolve:     dummyResolver,
	}

	rootFields := graphql.Fields{
		"Local":   localField,
		"Network": nil,
	}

	return rootFields, nil
}

// generate the static parts of the schema
func genThingsAndActionsFieldsForWeaviateLocalGetObj(localGetActions *graphql.Object, localGetThings *graphql.Object) *graphql.Object {
	getThingsAndActionFields := graphql.Fields{

		"Actions": &graphql.Field{
			Name:        "WeaviateLocalGetActions",
			Description: "Get Actions on the Local Weaviate",
			Type:        localGetActions,
			Resolve:     dummyResolver,
		},

		"Things": &graphql.Field{
			Name:        "WeaviateLocalGetThings",
			Description: "Get Things on the Local Weaviate",
			Type:        localGetThings,
			Resolve:     dummyResolver,
		},
	}

	getThingsAndActionFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalGetObj",
		Fields:      getThingsAndActionFields,
		Description: "Type of Get function to get Things or Actions on the Local Weaviate",
	}

	return graphql.NewObject(getThingsAndActionFieldsObject)
}

func genThingsAndActionsFieldsForWeaviateLocalGetMetaObj(localGetMetaActions *graphql.Object, localGetMetaThings *graphql.Object) *graphql.Object {
	getMetaThingsAndActionFields := graphql.Fields{

		"Actions": &graphql.Field{
			Name:        "WeaviateLocalGetMetaActions",
			Description: "Get Meta information about Actions on the Local Weaviate",
			Type:        localGetMetaActions,
			Resolve:     dummyResolver,
		},

		"Things": &graphql.Field{
			Name:        "WeaviateLocalGetMetaThings",
			Description: "Get Meta information about Things on the Local Weaviate",
			Type:        localGetMetaThings,
			Resolve:     dummyResolver,
		},
	}

	getMetaThingsAndActionFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalGetMetaObj",
		Fields:      getMetaThingsAndActionFields,
		Description: "Type of Get function to get meta information about Things or Actions on the Local Weaviate",
	}

	return graphql.NewObject(getMetaThingsAndActionFieldsObject)
}

func genGetAndGetMetaFields(localGetObject *graphql.Object, localGetMetaObject *graphql.Object, filterOptions map[string]*graphql.InputObject) *graphql.Object {
	filterFields := genFilterFields(filterOptions)
	getAndGetMetaFields := graphql.Fields{

		"Get": &graphql.Field{
			Name:        "WeaviateLocalGet",
			Type:        localGetObject,
			Description: "Get Things or Actions on the local weaviate",
			Args: graphql.FieldConfigArgument{
				"where": &graphql.ArgumentConfig{
					Description: "Filter options for the Get search, to convert the data to the filter input",
					Type: graphql.NewInputObject(
						graphql.InputObjectConfig{
							Name:        "WeaviateLocalGetWhereInpObj",
							Fields:      filterFields,
							Description: "Filter options for the Get search, to convert the data to the filter input",
						},
					),
				},
			},
			Resolve: dummyResolver,
		},

		"GetMeta": &graphql.Field{
			Name:        "WeaviateLocalGetMeta",
			Type:        localGetMetaObject,
			Description: "Query to Get Meta information about the data in the local Weaviate instance",
			Args: graphql.FieldConfigArgument{
				"where": &graphql.ArgumentConfig{
					Description: "Filter options for the GetMeta search, to convert the data to the filter input",
					Type: graphql.NewInputObject(
						graphql.InputObjectConfig{
							Name:        "WeaviateLocalGetMetaWhereInpObj",
							Fields:      filterFields,
							Description: "Filter options for the GetMeta search, to convert the data to the filter input",
						},
					),
				},
			},
			Resolve: dummyResolver,
		},
	}

	weaviateLocalObject := &graphql.ObjectConfig{
		Name:        "WeaviateLocalObj",
		Fields:      getAndGetMetaFields,
		Description: "Type of query on the local Weaviate",
	}

	return graphql.NewObject(*weaviateLocalObject)
}

// dummyResolver is a resolver for all Fields where we don't
// have specific resolvers yet. It is important that it does not
// error or return nil, because then the next resolver would not
// be called. I.e. if we error in the "Local" Resolver, we'll never get
// to the "Get" Resolver, and so on.
// Since we want to start with List of Things for an MVP,
// we need to be able to not block these lower resolvers.
func dummyResolver(p graphql.ResolveParams) (interface{}, error) {
	return "some string", nil
}
