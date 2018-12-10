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
	"context"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
	"github.com/graphql-go/graphql"
)

// The communication interface between the REST API and the GraphQL API.
type GraphQL interface {
	// Resolve the GraphQL query in 'query'.
	Resolve(query string, operationName string, variables map[string]interface{}, context context.Context) *graphql.Result
}

type graphQL struct {
	schema           graphql.Schema
	resolverProvider ResolverProvider
}

// Construct a GraphQL API from the database schema, and resolver interface.
func Build(dbSchema *schema.Schema, peers []libnetwork.Peer, resolverProvider ResolverProvider) (GraphQL, error) {
	graphqlSchema, err := buildGraphqlSchema(dbSchema, peers)

	if err != nil {
		return nil, err
	}

	return &graphQL{
		schema:           graphqlSchema,
		resolverProvider: resolverProvider,
	}, nil
}

func (g *graphQL) Resolve(query string, operationName string, variables map[string]interface{}, context context.Context) *graphql.Result {
	if g.resolverProvider == nil {
		panic("Empty resolver provider")
	}

	resolver := g.resolverProvider.GetResolver()
	networkResolver := g.resolverProvider.GetNetworkResolver()
	defer resolver.Close()

	return graphql.Do(graphql.Params{
		Schema: g.schema,
		RootObject: map[string]interface{}{
			"Resolver":        resolver,
			"NetworkResolver": networkResolver,
		},
		RequestString:  query,
		OperationName:  operationName,
		VariableValues: variables,
		Context:        context,
	})
}

func buildGraphqlSchema(dbSchema *schema.Schema, peers []libnetwork.Peer) (graphql.Schema, error) {
	localSchema, err := local.Build(dbSchema)
	if err != nil {
		return graphql.Schema{}, err
	}

	networkSchema, err := network.Build(dbSchema, peers)
	if err != nil {
		return graphql.Schema{}, err
	}

	schemaObject := graphql.ObjectConfig{
		Name:        "WeaviateObj",
		Description: "Location of the root query",
		Fields: graphql.Fields{
			"Local":   localSchema,
			"Network": networkSchema,
		},
	}

	// Run grahpql.NewSchema in a sub-closure, so that we can recover from panics.
	// We need to use panics to return errors deep inside the dynamic generation of the GraphQL schema,
	// inside the FieldThunks. There is _no_ way to bubble up an error besides panicking.
	var result graphql.Schema
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()

		result, err = graphql.NewSchema(graphql.SchemaConfig{
			Query: graphql.NewObject(schemaObject),
		})
	}()

	if err != nil {
		return graphql.Schema{}, fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}

	return result, nil
}
