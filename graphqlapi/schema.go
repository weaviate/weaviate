/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

// Package graphqlapi provides the graphql endpoint for Weaviate
package graphqlapi

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
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
	networkPeers     peers.Peers
	requestsLog      *telemetry.RequestsLog
	config           config.Config
}

// Construct a GraphQL API from the database schema, and resolver interface.
func Build(dbSchema *schema.Schema, peers peers.Peers, resolverProvider ResolverProvider, logger *messages.Messaging,
	config config.Config) (GraphQL, error) {
	graphqlSchema, err := buildGraphqlSchema(dbSchema, peers, logger, config)

	if err != nil {
		return nil, err
	}

	return &graphQL{
		schema:           graphqlSchema,
		resolverProvider: resolverProvider,
		networkPeers:     peers,
		config:           config,
	}, nil
}

func (g *graphQL) Resolve(query string, operationName string, variables map[string]interface{}, context context.Context) *graphql.Result {
	if g.resolverProvider == nil {
		panic("Empty resolver provider")
	}

	resolver, err := g.resolverProvider.GetResolver()
	if err != nil {
		panic(err)
	}
	defer resolver.Close()

	networkResolver := g.resolverProvider.GetNetworkResolver()

	contextionary := g.resolverProvider.GetContextionary()

	requestsLog := g.resolverProvider.GetRequestsLog()

	return graphql.Do(graphql.Params{
		Schema: g.schema,
		RootObject: map[string]interface{}{
			"Resolver":        resolver,
			"NetworkResolver": networkResolver,
			"NetworkPeers":    g.networkPeers,
			"Contextionary":   contextionary,
			"RequestsLog":     requestsLog,
			"Config":          g.config,
		},
		RequestString:  query,
		OperationName:  operationName,
		VariableValues: variables,
		Context:        context,
	})
}

func buildGraphqlSchema(dbSchema *schema.Schema, peers peers.Peers, logger *messages.Messaging,
	config config.Config) (graphql.Schema, error) {
	localSchema, err := local.Build(dbSchema, peers, logger, config)
	if err != nil {
		return graphql.Schema{}, err
	}

	networkSchema, err := network.Build(peers, config)
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
				err = fmt.Errorf("%v at %s", r, debug.Stack())
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
