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

// Package graphql provides the graphql endpoint for Weaviate
package graphql

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

type Traverser interface {
	local.Resolver
}

type NetworkTraverser interface {
	network.Resolver
}

type RequestsLogger interface {
	get.RequestsLog
}

// The communication interface between the REST API and the GraphQL API.
type GraphQL interface {
	// Resolve the GraphQL query in 'query'.
	Resolve(context context.Context, query string, operationName string, variables map[string]interface{}) *graphql.Result
}

type graphQL struct {
	schema           graphql.Schema
	traverser        Traverser
	networkTraverser NetworkTraverser
	requestsLogger   RequestsLogger
	networkPeers     peers.Peers
	config           config.Config
}

// Construct a GraphQL API from the database schema, and resolver interface.
func Build(schema *schema.Schema, peers peers.Peers, traverser Traverser,
	networkTraverser NetworkTraverser, requestsLogger RequestsLogger,
	logger logrus.FieldLogger, config config.Config) (GraphQL, error) {

	logger.WithField("action", "graphql_rebuild").
		WithField("peers", peers).
		WithField("schema", schema).
		Debug("rebuilding the graphql schema")

	graphqlSchema, err := buildGraphqlSchema(schema, peers, logger, config)
	if err != nil {
		return nil, err
	}

	return &graphQL{
		schema:           graphqlSchema,
		traverser:        traverser,
		networkTraverser: networkTraverser,
		requestsLogger:   requestsLogger,
		networkPeers:     peers,
		config:           config,
	}, nil
}

// Resolve at query time
func (g *graphQL) Resolve(context context.Context, query string, operationName string, variables map[string]interface{}) *graphql.Result {
	return graphql.Do(graphql.Params{
		Schema: g.schema,
		RootObject: map[string]interface{}{
			"Resolver":        g.traverser,
			"NetworkResolver": g.networkTraverser,
			"NetworkPeers":    g.networkPeers,
			"RequestsLog":     g.requestsLogger,
			"Config":          g.config,
		},
		RequestString:  query,
		OperationName:  operationName,
		VariableValues: variables,
		Context:        context,
	})
}

func buildGraphqlSchema(dbSchema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger,
	config config.Config) (graphql.Schema, error) {
	localSchema, err := local.Build(dbSchema, peers, logger, config)
	if err != nil {
		return graphql.Schema{}, err
	}

	schemaObject := graphql.ObjectConfig{
		Name:        "WeaviateObj",
		Description: "Location of the root query",
		Fields:      localSchema,
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
