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
	"github.com/creativesoftwarefdn/weaviate/config"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/graphql-go/graphql"
)

// GraphQL stores all relevant variables and functions
type GraphQL struct {
	weaviateGraphQLSchema graphql.Schema
	serverConfig          *config.WeaviateConfig
	databaseSchema        *schema.WeaviateSchema
	dbConnector           *dbconnector.DatabaseConnector
	messaging             *messages.Messaging
}

// Schema is called by the RestAPI handler to receive the schema.
func (g *GraphQL) Schema() *graphql.Schema {
	return &g.weaviateGraphQLSchema
}

// CreateSchema initializes the Graphl
func CreateSchema(dbConnector *dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig, databaseSchema *schema.WeaviateSchema, messaging *messages.Messaging) (GraphQL, error) {
	messaging.InfoMessage("Creating GraphQL schema...")
	var g GraphQL

	// Store for later use.
	g.dbConnector = dbConnector
	g.serverConfig = serverConfig
	g.databaseSchema = databaseSchema
	g.messaging = messaging

	// Now build the graphql schema
	err := g.buildGraphqlSchema()
	return g, err
}
