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
	"github.com/graphql-go/graphql"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// GraphQLSchema has some basic variables.
type GraphQLSchema struct {
	weaviateGraphQLSchema graphql.Schema
	serverConfig          *config.WeaviateConfig
	serverSchema          *schema.WeaviateSchema
	dbConnector           dbconnector.DatabaseConnector
	usedKey               *models.KeyGetResponse
	usedKeyToken          *models.KeyTokenGetResponse
	messaging             *messages.Messaging
}

// GetGraphQLSchema returns the schema if it is set
func (f *GraphQLSchema) GetGraphQLSchema() (graphql.Schema, error) {

	rootQuery := graphql.ObjectConfig{Name: "Weaviate", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	schema, err := graphql.NewSchema(schemaConfig)

	return schema, err
}
