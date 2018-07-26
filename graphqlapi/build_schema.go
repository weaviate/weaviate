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
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Build the GraphQL schema based on
// 1) the static query structure (e.g. LocalFetch)
// 2) the (dynamic) database schema from Weaviate

func (g *GraphQL) buildGraphqlSchema() error {
	local_field, err := g.buildLocalField()

	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	}

	var root_fields = graphql.Fields{
		"Local": local_field,
		// "Network" : etc
	}

	rootQuery := graphql.ObjectConfig{Name: "WeaviateObj", Fields: root_fields}

	g.weaviateGraphQLSchema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(rootQuery),
	})

	if err != nil {
		return fmt.Errorf("Could not build GraphQL schema, because: %v", err)
	} else {
		return nil
	}
}

func (g *GraphQL) buildLocalField() (*graphql.Field, error) {
	action_class_fields, err := g.buildExampleActionClassFields()

	if err != nil {
		return nil, err
	}

	local_fields := graphql.Fields{
		"ConvertedFetch": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
		"ActionClasses": &graphql.Field{
			Type: graphql.NewObject(graphql.ObjectConfig{Name: "SampleActionClass", Fields: action_class_fields}),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return nil, fmt.Errorf("Not supported")
			},
		},
	}

	local_object := graphql.ObjectConfig{Name: "WeaviateLocal", Fields: local_fields}
	field := graphql.Field{
		Type: graphql.NewObject(local_object),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}

	return &field, nil
}

// EXAMPLE: How to iterate through the
func (g *GraphQL) buildExampleActionClassFields() (graphql.Fields, error) {
	fields := graphql.Fields{}

	for _, class := range g.databaseSchema.ActionSchema.Schema.Classes {
		field, err := buildExampleActionClassField(class)
		if err != nil {
			return nil, err
		} else {
			fields[class.Class] = field
		}
	}

	return fields, nil
}

func buildExampleActionClassField(class *models.SemanticSchemaClass) (*graphql.Field, error) {
	return &graphql.Field{
		Type: graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Not supported")
		},
	}, nil
}
