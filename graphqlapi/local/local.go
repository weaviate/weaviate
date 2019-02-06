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
package local

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/aggregate"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/graphql-go/graphql"
)

// Build the local queries from the database schema.
func Build(dbSchema *schema.Schema, peers peers.Peers, logger *messages.Messaging) (*graphql.Field, error) {
	getField, err := get.Build(dbSchema, peers, logger)
	if err != nil {
		return nil, err
	}
	getMetaField, err := getmeta.Build(dbSchema)
	if err != nil {
		return nil, err
	}
	getAggregateField, err := aggregate.Build(dbSchema)
	if err != nil {
		return nil, err
	}
	fetchField := fetch.Build()

	localFields := graphql.Fields{
		"Get":       getField,
		"GetMeta":   getMetaField,
		"Aggregate": getAggregateField,
		"Fetch":     fetchField,
	}

	localObject := graphql.NewObject(graphql.ObjectConfig{
		Name:        "WeaviateLocalObj",
		Fields:      localFields,
		Description: descriptions.LocalObj,
	})

	localField := graphql.Field{
		Type:        localObject,
		Description: descriptions.WeaviateLocal,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// This step does nothing; all ways allow the resolver to continue
			return p.Source, nil
		},
	}

	return &localField, nil
}
