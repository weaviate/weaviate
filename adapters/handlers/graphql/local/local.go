/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package local

import (
	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/aggregate"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/fetch"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/getmeta"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

// Build the local queries from the database schema.
func Build(dbSchema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger,
	config config.Config) (*graphql.Field, error) {
	getField, err := get.Build(dbSchema, peers, logger)
	if err != nil {
		return nil, err
	}
	getMetaField, err := getmeta.Build(dbSchema, config)
	if err != nil {
		return nil, err
	}
	aggregateField, err := aggregate.Build(dbSchema, config)
	if err != nil {
		return nil, err
	}
	fetchField := fetch.Build()

	localFields := graphql.Fields{
		"Get":       getField,
		"GetMeta":   getMetaField,
		"Aggregate": aggregateField,
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
