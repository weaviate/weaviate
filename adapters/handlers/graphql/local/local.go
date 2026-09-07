//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package local

import (
	"github.com/sirupsen/logrus"
	"github.com/tailor-platform/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/aggregate"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/explore"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/get"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

// Build the local queries from the database schema.
func Build(dbSchema *schema.SchemaWithAliases, logger logrus.FieldLogger,
	config config.Config, modulesProvider *modules.Provider, authorizer authorization.Authorizer,
) (graphql.Fields, error) {
	// Constructed once and shared between Get and Aggregate: a GraphQL schema
	// can only have one type definition per name, so both builders must use
	// this exact instance rather than each building their own "FusionEnum".
	fusionEnum := common_filters.NewFusionEnum()

	getField, err := get.Build(dbSchema, logger, modulesProvider, authorizer, fusionEnum)
	if err != nil {
		return nil, err
	}

	aggregateField, err := aggregate.Build(dbSchema, config, modulesProvider, authorizer, fusionEnum)
	if err != nil {
		return nil, err
	}

	if modulesProvider.HasMultipleVectorizers() {
		localFields := graphql.Fields{
			"Get":       getField,
			"Aggregate": aggregateField,
		}

		return localFields, nil
	}

	exploreField := explore.Build(dbSchema.Objects, modulesProvider, authorizer)

	localFields := graphql.Fields{
		"Get":       getField,
		"Aggregate": aggregateField,
		"Explore":   exploreField,
	}

	return localFields, nil
}
