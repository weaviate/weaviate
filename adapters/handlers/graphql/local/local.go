//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package local

import (
	"github.com/sirupsen/logrus"
	"github.com/tailor-inc/graphql"
	"github.com/liutizhong/weaviate/adapters/handlers/graphql/local/aggregate"
	"github.com/liutizhong/weaviate/adapters/handlers/graphql/local/explore"
	"github.com/liutizhong/weaviate/adapters/handlers/graphql/local/get"
	"github.com/liutizhong/weaviate/entities/schema"
	"github.com/liutizhong/weaviate/usecases/auth/authorization"
	"github.com/liutizhong/weaviate/usecases/config"
	"github.com/liutizhong/weaviate/usecases/modules"
)

// Build the local queries from the database schema.
func Build(dbSchema *schema.Schema, logger logrus.FieldLogger,
	config config.Config, modulesProvider *modules.Provider, authorizer authorization.Authorizer,
) (graphql.Fields, error) {
	getField, err := get.Build(dbSchema, logger, modulesProvider, authorizer)
	if err != nil {
		return nil, err
	}

	aggregateField, err := aggregate.Build(dbSchema, config, modulesProvider, authorizer)
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
