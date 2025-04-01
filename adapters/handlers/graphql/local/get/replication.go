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

package get

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
)

func replicationEnabled(class *models.Class) bool {
	return class.ReplicationConfig != nil && class.ReplicationConfig.Factor > 1
}

func consistencyLevelArgument(class *models.Class) *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Description: descriptions.ConsistencyLevel,
		Type: graphql.NewEnum(graphql.EnumConfig{
			Name: fmt.Sprintf("%sConsistencyLevelEnum", class.Class),
			Values: graphql.EnumValueConfigMap{
				string(types.ConsistencyLevelOne):    &graphql.EnumValueConfig{},
				string(types.ConsistencyLevelQuorum): &graphql.EnumValueConfig{},
				string(types.ConsistencyLevelAll):    &graphql.EnumValueConfig{},
			},
		}),
	}
}
