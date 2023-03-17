package get

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/replica"
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
				string(replica.One):    &graphql.EnumValueConfig{},
				string(replica.Quorum): &graphql.EnumValueConfig{},
				string(replica.All):    &graphql.EnumValueConfig{},
			},
		}),
	}
}
