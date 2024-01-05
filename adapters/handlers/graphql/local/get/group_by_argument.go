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
)

func groupByArgument(className string) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("GetObjects%s", className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sGroupByInpObj", prefix),
				Fields:      groupByFields(prefix),
				Description: descriptions.GroupByFilter,
			},
		),
	}
}

func groupByFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"path": &graphql.InputObjectFieldConfig{
			Description: descriptions.GroupByPath,
			Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"groups": &graphql.InputObjectFieldConfig{
			Description: descriptions.GroupByGroups,
			Type:        graphql.NewNonNull(graphql.Int),
		},
		"objectsPerGroup": &graphql.InputObjectFieldConfig{
			Description: descriptions.GroupByObjectsPerGroup,
			Type:        graphql.NewNonNull(graphql.Int),
		},
	}
}
