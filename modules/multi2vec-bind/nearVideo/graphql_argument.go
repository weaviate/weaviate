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

package nearVideo

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearVideoArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearVideoArgument("GetObjects", classname)
}

func exploreNearVideoArgumentFn() *graphql.ArgumentConfig {
	return nearVideoArgument("Explore", "")
}

func aggregateNearVideoArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearVideoArgument("Aggregate", classname)
}

func nearVideoArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Multi2VecBind%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearVideoInpObj", prefixName),
				Fields:      nearVideoFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearVideoFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"video": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded video",
			Type:        graphql.NewNonNull(graphql.String),
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
	}
}
