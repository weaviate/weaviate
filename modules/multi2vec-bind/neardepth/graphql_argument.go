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

package neardepth

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearDepthArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearDepthArgument("GetObjects", classname)
}

func exploreNearDepthArgumentFn() *graphql.ArgumentConfig {
	return nearDepthArgument("Explore", "")
}

func aggregateNearDepthArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearDepthArgument("Aggregate", classname)
}

func nearDepthArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Multi2VecBind%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearDepthInpObj", prefixName),
				Fields:      nearDepthFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearDepthFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"depth": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded Depth data",
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
