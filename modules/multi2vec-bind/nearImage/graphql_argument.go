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

package nearImage

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearImageArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearImageArgument("GetObjects", classname)
}

func exploreNearImageArgumentFn() *graphql.ArgumentConfig {
	return nearImageArgument("Explore", "")
}

func aggregateNearImageArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearImageArgument("Aggregate", classname)
}

func nearImageArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Multi2VecBind%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearImageInpObj", prefixName),
				Fields:      nearImageFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearImageFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"image": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded image",
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
