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

package nearthermal

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearThermalArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearThermalArgument("GetObjects", classname)
}

func exploreNearThermalArgumentFn() *graphql.ArgumentConfig {
	return nearThermalArgument("Explore", "")
}

func aggregateNearThermalArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearThermalArgument("Aggregate", classname)
}

func nearThermalArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Multi2VecBind%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearThermalInpObj", prefixName),
				Fields:      nearThermalFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearThermalFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"thermal": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded thermal data",
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
