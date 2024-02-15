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

package nearImu

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearIMUArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearIMUArgument("GetObjects", classname)
}

func exploreNearIMUArgumentFn() *graphql.ArgumentConfig {
	return nearIMUArgument("Explore", "")
}

func aggregateNearIMUArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearIMUArgument("Aggregate", classname)
}

func nearIMUArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Multi2VecBind%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearIMUInpObj", prefixName),
				Fields:      nearIMUFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearIMUFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"imu": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded IMU data",
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
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
	}
}
