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

package nearAudio

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func getNearAudioArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearAudioArgument("GetObjects", classname)
}

func exploreNearAudioArgumentFn() *graphql.ArgumentConfig {
	return nearAudioArgument("Explore", "")
}

func aggregateNearAudioArgumentFn(classname string) *graphql.ArgumentConfig {
	return nearAudioArgument("Aggregate", classname)
}

func nearAudioArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("Multi2VecBind%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNearAudioInpObj", prefixName),
				Fields:      nearAudioFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func nearAudioFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"audio": &graphql.InputObjectFieldConfig{
			Description: "Base64 encoded audio file",
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
