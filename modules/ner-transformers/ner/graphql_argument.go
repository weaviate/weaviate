//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ask

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
)

func getNerArgumentFn(classname string) *graphql.ArgumentConfig {
	return nerArgument("GetObjects", classname)
}

func exploreNerArgumentFn() *graphql.ArgumentConfig {
	return nerArgument("Explore", "")
}

func nerArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("NERTransformers%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sNerInpObj", prefixName),
				Fields:      nerFields(prefixName),
				Description: "Named Entity Recognition filter field",
			},
		),
	}
}

func nerFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"limit": &graphql.InputObjectFieldConfig{
			Description: descriptions.Limit,
			Type:        graphql.Int,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"properties": &graphql.InputObjectFieldConfig{
			Description: "Properties which contains text",
			Type:        graphql.NewList(graphql.String),
		},
	}
}
