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

package ask

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func (g *GraphQLArgumentsProvider) getAskArgumentFn(classname string) *graphql.ArgumentConfig {
	return g.askArgument("GetObjects", classname)
}

func (g *GraphQLArgumentsProvider) exploreAskArgumentFn() *graphql.ArgumentConfig {
	return g.askArgument("Explore", "")
}

func (g *GraphQLArgumentsProvider) aggregateAskArgumentFn(classname string) *graphql.ArgumentConfig {
	return g.askArgument("Aggregate", classname)
}

func (g *GraphQLArgumentsProvider) askArgument(prefix, className string) *graphql.ArgumentConfig {
	prefixName := fmt.Sprintf("QnATransformers%s%s", prefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sAskInpObj", prefixName),
				Fields:      g.askFields(prefixName),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func (g *GraphQLArgumentsProvider) askFields(prefix string) graphql.InputObjectConfigFieldMap {
	askFields := graphql.InputObjectConfigFieldMap{
		"question": &graphql.InputObjectFieldConfig{
			Description: "Question to be answered",
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
		"properties": &graphql.InputObjectFieldConfig{
			Description: "Properties which contains text",
			Type:        graphql.NewList(graphql.String),
		},
		"rerank": &graphql.InputObjectFieldConfig{
			Description: "Arranges the results by certainty",
			Type:        graphql.Boolean,
		},
	}
	if g.askTransformer != nil {
		askFields["autocorrect"] = &graphql.InputObjectFieldConfig{
			Description: "Autocorrect input text values",
			Type:        graphql.Boolean,
		}
	}
	return askFields
}
