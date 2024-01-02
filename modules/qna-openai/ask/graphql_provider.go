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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

type GraphQLArgumentsProvider struct {
	askTransformer modulecapabilities.TextTransform
}

func New(askTransformer modulecapabilities.TextTransform) *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{askTransformer}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments["ask"] = g.getAsk()
	return arguments
}

func (g *GraphQLArgumentsProvider) getAsk() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:       g.getAskArgumentFn,
		AggregateArgumentsFunction: g.aggregateAskArgumentFn,
		ExploreArgumentsFunction:   g.exploreAskArgumentFn,
		ExtractFunction:            g.extractAskFn,
		ValidateFunction:           g.validateAskFn,
	}
}
