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

package neartext

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

type GraphQLArgumentsProvider struct {
	nearTextTransformer modulecapabilities.TextTransform
}

func New(nearTextTransformer modulecapabilities.TextTransform) *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{nearTextTransformer}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments["nearText"] = g.getNearText()
	return arguments
}

func (g *GraphQLArgumentsProvider) getNearText() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:       g.getNearTextArgumentFn,
		AggregateArgumentsFunction: g.aggregateNearTextArgumentFn,
		ExtractFunction:            g.extractNearTextFn,
		ValidateFunction:           g.validateNearTextFn,
	}
}
