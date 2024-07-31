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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

const Name = "nearImage"

type GraphQLArgumentsProvider struct{}

func New() *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments[Name] = g.getNearImage()
	return arguments
}

func (g *GraphQLArgumentsProvider) getNearImage() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:       getNearImageArgumentFn,
		AggregateArgumentsFunction: aggregateNearImageArgumentFn,
		ExploreArgumentsFunction:   exploreNearImageArgumentFn,
		ExtractFunction:            extractNearImageFn,
		ValidateFunction:           ValidateNearImageFn,
	}
}
