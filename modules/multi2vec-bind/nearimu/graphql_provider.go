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

package nearimu

import (
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

type GraphQLArgumentsProvider struct{}

func New() *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments["nearIMU"] = g.getNearIMU()
	return arguments
}

func (g *GraphQLArgumentsProvider) getNearIMU() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:       getNearIMUArgumentFn,
		AggregateArgumentsFunction: aggregateNearIMUArgumentFn,
		ExploreArgumentsFunction:   exploreNearIMUArgumentFn,
		ExtractFunction:            extractNearIMUFn,
		ValidateFunction:           validateNearIMUFn,
	}
}
