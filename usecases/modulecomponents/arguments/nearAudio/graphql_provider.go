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
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
)

const Name = "nearAudio"

type GraphQLArgumentsProvider struct{}

func New() *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments[Name] = g.getNearAudio()
	return arguments
}

func (g *GraphQLArgumentsProvider) getNearAudio() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:       getNearAudioArgumentFn,
		AggregateArgumentsFunction: aggregateNearAudioArgumentFn,
		ExploreArgumentsFunction:   exploreNearAudioArgumentFn,
		ExtractFunction:            extractNearAudioFn,
		ValidateFunction:           validateNearAudioFn,
	}
}
