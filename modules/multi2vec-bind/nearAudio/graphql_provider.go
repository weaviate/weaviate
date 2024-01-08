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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/nearAudio"
)

type GraphQLArgumentsProvider struct{}

func New() *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments["nearAudio"] = g.getNearAudio()
	return arguments
}

func (g *GraphQLArgumentsProvider) getNearAudio() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:       getNearAudioArgumentFn,
		AggregateArgumentsFunction: aggregateNearAudioArgumentFn,
		ExploreArgumentsFunction:   exploreNearAudioArgumentFn,
		ExtractFunction:            extractNearAudioFn,
		ValidateFunction:           nearAudio.ValidateNearAudioFn,
	}
}
