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
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
)

type GraphQLArgumentsProvider struct{}

func New() *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{}
}

func (g *GraphQLArgumentsProvider) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	arguments["ask"] = g.getAsk()
	return arguments
}

func (g *GraphQLArgumentsProvider) getAsk() modulecapabilities.GraphQLArgument {
	return modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:     getAskArgumentFn,
		ExploreArgumentsFunction: exploreAskArgumentFn,
		ExtractFunction:          extractAskFn,
		ValidateFunction:         validateAskFn,
	}
}
