//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package neartext

import (
	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
)

type GraphQLArgumentsProvider struct{}

func New() *GraphQLArgumentsProvider {
	return &GraphQLArgumentsProvider{}
}

func (g *GraphQLArgumentsProvider) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	arguments["nearText"] = nearTextArgument("GetObjects", classname)
	return arguments
}

func (g *GraphQLArgumentsProvider) ExploreArguments() map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	arguments["nearText"] = nearTextArgument("Explore", "")
	return arguments
}

func (g *GraphQLArgumentsProvider) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	extractedFns := map[string]modulecapabilities.ExtractFn{}
	extractedFns["nearText"] = extractNearTextFn
	return extractedFns
}

func (g *GraphQLArgumentsProvider) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	validateFns := map[string]modulecapabilities.ValidateFn{}
	validateFns["nearText"] = validateNearText
	return validateFns
}
