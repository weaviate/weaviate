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

package additional

import (
	"context"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	rankerrank "github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

type reRankerClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
}

type GraphQLAdditionalRankerProvider struct {
	ReRankerProvider AdditionalProperty
}

func NewRankerProvider(client reRankerClient) *GraphQLAdditionalRankerProvider {
	return &GraphQLAdditionalRankerProvider{rankerrank.New(client)}
}

func (p *GraphQLAdditionalRankerProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["rerank"] = p.getReRanker()
	return additionalProperties
}

func (p *GraphQLAdditionalRankerProvider) getReRanker() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{"rerank"},
		GraphQLFieldFunction:   p.ReRankerProvider.AdditionalFieldFn,
		GraphQLExtractFunction: p.ReRankerProvider.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.ReRankerProvider.AdditionalPropertyFn,
			ExploreList: p.ReRankerProvider.AdditionalPropertyFn,
		},
	}
}
