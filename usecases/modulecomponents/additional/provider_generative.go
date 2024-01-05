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
	generativegenerate "github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

type generativeClient interface {
	GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)
	GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)
	Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error)
}

type GraphQLAdditionalGenerativeProvider struct {
	generative AdditionalProperty
}

func NewGenerativeProvider(client generativeClient) *GraphQLAdditionalGenerativeProvider {
	return &GraphQLAdditionalGenerativeProvider{generativegenerate.New(client)}
}

func (p *GraphQLAdditionalGenerativeProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["generate"] = p.getGenerate()
	return additionalProperties
}

func (p *GraphQLAdditionalGenerativeProvider) getGenerate() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{"generate"},
		GraphQLFieldFunction:   p.generative.AdditionalFieldFn,
		GraphQLExtractFunction: p.generative.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.generative.AdditionalPropertyFn,
			ExploreList: p.generative.AdditionalPropertyFn,
		},
	}
}
