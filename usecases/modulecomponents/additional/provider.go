//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package additional

import (
	"context"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	generativegenerate "github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

type generativeClient interface {
	GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)
	GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)
	Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error)
}

type AdditionalProperty interface {
	AdditionalPropertyFn(ctx context.Context,
		in []search.Result, params interface{}, limit *int,
		argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig) ([]search.Result, error)
	ExtractAdditionalFn(param []*ast.Argument) interface{}
	AdditionalPropertyDefaultValue() interface{}
	AdditionalFieldFn(classname string) *graphql.Field
}

type GraphQLAdditionalArgumentsProvider struct {
	generateProvider AdditionalProperty
}

func NewGenerativeProvider(client generativeClient) *GraphQLAdditionalArgumentsProvider {
	return &GraphQLAdditionalArgumentsProvider{generativegenerate.New(client)}
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["generate"] = p.getGenerate()
	return additionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) getGenerate() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{"generate"},
		GraphQLFieldFunction:   p.generateProvider.AdditionalFieldFn,
		GraphQLExtractFunction: p.generateProvider.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.generateProvider.AdditionalPropertyFn,
			ExploreList: p.generateProvider.AdditionalPropertyFn,
		},
	}
}
