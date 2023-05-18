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

package generate

import (
	"context"
	"errors"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/generative-cohere/ent"
)

const maximumNumberOfGoroutines = 10

type cohereClient interface {
	GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error)
	GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error)
	Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*ent.GenerateResult, error)
}

type GenerateProvider struct {
	client                    cohereClient
	maximumNumberOfGoroutines int
}

func New(cohere cohereClient) *GenerateProvider {
	return &GenerateProvider{cohere, maximumNumberOfGoroutines}
}

func (p *GenerateProvider) AdditonalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *GenerateProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return p.parseGenerateArguments(param)
}

func (p *GenerateProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalGenerateField(classname)
}

func (p *GenerateProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.generateResult(ctx, in, parameters, limit, argumentModuleParams, cfg)
	}
	return nil, errors.New("wrong parameters")
}
