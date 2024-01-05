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

package answer

import (
	"context"
	"errors"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/qna-openai/ent"
)

type Params struct{}

type qnaClient interface {
	Answer(ctx context.Context, text, question string, cfg moduletools.ClassConfig) (*ent.AnswerResult, error)
}

type paramsHelper interface {
	GetQuestion(params interface{}) string
	GetProperties(params interface{}) []string
}

type AnswerProvider struct {
	qna qnaClient
	paramsHelper
}

func New(qna qnaClient, paramsHelper paramsHelper) *AnswerProvider {
	return &AnswerProvider{qna, paramsHelper}
}

func (p *AnswerProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *AnswerProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &Params{}
}

func (p *AnswerProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalAnswerField(classname)
}

func (p *AnswerProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.findAnswer(ctx, in, parameters, limit, argumentModuleParams, cfg)
	}
	return nil, errors.New("wrong parameters")
}
