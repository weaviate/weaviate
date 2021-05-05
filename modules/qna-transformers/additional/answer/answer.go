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

package answer

import (
	"context"
	"errors"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/modules/qna-transformers/ent"
)

type Params struct{}

type qnaClient interface {
	Answer(ctx context.Context,
		text, question string) (*ent.AnswerResult, error)
}

type paramsHelper interface {
	GetQuestion(params interface{}) string
	GetProperties(params interface{}) []string
	GetCertainty(params interface{}) float64
}

type AnswerProvider struct {
	qna qnaClient
	paramsHelper
}

func New(qna qnaClient, paramsHelper paramsHelper) *AnswerProvider {
	return &AnswerProvider{qna, paramsHelper}
}

func (p *AnswerProvider) AdditonalPropertyDefaultValue() interface{} {
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
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.findAnswer(ctx, in, parameters, limit, argumentModuleParams)
	}
	return nil, errors.New("wrong parameters")
}
