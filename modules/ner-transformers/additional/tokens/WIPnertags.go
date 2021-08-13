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

package tokens

import (
	"context"
	"errors"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/modules/ner-transformers/ent"
)

type Params struct{}

// type nerClient interface {
// 	Tokens(ctx context.Context,
// 		text, question string) (*ent.TokensResult, error)
// }

// type paramsHelper interface {
// 	GetQuestion(params interface{}) string
// 	GetProperties(params interface{}) []string
// 	GetCertainty(params interface{}) float64
// }

// type TokensProvider struct {
// 	qna qnaClient
// 	paramsHelper
// }

// func New(qna qnaClient, paramsHelper paramsHelper) *TokensProvider {
// 	return &TokensProvider{qna, paramsHelper}
// }

// func (p *TokensProvider) AdditonalPropertyDefaultValue() interface{} {
// 	return &Params{}
// }

// func (p *TokensProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
// 	return &Params{}
// }

// func (p *TokensProvider) AdditionalFieldFn(classname string) *graphql.Field {
// 	return p.additionalTokensField(classname)
// }

// func (p *TokensProvider) AdditionalPropertyFn(ctx context.Context,
// 	in []search.Result, params interface{}, limit *int,
// 	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
// 	if parameters, ok := params.(*Params); ok {
// 		return p.findTokens(ctx, in, parameters, limit, argumentModuleParams)
// 	}
// 	return nil, errors.New("wrong parameters")
// }
