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

package tokens

import (
	"context"
	"errors"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/ner-transformers/ent"
)

type nerClient interface {
	GetTokens(ctx context.Context, property, text string) ([]ent.TokenResult, error)
}

type TokenProvider struct {
	ner nerClient
}

func New(ner nerClient) *TokenProvider {
	return &TokenProvider{ner}
}

func (p *TokenProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *TokenProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return p.parseTokenArguments(param)
}

func (p *TokenProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalTokensField(classname)
}

func (p *TokenProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.findTokens(ctx, in, parameters)
	}
	return nil, errors.New("wrong parameters")
}
