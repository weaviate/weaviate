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

package rank

import (
	"context"
	"errors"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// const maximumNumberOfGoroutines = 10
type ReRankerClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
}

type ReRankerProvider struct {
	client ReRankerClient
}

func New(reranker ReRankerClient) *ReRankerProvider {
	return &ReRankerProvider{reranker}
}

func (p *ReRankerProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *ReRankerProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return p.parseReRankerArguments(param)
}

func (p *ReRankerProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalReRankerField(classname)
}

func (p *ReRankerProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.getScore(ctx, cfg, in, parameters)
	}
	return nil, errors.New("wrong parameters")
}
