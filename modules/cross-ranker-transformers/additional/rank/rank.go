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

package rank

import (
	"context"
	"errors"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/cross-ranker-transformers/ent"
)

//const maximumNumberOfGoroutines = 10

type CrossRankerClient interface {
	Rank(ctx context.Context, property string, query string) (*ent.RankResult, error)
}

type CrossRankerProvider struct {
	client CrossRankerClient
}

func New(crossranker CrossRankerClient) *CrossRankerProvider {
	return &CrossRankerProvider{crossranker}
}

func (p *CrossRankerProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *CrossRankerProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return p.parseCrossRankerArguments(param)
}

func (p *CrossRankerProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalCrossRankerField(classname)
}

func (p *CrossRankerProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.getScore(ctx, in, parameters)
	}
	return nil, errors.New("wrong parameters")
}
