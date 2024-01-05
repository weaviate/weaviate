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

package summary

import (
	"context"
	"errors"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/sum-transformers/ent"
)

type sumClient interface {
	GetSummary(ctx context.Context, property, text string) ([]ent.SummaryResult, error)
}

type SummaryProvider struct {
	sum sumClient
}

func New(sum sumClient) *SummaryProvider {
	return &SummaryProvider{sum}
}

func (p *SummaryProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *SummaryProvider) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return p.parseSummaryArguments(param)
}

func (p *SummaryProvider) AdditionalFieldFn(classname string) *graphql.Field {
	return p.additionalSummaryField(classname)
}

func (p *SummaryProvider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return p.findSummary(ctx, in, parameters)
	}
	return nil, errors.New("wrong parameters")
}
