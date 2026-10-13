//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rank

import (
	"context"
	"errors"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/language/ast"
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
	// decisionDefaults is set for a decisions module, see DecisionDefaults.
	decisionDefaults DecisionDefaults
}

func New(reranker ReRankerClient) *ReRankerProvider {
	return &ReRankerProvider{client: reranker}
}

// NewForDecisions is the provider of a decisions module: the score is a
// probability, the results below the threshold are dropped and the order is
// the search's or the probability's, from the request or the class settings.
func NewForDecisions(reranker ReRankerClient, defaults DecisionDefaults) *ReRankerProvider {
	return &ReRankerProvider{client: reranker, decisionDefaults: defaults}
}

func (p *ReRankerProvider) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (p *ReRankerProvider) ExtractAdditionalFn(param []*ast.Argument, class *models.Class) interface{} {
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
