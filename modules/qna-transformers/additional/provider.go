//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package additional

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
)

type AdditionalProperty interface {
	AdditionalPropertyFn(ctx context.Context,
		in []search.Result, params interface{}, limit *int,
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
	ExtractAdditionalFn(param []*ast.Argument) interface{}
	AdditonalPropertyDefaultValue() interface{}
	AdditionalFieldFn(classname string) *graphql.Field
}

type GraphQLAdditionalArgumentsProvider struct {
	answerProvider AdditionalProperty
}

func New(answerProvider AdditionalProperty) *GraphQLAdditionalArgumentsProvider {
	return &GraphQLAdditionalArgumentsProvider{answerProvider}
}

func (p *GraphQLAdditionalArgumentsProvider) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["answer"] = p.getAnswer()
	return additionalProperties
}

func (p *GraphQLAdditionalArgumentsProvider) getAnswer() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		GraphQLNames:           []string{"answer"},
		GraphQLFieldFunction:   p.answerProvider.AdditionalFieldFn,
		GraphQLExtractFunction: p.answerProvider.ExtractAdditionalFn,
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet:  p.answerProvider.AdditionalPropertyFn,
			ExploreList: p.answerProvider.AdditionalPropertyFn,
		},
	}
}
