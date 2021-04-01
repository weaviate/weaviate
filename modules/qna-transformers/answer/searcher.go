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

	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type Searcher struct {
	// here goes the module dependency
	nearTextDep modulecapabilities.Dependency
}

func NewSearcher(nearTextDep modulecapabilities.Dependency) *Searcher {
	return &Searcher{nearTextDep}
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["answer"] = s.vectorForAnswerParam
	return vectorSearches
}

func (s *Searcher) vectorForAnswerParam(ctx context.Context, params interface{},
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig) ([]float32, error) {
	return s.vectorFromAnswerParam(ctx, params.(*AnswerParams), findVectorFn, cfg)
}

func (s *Searcher) vectorFromAnswerParam(ctx context.Context,
	params *AnswerParams, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig) ([]float32, error) {
	arg := s.nearTextDep.GraphQLArgument()

	rawNearTextParam := map[string]interface{}{}
	rawNearTextParam["concepts"] = []interface{}{params.Question}
	rawNearTextParam["certainty"] = params.Certainty
	rawNearTextParam["limit"] = params.Limit

	nearTextParam := arg.ExtractFunction(rawNearTextParam)
	vectorSearchFn := s.nearTextDep.VectorSearch()

	return vectorSearchFn(ctx, nearTextParam, findVectorFn, cfg)
}
