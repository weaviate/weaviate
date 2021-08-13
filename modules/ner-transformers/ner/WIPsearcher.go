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

package ask

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type Searcher struct {
	// nearText module dependency
	nearTextDep modulecapabilities.Dependency
}

func NewSearcher(nearTextDep modulecapabilities.Dependency) *Searcher {
	return &Searcher{nearTextDep}
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["ask"] = s.vectorForAskParam
	return vectorSearches
}

func (s *Searcher) vectorForAskParam(ctx context.Context, params interface{},
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig) ([]float32, error) {
	return s.vectorFromAskParam(ctx, params.(*AskParams), findVectorFn, cfg)
}

func (s *Searcher) vectorFromAskParam(ctx context.Context,
	params *AskParams, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig) ([]float32, error) {
	arg := s.nearTextDep.GraphQLArgument()

	rawNearTextParam := map[string]interface{}{}
	rawNearTextParam["concepts"] = []interface{}{params.Question}

	nearTextParam := arg.ExtractFunction(rawNearTextParam)
	vectorSearchFn := s.nearTextDep.VectorSearch()

	return vectorSearchFn(ctx, nearTextParam, findVectorFn, cfg)
}
