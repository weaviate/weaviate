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

package ask

import (
	"context"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generictypes"
)

type vectorFromAskParam struct {
	nearTextDep modulecapabilities.Dependency[[]float32]
}

func (s *vectorFromAskParam) vectorForAskParamFn(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn[[]float32],
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromAskParam(ctx, params.(*AskParams), className, findVectorFn, cfg)
}

func (s *vectorFromAskParam) vectorFromAskParam(ctx context.Context,
	params *AskParams, className string,
	findVectorFn modulecapabilities.FindVectorFn[[]float32],
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	arg := s.nearTextDep.GraphQLArgument()

	rawNearTextParam := map[string]interface{}{}
	rawNearTextParam["concepts"] = []interface{}{params.Question}

	nearTextParam, _, _ := arg.ExtractFunction(rawNearTextParam)
	vectorSearchFn := s.nearTextDep.VectorSearch()

	return vectorSearchFn.VectorForParams(ctx, nearTextParam, className, findVectorFn, cfg)
}

type Searcher struct {
	// nearText modules dependencies
	nearTextDeps []modulecapabilities.Dependency[[]float32]
}

func NewSearcher(nearTextDeps []modulecapabilities.Dependency[[]float32]) *Searcher {
	return &Searcher{nearTextDeps}
}

func (s *Searcher) VectorSearches() map[string]map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearchers := map[string]map[string]modulecapabilities.VectorForParams[[]float32]{}
	for _, nearTextDep := range s.nearTextDeps {
		vectorSearchers[nearTextDep.ModuleName()] = s.vectorSearches(nearTextDep)
	}
	return vectorSearchers
}

func (s *Searcher) vectorSearches(nearTextDep modulecapabilities.Dependency[[]float32]) map[string]modulecapabilities.VectorForParams[[]float32] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[[]float32]{}
	vectorFromAsk := &vectorFromAskParam{nearTextDep}
	vectorSearches["ask"] = generictypes.VectorForParams(vectorFromAsk.vectorForAskParamFn)
	return vectorSearches
}
