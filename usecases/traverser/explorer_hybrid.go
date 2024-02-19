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

package traverser

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

func sparseSearch(ctx context.Context, e *Explorer, params dto.GetParams) ([]*search.Result, string, error) {
	params.KeywordRanking = &searchparams.KeywordRanking{
		Query:      params.HybridSearch.Query,
		Type:       "bm25",
		Properties: params.HybridSearch.Properties,
	}

	if params.Pagination == nil {
		return nil, "", fmt.Errorf("invalid params, pagination object is nil")
	}

	totalLimit, err := e.CalculateTotalLimit(params.Pagination)
	if err != nil {
		return nil, "", err
	}

	enforcedMin := MaxInt(params.Pagination.Offset+hybrid.DefaultLimit, totalLimit)

	oldLimit := params.Pagination.Limit
	params.Pagination.Limit = enforcedMin - params.Pagination.Offset

	results, scores, err := e.searcher.SparseObjectSearch(ctx, params)
	if err != nil {
		return nil, "", err
	}
	params.Pagination.Limit = oldLimit

	out := make([]*search.Result, len(results))
	for i, obj := range results {
		sr := obj.SearchResultWithScore(additional.Properties{}, scores[i])
		sr.SecondarySortValue = sr.Score
		out[i] = &sr
	}

	return out, "keyword,dunno", nil
}

func denseSearch(ctx context.Context, vec []float32, targetVector string, e *Explorer, params dto.GetParams) ([]*search.Result, string, error) {
	/* FIXME
	baseSearchLimit := params.Pagination.Limit + params.Pagination.Offset
	var hybridSearchLimit int
	if baseSearchLimit <= hybrid.DefaultLimit {
		hybridSearchLimit = hybrid.DefaultLimit
	} else {
		hybridSearchLimit = baseSearchLimit
	}
	*/

	params.NearVector = &searchparams.NearVector{
		Vector:        vec,
		TargetVectors: []string{targetVector},
	}

	params.Pagination.Offset = 0
	if params.Pagination.Limit < hybrid.DefaultLimit {
		params.Pagination.Limit = hybrid.DefaultLimit
	}

	partial_results, vector, err := e.getClassVectorSearch(ctx, params)
	if err != nil {
		return nil, "", err
	}

	results, err := e.searchResultsToGetResponseWithType(ctx, partial_results, vector, params)
	if err != nil {
		return nil, "", err
	}

	out := make([]*search.Result, 0, len(results))
	for _, sr := range results {
		out_sr := sr
		out_sr.SecondarySortValue = 1 - sr.Dist
		out = append(out, &out_sr)
	}

	return out, "vector,nearVector", nil
}

func nearTextSubSearch(ctx context.Context, e *Explorer, params dto.GetParams) ([]*search.Result, string, error) {
	subSearchParams := params
	subSearchParams.ModuleParams["nearText"] = params.HybridSearch.NearTextParams
	subSearchParams.HybridSearch = nil
	partial_results, vector, err := e.getClassVectorSearch(ctx, subSearchParams)
	if err != nil {
		return nil, "", err
	}
	results, err := e.searchResultsToGetResponseWithType(ctx, partial_results, vector, params)
	if err != nil {
		return nil, "", err
	}

	var out []*search.Result
	for _, res := range results {
		sr := res
		sr.SecondarySortValue = 1 - sr.Dist
		out = append(out, &sr)
	}

	return out, "vector,nearText", nil
}

func (e *Explorer) Hybrid(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	var results [][]*search.Result
	var weights []float64
	var names []string

	origParams := params
	params.Pagination = &filters.Pagination{
		Limit:   params.Pagination.Limit,
		Offset:  params.Pagination.Offset,
		Autocut: params.Pagination.Autocut,
	}

	if (params.HybridSearch.Alpha) > 0 {
		if params.HybridSearch.NearTextParams != nil {
			res, name, err := nearTextSubSearch(ctx, e, params)
			if err != nil {
				e.logger.WithField("action", "hybrid").WithError(err).Error("nearTextSubSearch failed")
				return nil, err
			} else {
				weights = append(weights, params.HybridSearch.Alpha)
				results = append(results, res)
				names = append(names, name)
			}
		} else {
			sch := e.schemaGetter.GetSchemaSkipAuth()
			class := sch.FindClassByName(schema.ClassName(params.ClassName))
			if class == nil {
				return nil, fmt.Errorf("class %q not found", params.ClassName)
			}

			vectoriser := class.Vectorizer
			if vectoriser != "none" {
				if len(params.HybridSearch.Vector) == 0 {
					var err error
					params.SearchVector, err = e.modulesProvider.VectorFromInput(ctx, params.ClassName, params.HybridSearch.Query, params.HybridSearch.TargetVectors[0])
					if err != nil {
						return nil, err
					}
				} else {
					params.SearchVector = params.HybridSearch.Vector
				}

				res, name, err := denseSearch(ctx, params.SearchVector, origParams.HybridSearch.TargetVectors[0], e, params)
				if err != nil {
					e.logger.WithField("action", "hybrid").WithError(err).Error("denseSearch failed")
					return nil, err
				} else {
					weights = append(weights, 1-params.HybridSearch.Alpha)
					results = append(results, res)
					names = append(names, name)
				}
			}
		}
	}

	if params.HybridSearch.Alpha > 0 {
		sparseResults, name, err := sparseSearch(ctx, e, params)
		if err != nil {
			e.logger.WithField("action", "hybrid").WithError(err).Error("sparseSearch failed")
			return nil, err
		} else {
			weights = append(weights, 1arams.HybridSearch.Alpha)
			results = append(results, sparseResults)
			names = append(names, name)
		}
	}

	postProcess := func(results []*search.Result) ([]search.Result, error) {
		totalLimit, err := e.CalculateTotalLimit(origParams.Pagination)
		if err != nil {
			return nil, err
		}

		if len(results) > totalLimit {
			results = results[:totalLimit]
		}

		res1 := make([]search.Result, 0, len(results))
		for _, res := range results {
			res1 = append(res1, *res)
		}

		res, err := e.searcher.ResolveReferences(ctx, res1, origParams.Properties, nil, origParams.AdditionalProperties, origParams.Tenant)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	res, err := hybrid.Do(ctx, &hybrid.Params{
		HybridSearch: origParams.HybridSearch,
		Keyword:      origParams.KeywordRanking,
		Class:        origParams.ClassName,
		Autocut:      origParams.Pagination.Autocut,
	}, results, weights, names, e.logger, postProcess)
	if err != nil {
		return nil, err
	}

	var pointerResultList hybrid.Results

	if origParams.Pagination.Limit <= 0 {
		origParams.Pagination.Limit = hybrid.DefaultLimit
	}

	if origParams.Pagination.Offset < 0 {
		origParams.Pagination.Offset = 0
	}

	if len(res) >= origParams.Pagination.Limit+origParams.Pagination.Offset {
		pointerResultList = res[origParams.Pagination.Offset : origParams.Pagination.Limit+origParams.Pagination.Offset]
	}
	if len(res) < origParams.Pagination.Limit+origParams.Pagination.Offset && len(res) > origParams.Pagination.Offset {
		pointerResultList = res[origParams.Pagination.Offset:]
	}
	if len(res) <= origParams.Pagination.Offset {
		pointerResultList = hybrid.Results{}
	}

	out := make([]search.Result, 0, len(pointerResultList))
	for _, pointerResult := range pointerResultList {
		out = append(out, *pointerResult)
	}

	return out, nil
}
