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
	nearText2 "github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

// Do a bm25 search.  The results will be used in the hybrid algorithm
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

	return out, "keyword,bm25", nil
}

// Do a nearvector search.  The results will be used in the hybrid algorithm
func denseSearch(ctx context.Context, nearVecParams *searchparams.NearVector, e *Explorer, params dto.GetParams, searchname string) ([]*search.Result, string, error) {
	params.NearVector = nearVecParams
	params.Pagination.Offset = 0
	if params.Pagination.Limit < hybrid.DefaultLimit {
		params.Pagination.Limit = hybrid.DefaultLimit
	}

	targetVector := ""
	if len(params.HybridSearch.TargetVectors) > 0 {
		targetVector = params.HybridSearch.TargetVectors[0]
	}
	// Subsearch takes precedence over the top level
	if len(nearVecParams.TargetVectors) > 0 {
		targetVector = nearVecParams.TargetVectors[0]
	}

	targetVector, err := e.targetParamHelper.GetTargetVectorOrDefault(e.schemaGetter.GetSchemaSkipAuth(), params.ClassName, targetVector)
	if err != nil {
		return nil, "", err
	}

	params.NearVector.TargetVectors = []string{targetVector}

	// TODO confirm that targetVectos is being passed through as part of the params
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

	return out, "vector," + searchname, nil
}

/*
type NearTextParams struct {
    Values        []string
    Limit         int
    MoveTo        ExploreMove
    MoveAwayFrom  ExploreMove
    Certainty     float64
    Distance      float64
    WithDistance  bool
    Network       bool
    Autocorrect   bool
    TargetVectors []string
}
*/
// Do a nearText search.  The results will be used in the hybrid algorithm
func nearTextSubSearch(ctx context.Context, e *Explorer, params dto.GetParams) ([]*search.Result, string, error) {
	var subSearchParams nearText2.NearTextParams

	subSearchParams.Values = params.HybridSearch.NearTextParams.Values
	subSearchParams.Limit = params.HybridSearch.NearTextParams.Limit

	subSearchParams.Certainty = params.HybridSearch.NearTextParams.Certainty
	subSearchParams.Distance = params.HybridSearch.NearTextParams.Distance
	subSearchParams.Limit = params.HybridSearch.NearTextParams.Limit
	subSearchParams.MoveTo.Force = params.HybridSearch.NearTextParams.MoveTo.Force
	subSearchParams.MoveTo.Values = params.HybridSearch.NearTextParams.MoveTo.Values

	// TODO objects

	subSearchParams.MoveAwayFrom.Force = params.HybridSearch.NearTextParams.MoveAwayFrom.Force
	subSearchParams.MoveAwayFrom.Values = params.HybridSearch.NearTextParams.MoveAwayFrom.Values
	// TODO objects

	subSearchParams.Network = params.HybridSearch.NearTextParams.Network

	subSearchParams.WithDistance = params.HybridSearch.NearTextParams.WithDistance

	targetVector := ""
	if len(params.HybridSearch.TargetVectors) > 0 {
		targetVector = params.HybridSearch.TargetVectors[0]
	}
	/*
		//FIXME?

		// Subsearch takes precedence over the top level
		if len(subSearchParams.TargetVectors) > 0 {
			targetVector = params.HybridSearch.NearTextParams.TargetVectors[0]
		}
	*/

	targetVector, err := e.targetParamHelper.GetTargetVectorOrDefault(e.schemaGetter.GetSchemaSkipAuth(), params.ClassName, targetVector)
	if err != nil {
		return nil, "", err
	}

	subSearchParams.TargetVectors = []string{targetVector}

	subsearchWrap := params
	if subsearchWrap.ModuleParams == nil {
		subsearchWrap.ModuleParams = map[string]interface{}{}
	}

	subsearchWrap.ModuleParams["nearText"] = &subSearchParams

	subsearchWrap.HybridSearch = nil
	partial_results, vector, err := e.getClassVectorSearch(ctx, subsearchWrap)
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

// Hybrid search.  This is the main entry point to the hybrid search algorithm
func (e *Explorer) Hybrid(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	var results [][]*search.Result
	var weights []float64
	var names []string
	var targetVector string

	if params.HybridSearch.NearTextParams != nil && params.HybridSearch.NearVectorParams != nil {
		return nil, fmt.Errorf("hybrid search cannot have both nearText and nearVector parameters")
	}

	if len(params.HybridSearch.TargetVectors) > 0 {
		targetVector = params.HybridSearch.TargetVectors[0]
	}

	var err error
	targetVector, err = e.targetParamHelper.GetTargetVectorOrDefault(e.schemaGetter.GetSchemaSkipAuth(), params.ClassName, targetVector)
	if err != nil {
		return nil, err
	}

	origParams := params
	params.Pagination = &filters.Pagination{
		Limit:   params.Pagination.Limit,
		Offset:  params.Pagination.Offset,
		Autocut: params.Pagination.Autocut,
	}

	// If the user has given any weight to the vector search, choose 1 of three possible vector searches
	//
	// 1. If the user hase provided nearText parameters, use them in a nearText search
	// 2. If the user has provided nearVector parameters, use them in a nearVector search
	// 3. (Default) Do a vector search with the default parameters (the old hybrid search)
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
		} else if params.HybridSearch.NearVectorParams != nil {
			res, name, err := denseSearch(ctx, params.HybridSearch.NearVectorParams, e, params, "nearVector")
			if err != nil {
				e.logger.WithField("action", "hybrid").WithError(err).Error("denseSearch failed")
				return nil, err
			}
			weights = append(weights, params.HybridSearch.Alpha)
			results = append(results, res)
			names = append(names, name)
		} else {
			sch := e.schemaGetter.GetSchemaSkipAuth()
			class := sch.FindClassByName(schema.ClassName(params.ClassName))
			if class == nil {
				return nil, fmt.Errorf("class %q not found", params.ClassName)
			}
			if len(params.HybridSearch.Vector) == 0 {
				var err error
				params.SearchVector, err = e.modulesProvider.VectorFromInput(ctx, params.ClassName, params.HybridSearch.Query, targetVector)
				if err != nil {
					return nil, err
				}
			} else {
				params.SearchVector = params.HybridSearch.Vector
			}

			// Build a new vearvec search
			nearVecParams := &searchparams.NearVector{
				Vector:        params.SearchVector,
				TargetVectors: params.HybridSearch.TargetVectors,
			}

			res, name, err := denseSearch(ctx, nearVecParams, e, params, "hybridVector")
			if err != nil {
				e.logger.WithField("action", "hybrid").WithError(err).Error("denseSearch failed")
				return nil, err
			} else {
				weights = append(weights, params.HybridSearch.Alpha)
				results = append(results, res)
				names = append(names, name)
			}

		}
	}

	// If the user has given any weight to the keyword search, do a keyword search
	if 1-params.HybridSearch.Alpha > 0 {
		sparseResults, name, err := sparseSearch(ctx, e, params)
		if err != nil {
			e.logger.WithField("action", "hybrid").WithError(err).Error("sparseSearch failed")
			return nil, err
		} else {
			weights = append(weights, 1-params.HybridSearch.Alpha)
			results = append(results, sparseResults)
			names = append(names, name)
		}
	}

	// The postProcess function is used to limit the number of results and to resolve references
	// in the results.  It is called after all the subsearches have been completed, and before autocut
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

	res, err := hybrid.HybridSubsearch(ctx, &hybrid.Params{
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

	// The rest of weaviate uses []search.Result, so we convert the hpointerResultList to []search.Result

	out := make([]search.Result, 0, len(pointerResultList))
	for _, pointerResult := range pointerResultList {
		out = append(out, *pointerResult)
	}

	return out, nil
}
