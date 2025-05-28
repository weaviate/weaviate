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

	"github.com/go-openapi/strfmt"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"

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

	params.Group = nil
	params.GroupBy = nil

	if params.Pagination == nil {
		return nil, "", fmt.Errorf("invalid params, pagination object is nil")
	}

	if params.HybridSearch.SearchOperator != "" {
		params.KeywordRanking.SearchOperator = params.HybridSearch.SearchOperator
	}

	if params.HybridSearch.MinimumOrTokensMatch != 0 {
		params.KeywordRanking.MinimumOrTokensMatch = params.HybridSearch.MinimumOrTokensMatch
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
func denseSearch(ctx context.Context, e *Explorer, params dto.GetParams, searchname string, targetVectors []string, searchVector *searchparams.NearVector) ([]*search.Result, string, error) {
	params.Pagination.Offset = 0
	if params.Pagination.Limit < hybrid.DefaultLimit {
		params.Pagination.Limit = hybrid.DefaultLimit
	}
	params.Group = nil
	params.GroupBy = nil

	partialResults, searchVectors, err := e.searchForTargets(ctx, params, targetVectors, searchVector)
	if err != nil {
		return nil, "", err
	}
	var vector models.Vector
	if len(searchVectors) > 0 {
		vector = searchVectors[0]
	}

	results, err := e.searchResultsToGetResponseWithType(ctx, partialResults, vector, params)
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
func nearTextSubSearch(ctx context.Context, e *Explorer, params dto.GetParams, targetVectors []string) ([]*search.Result, string, error) {
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

	subSearchParams.TargetVectors = targetVectors // TODO support multiple target vectors

	subsearchWrap := params
	if subsearchWrap.ModuleParams == nil {
		subsearchWrap.ModuleParams = map[string]interface{}{}
	}

	subsearchWrap.ModuleParams["nearText"] = &subSearchParams

	subsearchWrap.HybridSearch = nil
	subsearchWrap.Group = nil
	subsearchWrap.GroupBy = nil
	partialResults, vectors, err := e.searchForTargets(ctx, subsearchWrap, targetVectors, nil)
	if err != nil {
		return nil, "", err
	}

	var vector models.Vector
	if len(vectors) > 0 {
		vector = vectors[0]
	}

	results, err := e.searchResultsToGetResponseWithType(ctx, partialResults, vector, params)
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
	var err error
	var results [][]*search.Result
	var weights []float64
	var names []string
	var targetVectors []string

	if params.HybridSearch.NearTextParams != nil && params.HybridSearch.NearVectorParams != nil {
		return nil, fmt.Errorf("hybrid search cannot have both nearText and nearVector parameters")
	}

	origParams := params
	params.Pagination = &filters.Pagination{
		Limit:   params.Pagination.Limit,
		Offset:  params.Pagination.Offset,
		Autocut: params.Pagination.Autocut,
	}

	// pagination is handled after combining results
	vectorParams := params
	vectorParams.Pagination = &filters.Pagination{
		Limit:   params.Pagination.Limit,
		Offset:  0,
		Autocut: -1,
	}

	keywordParams := params
	keywordParams.Pagination = &filters.Pagination{
		Limit:   params.Pagination.Limit,
		Offset:  0,
		Autocut: -1,
	}

	targetVectors, err = e.targetParamHelper.GetTargetVectorOrDefault(e.schemaGetter.GetSchemaSkipAuth(), params.ClassName, params.HybridSearch.TargetVectors)
	if err != nil {
		return nil, err
	}

	// If the user has given any weight to the vector search, choose 1 of three possible vector searches
	//
	// 1. If the user hase provided nearText parameters, use them in a nearText search
	// 2. If the user has provided nearVector parameters, use them in a nearVector search
	// 3. (Default) Do a vector search with the default parameters (the old hybrid search)

	resultsCount := 1
	if params.HybridSearch.Alpha != 0 && params.HybridSearch.Alpha != 1 {
		resultsCount = 2
	}

	eg := enterrors.NewErrorGroupWrapper(e.logger)
	eg.SetLimit(resultsCount)

	results = make([][]*search.Result, resultsCount)
	weights = make([]float64, resultsCount)
	names = make([]string, resultsCount)
	var belowCutoffSet map[strfmt.UUID]struct{}

	if (params.HybridSearch.Alpha) > 0 {
		eg.Go(func() error {
			params := vectorParams
			var err error
			var name string
			var res []*search.Result
			var errorText string
			if params.HybridSearch.NearTextParams != nil {
				res, name, err = nearTextSubSearch(ctx, e, params, targetVectors)
				errorText = "nearTextSubSearch"
			} else if params.HybridSearch.NearVectorParams != nil {
				searchVectors := make([]*searchparams.NearVector, len(targetVectors))
				for i, targetVector := range targetVectors {
					searchVectors[i] = params.HybridSearch.NearVectorParams
					searchVectors[i].TargetVectors = []string{targetVector}
				}
				res, name, err = denseSearch(ctx, e, params, "nearVector", targetVectors, params.HybridSearch.NearVectorParams)
				errorText = "nearVectorSubSearch"
			} else {
				sch := e.schemaGetter.GetSchemaSkipAuth()
				class := sch.FindClassByName(schema.ClassName(params.ClassName))
				if class == nil {
					return fmt.Errorf("class %q not found", params.ClassName)
				}

				searchVectors := &searchparams.NearVector{}
				searchVectors.Vectors = make([]models.Vector, len(targetVectors))
				searchVectors.TargetVectors = make([]string, len(targetVectors))
				isVectorEmpty, isVectorEmptyErr := dto.IsVectorEmpty(params.HybridSearch.Vector)
				if isVectorEmptyErr != nil {
					return fmt.Errorf("is hybrid vector empty: %w", isVectorEmptyErr)
				}
				if !isVectorEmpty {
					for i, targetVector := range targetVectors {
						searchVectors.TargetVectors[i] = targetVector
						searchVectors.Vectors[i] = params.HybridSearch.Vector
					}
				} else {
					eg2 := enterrors.NewErrorGroupWrapper(e.logger)
					eg2.SetLimit(_NUMCPU)
					for i, targetVector := range targetVectors {
						i := i
						targetVector := targetVector
						eg2.Go(func() error {
							isMultiVector, err := e.modulesProvider.IsTargetVectorMultiVector(params.ClassName, targetVector)
							if err != nil {
								return fmt.Errorf("hybrid: is target vector multi vector: %w", err)
							}
							if isMultiVector {
								searchVectors.TargetVectors[i] = targetVector
								searchVector, err := e.modulesProvider.MultiVectorFromInput(ctx, params.ClassName, params.HybridSearch.Query, targetVector)
								searchVectors.Vectors[i] = searchVector
								return err
							}
							searchVectors.TargetVectors[i] = targetVector
							searchVector, err := e.modulesProvider.VectorFromInput(ctx, params.ClassName, params.HybridSearch.Query, targetVector)
							searchVectors.Vectors[i] = searchVector
							return err
						})
					}
					if err := eg2.Wait(); err != nil {
						return err
					}
				}

				res, name, err = denseSearch(ctx, e, params, "hybridVector", targetVectors, searchVectors)
				errorText = "hybrid"
			}

			if params.HybridSearch.WithDistance {
				belowCutoffSet = map[strfmt.UUID]struct{}{}
				maxFound := -1
				for i := range res {
					if res[i].Dist <= params.HybridSearch.Distance {
						belowCutoffSet[res[i].ID] = struct{}{}
						maxFound = i
					} else {
						break
					}
				}
				// sorted by distance, so just remove everything after the first entry we found
				res = res[:maxFound+1]
			}

			if err != nil {
				e.logger.WithField("action", "hybrid").WithError(err).Error(errorText + " failed")
				return err
			} else {
				weights[0] = params.HybridSearch.Alpha
				results[0] = res
				names[0] = name
			}

			return nil
		})
	}

	sparseSearchIndex := -1
	if 1-params.HybridSearch.Alpha > 0 {
		eg.Go(func() error {
			// If the user has given any weight to the keyword search, do a keyword search
			params := keywordParams
			sparseResults, name, err := sparseSearch(ctx, e, params)
			if err != nil {
				e.logger.WithField("action", "hybrid").WithError(err).Error("sparseSearch failed")
				return err
			} else {
				weights[len(weights)-1] = 1 - params.HybridSearch.Alpha
				results[len(weights)-1] = sparseResults
				names[len(weights)-1] = name
				sparseSearchIndex = len(weights) - 1
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// remove results with a vector distance above the cutoff from the BM25 results
	if sparseSearchIndex >= 0 && belowCutoffSet != nil {
		newResults := make([]*search.Result, 0, len(results[sparseSearchIndex]))
		for i := range results[sparseSearchIndex] {
			if _, ok := belowCutoffSet[results[sparseSearchIndex][i].ID]; ok {
				newResults = append(newResults, results[sparseSearchIndex][i])
			}
		}
		results[sparseSearchIndex] = newResults
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

	res, err := hybrid.HybridCombiner(ctx, &hybrid.Params{
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

	if origParams.GroupBy != nil {
		groupedResults, err := e.groupSearchResults(ctx, out, origParams.GroupBy)
		if err != nil {
			return nil, err
		}
		return groupedResults, nil
	}
	return out, nil
}
