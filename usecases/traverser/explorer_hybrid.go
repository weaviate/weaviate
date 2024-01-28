package traverser

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
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

func denseSearch(ctx context.Context, vec []float32, e *Explorer, params dto.GetParams) ([]*search.Result, string, error) {
	/* FIXME
	baseSearchLimit := params.Pagination.Limit + params.Pagination.Offset
	var hybridSearchLimit int
	if baseSearchLimit <= hybrid.DefaultLimit {
		hybridSearchLimit = hybrid.DefaultLimit
	} else {
		hybridSearchLimit = baseSearchLimit
	}
	*/

	e.modulesProvider.VectorFromInput(ctx, params.ClassName, params.HybridSearch.Query)
	params.NearVector = &searchparams.NearVector{
		Vector: vec,
	}

	partial_results, vector, err := e.getClassVectorSearch(ctx, params)
	if err != nil {
		return nil, "", err
	}

	results, err := e.searchResultsToGetResponseWithType(ctx, partial_results, vector, params)
	if err != nil {
		return nil, "", err
	}

	out := make([]*search.Result, len(results))
	for i, sr := range results {
		sr.SecondarySortValue = 1 - sr.Dist
		out[i] = &sr
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

	if params.HybridSearch.NearTextParams != nil {
		res, name, err := nearTextSubSearch(ctx, e, params)
		if err != nil {
			return nil, err
			e.logger.WithField("action", "hybrid").WithError(err).Error("nearTextSubSearch failed")
		} else {
			weights = append(weights, params.HybridSearch.Alpha)
			results = append(results, res)
			names = append(names, name)
		}
	} else {
		res, name, err := denseSearch(ctx, params.SearchVector, e, params)
		if err != nil {
			return nil, err
			e.logger.WithField("action", "hybrid").WithError(err).Error("denseSearch failed")
		} else {
			weights = append(weights, params.HybridSearch.Alpha)
			results = append(results, res)
			names = append(names, name)
		}
	}

	sparseResults , name, err := sparseSearch(ctx, e, params)
	if err != nil {
		return nil, err
		e.logger.WithField("action", "hybrid").WithError(err).Error("sparseSearch failed")
	} else {
		weights = append(weights, 1-params.HybridSearch.Alpha)
		results = append(results, sparseResults)
		names = append(names, name)
	}

	postProcess := func(results []*search.Result) ([]search.Result, error) {
		totalLimit, err := e.CalculateTotalLimit(params.Pagination)
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

		res, err := e.searcher.ResolveReferences(ctx, res1, params.Properties, nil, params.AdditionalProperties, params.Tenant)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	res, err := hybrid.Do(ctx, &hybrid.Params{
		HybridSearch: params.HybridSearch,
		Keyword:      params.KeywordRanking,
		Class:        params.ClassName,
		Autocut:      params.Pagination.Autocut,
	}, results, weights, names, e.logger, postProcess)
	if err != nil {
		return nil, err
	}

	var pointerResultList hybrid.Results

	if params.Pagination.Limit <= 0 {
		params.Pagination.Limit = hybrid.DefaultLimit
	}

	if params.Pagination.Offset < 0 {
		params.Pagination.Offset = 0
	}

	if len(res) >= params.Pagination.Limit+params.Pagination.Offset {
		pointerResultList = res[params.Pagination.Offset : params.Pagination.Limit+params.Pagination.Offset]
	}
	if len(res) < params.Pagination.Limit+params.Pagination.Offset && len(res) > params.Pagination.Offset {
		pointerResultList = res[params.Pagination.Offset:]
	}
	if len(res) <= params.Pagination.Offset {
		pointerResultList = hybrid.Results{}
	}

	out := make([]search.Result, len(pointerResultList))
	for i := range pointerResultList {
		out[i] = *pointerResultList[i]
	}

	return out, nil
}
