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

package hybrid

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	uc "github.com/weaviate/weaviate/usecases/schema"
)

const DefaultLimit = 100

type Params struct {
	*searchparams.HybridSearch
	Keyword *searchparams.KeywordRanking
	Class   string
	Autocut int
}

// Result facilitates the pairing of a search result with its internal doc id.
//
// This type is key in generalising hybrid search across different use cases.
// Some use cases require a full search result (Get{} queries) and others need
// only a doc id (Aggregate{}) which the search.Result type does not contain.
// It does now

type Results []*search.Result

// sparseSearchFunc is the signature of a closure which performs sparse search.
// Any package which wishes use hybrid search must provide this. The weights are
// used in calculating the final scores of the result set.
type sparseSearchFunc func() (results []*storobj.Object, weights []float32, err error)

// denseSearchFunc is the signature of a closure which performs dense search.
// A search vector argument is required to pass along to the vector index.
// Any package which wishes use hybrid search must provide this The weights are
// used in calculating the final scores of the result set.
type denseSearchFunc func(searchVector []float32) (results []*storobj.Object, weights []float32, err error)

// postProcFunc takes the results of the hybrid search and applies some transformation.
// This is optionally provided, and allows the caller to somehow change the nature of
// the result set. For example, Get{} queries sometimes require resolving references,
// which is implemented by doing the reference resolution within a postProcFunc closure.
type postProcFunc func(hybridResults []*search.Result) (postProcResults []search.Result, err error)

type modulesProvider interface {
	VectorFromInput(ctx context.Context,
		className, input, targetVector string) ([]float32, error)
}

type targetVectorParamHelper interface {
	GetTargetVectorOrDefault(sch schema.Schema, className, targetVector string) (string, error)
}

// Search executes sparse and dense searches and combines the result sets using Reciprocal Rank Fusion
func Search(ctx context.Context, params *Params, logger logrus.FieldLogger, sparseSearch sparseSearchFunc,
	denseSearch denseSearchFunc, postProc postProcFunc, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) ([]*search.Result, error) {
	var (
		found   [][]*search.Result
		weights []float64
		names   []string
	)

	if params.Query != "" {
		alpha := params.Alpha

		if alpha < 1 {
			res, err := processSparseSearch(sparseSearch())
			if err != nil {
				return nil, err
			}

			found = append(found, res)
			weights = append(weights, 1-alpha)
			names = append(names, "keyword")
		}

		if alpha > 0 {
			res, err := processDenseSearch(ctx, denseSearch, params, modules, schemaGetter, targetVectorParamHelper)
			if err != nil {
				return nil, err
			}

			found = append(found, res)
			weights = append(weights, alpha)
			names = append(names, "vector")
		}
	} else if params.Vector != nil {
		// Perform a plain vector search, no keyword query provided
		res, err := processDenseSearch(ctx, denseSearch, params, modules, schemaGetter, targetVectorParamHelper)
		if err != nil {
			return nil, err
		}

		found = append(found, res)
		// weight is irrelevant here, we're doing vector search only
		weights = append(weights, 1)
		names = append(names, "vector")
	} else if params.SubSearches != nil {
		ss := params.SubSearches

		// To catch error if ss is empty
		_, err := decideSearchVector(ctx, params, modules, schemaGetter, targetVectorParamHelper)
		if err != nil {
			return nil, err
		}

		for _, subsearch := range ss.([]searchparams.WeightedSearchResult) {
			res, name, weight, err := handleSubSearch(ctx, &subsearch, denseSearch, sparseSearch, params, modules, schemaGetter, targetVectorParamHelper)
			if err != nil {
				return nil, err
			}

			if res == nil {
				continue
			}

			found = append(found, res)
			weights = append(weights, weight)
			names = append(names, name)
		}
	} else {
		// This should not happen, as it should be caught at the validation level,
		// but just in case it does, we catch it here.
		return nil, fmt.Errorf("no query, search vector, or sub-searches provided")
	}
	if len(weights) != len(found) {
		return nil, fmt.Errorf("length of weights and results do not match for hybrid search %v vs. %v", len(weights), len(found))
	}

	var fused []*search.Result
	if params.FusionAlgorithm == common_filters.HybridRankedFusion {
		fused = FusionRanked(weights, found, names)
	} else if params.FusionAlgorithm == common_filters.HybridRelativeScoreFusion {
		fused = FusionRelativeScore(weights, found, names)
	} else {
		return nil, fmt.Errorf("unknown ranking algorithm %v for hybrid search", params.FusionAlgorithm)
	}

	if postProc != nil {
		sr, err := postProc(fused)
		if err != nil {
			return nil, fmt.Errorf("hybrid search post-processing: %w", err)
		}
		newResults := make([]*search.Result, len(sr))
		for i := range sr {
			if err != nil {
				return nil, fmt.Errorf("hybrid search post-processing: %w", err)
			}
			newResults[i] = &sr[i]
		}
		fused = newResults
	}
	if params.Autocut > 0 {
		scores := make([]float32, len(fused))
		for i := range fused {
			scores[i] = fused[i].Score
		}
		cutOff := autocut.Autocut(scores, params.Autocut)
		fused = fused[:cutOff]
	}
	return fused, nil
}

// Search combines the result sets using Reciprocal Rank Fusion or Relative Score Fusion
func HybridSubsearch(ctx context.Context, params *Params, resultSet [][]*search.Result, weights []float64, names []string, logger logrus.FieldLogger, postProc postProcFunc) ([]*search.Result, error) {
	if len(weights) != len(resultSet) {
		return nil, fmt.Errorf("length of weights and results do not match for hybrid search %v vs. %v", len(weights), len(resultSet))
	}
	if len(weights) != len(names) {
		return nil, fmt.Errorf("length of weights and names do not match for hybrid search %v vs. %v", len(weights), len(names))
	}

	var fused []*search.Result
	if params.FusionAlgorithm == common_filters.HybridRankedFusion {
		fused = FusionRanked(weights, resultSet, names)
	} else if params.FusionAlgorithm == common_filters.HybridRelativeScoreFusion {
		fused = FusionRelativeScore(weights, resultSet, names)
	} else {
		return nil, fmt.Errorf("unknown ranking algorithm %v for hybrid search", params.FusionAlgorithm)
	}

	if postProc != nil {
		sr, err := postProc(fused)
		if err != nil {
			return nil, fmt.Errorf("hybrid search post-processing: %w", err)
		}
		newResults := make([]*search.Result, len(sr))
		for i := range sr {
			if err != nil {
				return nil, fmt.Errorf("hybrid search post-processing: %w", err)
			}
			newResults[i] = &sr[i]
		}
		fused = newResults
	}
	if params.Autocut > 0 {
		scores := make([]float32, len(fused))
		for i := range fused {
			scores[i] = fused[i].Score
		}
		cutOff := autocut.Autocut(scores, params.Autocut)
		fused = fused[:cutOff]
	}
	return fused, nil
}

func processSparseSearch(results []*storobj.Object, scores []float32, err error) ([]*search.Result, error) {
	if err != nil {
		return nil, fmt.Errorf("sparse search: %w", err)
	}

	out := make([]*search.Result, len(results))
	for i, obj := range results {
		sr := obj.SearchResultWithScore(additional.Properties{}, scores[i])
		sr.SecondarySortValue = sr.Score
		out[i] = &sr
	}
	return out, nil
}

func processDenseSearch(ctx context.Context,
	denseSearch denseSearchFunc, params *Params, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) ([]*search.Result, error) {
	vector, err := decideSearchVector(ctx, params, modules, schemaGetter, targetVectorParamHelper)
	if err != nil {
		return nil, err
	}

	res, dists, err := denseSearch(vector)
	if err != nil {
		return nil, fmt.Errorf("dense search: %w", err)
	}

	out := make([]*search.Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		sr.SecondarySortValue = 1 - sr.Dist
		out[i] = &sr
	}
	return out, nil
}

func handleSubSearch(ctx context.Context,
	subsearch *searchparams.WeightedSearchResult, denseSearch denseSearchFunc, sparseSearch sparseSearchFunc,
	params *Params, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) ([]*search.Result, string, float64, error) {
	switch subsearch.Type {
	case "bm25":
		fallthrough
	case "sparseSearch":
		return sparseSubSearch(subsearch, params, sparseSearch)
	case "nearText":
		return nearTextSubSearch(ctx, subsearch, denseSearch, params, modules, schemaGetter, targetVectorParamHelper)
	case "nearVector":
		return nearVectorSubSearch(subsearch, denseSearch)
	default:
		return nil, "unknown", 0, fmt.Errorf("unknown hybrid search type %q", subsearch.Type)
	}
}

func sparseSubSearch(subsearch *searchparams.WeightedSearchResult, params *Params, sparseSearch sparseSearchFunc) ([]*search.Result, string, float64, error) {
	sp := subsearch.SearchParams.(searchparams.KeywordRanking)
	params.Keyword = &sp

	res, dists, err := sparseSearch()
	if err != nil {
		return nil, "", 0, fmt.Errorf("sparse subsearch: %w", err)
	}

	out := make([]*search.Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		sr.SecondarySortValue = sr.Score
		out[i] = &sr
	}

	return out, "bm25f", subsearch.Weight, nil
}

func nearTextSubSearch(ctx context.Context, subsearch *searchparams.WeightedSearchResult, denseSearch denseSearchFunc,
	params *Params, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) ([]*search.Result, string, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearTextParams)
	if modules == nil || schemaGetter == nil || targetVectorParamHelper == nil {
		return nil, "", 0, nil
	}

	targetVector := getTargetVector(params.TargetVectors)
	targetVector, err := targetVectorParamHelper.GetTargetVectorOrDefault(schemaGetter.GetSchemaSkipAuth(),
		params.Class, targetVector)
	if err != nil {
		return nil, "", 0, err
	}

	vector, err := vectorFromModuleInput(ctx, params.Class, sp.Values[0], targetVector, modules)
	if err != nil {
		return nil, "", 0, err
	}

	res, dists, err := denseSearch(vector)
	if err != nil {
		return nil, "", 0, err
	}

	out := make([]*search.Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		sr.SecondarySortValue = 1 - sr.Dist
		out[i] = &sr
	}

	return out, "vector,nearText", subsearch.Weight, nil
}

func nearVectorSubSearch(subsearch *searchparams.WeightedSearchResult, denseSearch denseSearchFunc) ([]*search.Result, string, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearVector)

	res, dists, err := denseSearch(sp.Vector)
	if err != nil {
		return nil, "", 0, err
	}

	out := make([]*search.Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		sr.SecondarySortValue = 1 - sr.Dist
		out[i] = &sr
	}

	return out, "vector,nearVector", subsearch.Weight, nil
}

func decideSearchVector(ctx context.Context,
	params *Params, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) ([]float32, error) {
	var (
		vector []float32
		err    error
	)

	if params.Vector != nil && len(params.Vector) != 0 {
		vector = params.Vector
	} else {
		if modules != nil && schemaGetter != nil && targetVectorParamHelper != nil {
			targetVector := getTargetVector(params.TargetVectors)
			targetVector, err = targetVectorParamHelper.GetTargetVectorOrDefault(schemaGetter.GetSchemaSkipAuth(),
				params.Class, targetVector)
			if err != nil {
				return nil, err
			}
			vector, err = vectorFromModuleInput(ctx, params.Class, params.Query, targetVector, modules)
			if err != nil {
				return nil, err
			}
		}
	}

	return vector, nil
}

func vectorFromModuleInput(ctx context.Context, class, input, targetVector string, modules modulesProvider) ([]float32, error) {
	vector, err := modules.VectorFromInput(ctx, class, input, targetVector)
	if err != nil {
		return nil, fmt.Errorf("get vector input from modules provider: %w", err)
	}
	return vector, nil
}

func getTargetVector(targetVectors []string) string {
	if len(targetVectors) == 1 {
		return targetVectors[0]
	}
	return ""
}
