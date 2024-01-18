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

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"

	"github.com/weaviate/weaviate/entities/autocut"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
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
type Result struct {
	DocID uint64
	*search.Result
}

type Results []*Result

func (res Results) SearchResults() []search.Result {
	out := make([]search.Result, len(res))
	for i, r := range res {
		out[i] = *r.Result
	}
	return out
}

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
type postProcFunc func(hybridResults Results) (postProcResults []search.Result, err error)

type modulesProvider interface {
	VectorFromInput(ctx context.Context,
		className string, input string) ([]float32, error)
}

// Search executes sparse and dense searches and combines the result sets using Reciprocal Rank Fusion
func Search(ctx context.Context, params *Params, logger logrus.FieldLogger, sparseSearch sparseSearchFunc, denseSearch denseSearchFunc, postProc postProcFunc, modules modulesProvider) (Results, error) {
	var (
		found   [][]*Result
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
			res, err := processDenseSearch(ctx, denseSearch, params, modules)
			if err != nil {
				return nil, err
			}

			found = append(found, res)
			weights = append(weights, alpha)
			names = append(names, "vector")
		}
	} else {
		ss := params.SubSearches

		// To catch error if ss is empty
		_, err := decideSearchVector(ctx, params, modules)
		if err != nil {
			return nil, err
		}

		for _, subsearch := range ss.([]searchparams.WeightedSearchResult) {
			res, name, weight, err := handleSubSearch(ctx, &subsearch, denseSearch, sparseSearch, params, modules)
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
	}
	if len(weights) != len(found) {
		return nil, fmt.Errorf("length of weights and results do not match for hybrid search %v vs. %v", len(weights), len(found))
	}

	var fused []*Result
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
		fused = fused[:len(sr)]
		for i := range fused {
			fused[i].Result = &(sr[i])
		}
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

func processSparseSearch(results []*storobj.Object, weights []float32, err error) ([]*Result, error) {
	if err != nil {
		return nil, fmt.Errorf("sparse search: %w", err)
	}

	out := make([]*Result, len(results))
	for i, obj := range results {
		sr := obj.SearchResultWithDist(additional.Properties{}, weights[i])
		sr.SecondarySortValue = sr.Score
		sr.ExplainScore = "(bm25)" + sr.ExplainScore
		out[i] = &Result{obj.DocID(), &sr}
	}
	return out, nil
}

func processDenseSearch(ctx context.Context, denseSearch denseSearchFunc, params *Params, modules modulesProvider) ([]*Result, error) {
	vector, err := decideSearchVector(ctx, params, modules)
	if err != nil {
		return nil, err
	}

	res, dists, err := denseSearch(vector)
	if err != nil {
		return nil, fmt.Errorf("dense search: %w", err)
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		sr.SecondarySortValue = 1 - sr.Dist
		sr.ExplainScore = fmt.Sprintf(
			"(vector) %v %v ", truncateVectorString(10, vector),
			res[i].ExplainScore())
		out[i] = &Result{obj.DocID(), &sr}
	}
	return out, nil
}

func handleSubSearch(ctx context.Context, subsearch *searchparams.WeightedSearchResult, denseSearch denseSearchFunc, sparseSearch sparseSearchFunc, params *Params, modules modulesProvider) ([]*Result, string, float64, error) {
	switch subsearch.Type {
	case "bm25":
		fallthrough
	case "sparseSearch":
		return sparseSubSearch(subsearch, params, sparseSearch)
	case "nearText":
		return nearTextSubSearch(ctx, subsearch, denseSearch, params, modules)
	case "nearVector":
		return nearVectorSubSearch(subsearch, denseSearch)
	default:
		return nil, "unknown", 0, fmt.Errorf("unknown hybrid search type %q", subsearch.Type)
	}
}

func sparseSubSearch(subsearch *searchparams.WeightedSearchResult, params *Params, sparseSearch sparseSearchFunc) ([]*Result, string, float64, error) {
	sp := subsearch.SearchParams.(searchparams.KeywordRanking)
	params.Keyword = &sp

	res, dists, err := sparseSearch()
	if err != nil {
		return nil, "", 0, fmt.Errorf("sparse subsearch: %w", err)
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		out[i] = &Result{obj.DocID(), &sr}
	}

	return out, "bm25f", subsearch.Weight, nil
}

func nearTextSubSearch(ctx context.Context, subsearch *searchparams.WeightedSearchResult, denseSearch denseSearchFunc, params *Params, modules modulesProvider) ([]*Result, string, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearTextParams)
	if modules == nil {
		return nil, "", 0, nil
	}

	vector, err := vectorFromModuleInput(ctx, params.Class, sp.Values[0], modules)
	if err != nil {
		return nil, "", 0, err
	}

	res, dists, err := denseSearch(vector)
	if err != nil {
		return nil, "", 0, err
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		out[i] = &Result{obj.DocID(), &sr}
	}

	return out, "vector,nearText", subsearch.Weight, nil
}

func nearVectorSubSearch(subsearch *searchparams.WeightedSearchResult, denseSearch denseSearchFunc) ([]*Result, string, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearVector)

	res, dists, err := denseSearch(sp.Vector)
	if err != nil {
		return nil, "", 0, err
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		out[i] = &Result{obj.DocID(), &sr}
	}

	return out, "vector,nearVector", subsearch.Weight, nil
}

func decideSearchVector(ctx context.Context, params *Params, modules modulesProvider) ([]float32, error) {
	var (
		vector []float32
		err    error
	)

	if params.Vector != nil && len(params.Vector) != 0 {
		vector = params.Vector
	} else {
		if modules != nil {
			vector, err = vectorFromModuleInput(ctx, params.Class, params.Query, modules)
			if err != nil {
				return nil, err
			}
		}
	}

	return vector, nil
}

func vectorFromModuleInput(ctx context.Context, class, input string, modules modulesProvider) ([]float32, error) {
	vector, err := modules.VectorFromInput(ctx, class, input)
	if err != nil {
		return nil, fmt.Errorf("get vector input from modules provider: %w", err)
	}
	return vector, nil
}

func truncateVectorString(maxLength int, vector []float32) string {
	if len(vector) <= maxLength {
		return fmt.Sprintf("%v", vector)
	}
	return fmt.Sprintf("%v...", vector[:maxLength])
}
