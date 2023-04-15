//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hybrid

import (
	"context"
	"fmt"

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

type Searcher struct {
	params           *Params
	logger           logrus.FieldLogger
	sparseSearchFunc sparseSearchFunc
	denseSearchFunc  denseSearchFunc
	postProcFunc     postProcFunc
	modulesProvider  modulesProvider
}

func NewSearcher(params *Params, logger logrus.FieldLogger,
	sparse sparseSearchFunc, dense denseSearchFunc,
	postProc postProcFunc, modulesProvider modulesProvider,
) *Searcher {
	return &Searcher{
		logger:           logger,
		params:           params,
		sparseSearchFunc: sparse,
		denseSearchFunc:  dense,
		postProcFunc:     postProc,
		modulesProvider:  modulesProvider,
	}
}

// Search executes sparse and dense searches and combines the result sets using Reciprocal Rank Fusion
func (s *Searcher) Search(ctx context.Context) (Results, error) {
	var (
		found   [][]*Result
		weights []float64
	)

	if s.params.Query != "" {
		alpha := s.params.Alpha

		if alpha < 1 {
			res, err := s.sparseSearch()
			if err == nil {

				found = append(found, res)
				weights = append(weights, 1-alpha)
			}
		}

		if alpha > 0 {
			res, err := s.denseSearch(ctx)
			if err == nil {

				found = append(found, res)
				weights = append(weights, alpha)
			}
		}
	} else {
		ss := s.params.SubSearches
		for _, subsearch := range ss.([]searchparams.WeightedSearchResult) {
			res, weight, err := s.handleSubSearch(ctx, &subsearch)
			if err == nil {

				if res == nil {
					continue
				}

				found = append(found, res)
				weights = append(weights, weight)
			}
		}
	}

	fused := FusionReciprocal(weights, found)

	if s.params.Limit >= 1 && (len(fused) > s.params.Limit) { //-1 is possible?
		s.logger.Debugf("found more hybrid search results than limit, "+
			"limiting %v results to %v\n",
			len(fused), s.params.Limit)
		fused = fused[:s.params.Limit]
	}

	if s.postProcFunc != nil {
		sr, err := s.postProcFunc(fused)
		if err != nil {
			return nil, fmt.Errorf("hybrid search post-processing: %w", err)
		}
		for i := range fused {
			fused[i].Result = &(sr[i])
		}
	}

	return fused, nil
}

func (s *Searcher) sparseSearch() ([]*Result, error) {
	res, dists, err := s.sparseSearchFunc()
	if err != nil {
		return nil, fmt.Errorf("sparse search: %w", err)
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, 0, dists[i])
		sr.SecondarySortValue = sr.Score
		sr.ExplainScore = "(bm25)" + sr.ExplainScore
		out[i] = &Result{obj.DocID(), &sr}
	}
	return out, nil
}

func (s *Searcher) denseSearch(ctx context.Context) ([]*Result, error) {
	vector, err := s.decideSearchVector(ctx)
	if err != nil {
		return nil, err
	}

	res, dists, err := s.denseSearchFunc(vector)
	if err != nil {
		return nil, fmt.Errorf("dense search: %w", err)
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, 0, dists[i])
		sr.SecondarySortValue = 1 - sr.Dist
		sr.ExplainScore = fmt.Sprintf(
			"(vector) %v %v ", truncateVectorString(10, vector),
			res[i].ExplainScore())
		out[i] = &Result{obj.DocID(), &sr}
	}
	return out, nil
}

func (s *Searcher) handleSubSearch(ctx context.Context,
	subsearch *searchparams.WeightedSearchResult,
) ([]*Result, float64, error) {
	switch subsearch.Type {
	case "bm25":
		fallthrough
	case "sparseSearch":
		return s.sparseSubSearch(subsearch)
	case "nearText":
		return s.nearTextSubSearch(ctx, subsearch)
	case "nearVector":
		return s.nearVectorSubSearch(subsearch)
	default:
		return nil, 0, fmt.Errorf("unknown hybrid search type %q", subsearch.Type)
	}
}

func (s *Searcher) sparseSubSearch(
	subsearch *searchparams.WeightedSearchResult,
) ([]*Result, float64, error) {
	sp := subsearch.SearchParams.(searchparams.KeywordRanking)
	s.params.Keyword = &sp

	res, dists, err := s.sparseSearchFunc()
	if err != nil {
		return nil, 0, fmt.Errorf("sparse subsearch: %w", err)
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, 0, dists[i])
		sr.ExplainScore = "(bm25)" + sr.ExplainScore
		out[i] = &Result{obj.DocID(), &sr}
	}

	return out, subsearch.Weight, nil
}

func (s *Searcher) nearTextSubSearch(ctx context.Context,
	subsearch *searchparams.WeightedSearchResult,
) ([]*Result, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearTextParams)
	if s.modulesProvider == nil {
		return nil, 0, nil
	}

	vector, err := s.vectorFromModuleInput(ctx, s.params.Class, sp.Values[0])
	if err != nil {
		return nil, 0, err
	}

	res, dists, err := s.denseSearchFunc(vector)
	if err != nil {
		return nil, 0, err
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, 0, dists[i])
		sr.ExplainScore = fmt.Sprintf("(vector) %v %v ",
			truncateVectorString(10, vector), res[i].ExplainScore())
		out[i] = &Result{obj.DocID(), &sr}
	}

	return out, subsearch.Weight, nil
}

func (s *Searcher) nearVectorSubSearch(
	subsearch *searchparams.WeightedSearchResult,
) ([]*Result, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearVector)

	res, dists, err := s.denseSearchFunc(sp.Vector)
	if err != nil {
		return nil, 0, err
	}

	out := make([]*Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, 0, dists[i])
		sr.ExplainScore = fmt.Sprintf("(vector) %v %v ",
			truncateVectorString(10, sp.Vector), res[i].ExplainScore())
		out[i] = &Result{obj.DocID(), &sr}
	}

	return out, subsearch.Weight, nil
}

func (s *Searcher) decideSearchVector(ctx context.Context) ([]float32, error) {
	var (
		vector []float32
		err    error
	)

	if s.params.Vector != nil && len(s.params.Vector) != 0 {
		vector = s.params.Vector
	} else {
		if s.modulesProvider != nil {
			vector, err = s.vectorFromModuleInput(ctx, s.params.Class, s.params.Query)
			if err != nil {
				return nil, err
			}
		}
	}

	return vector, nil
}

func (s *Searcher) vectorFromModuleInput(ctx context.Context, class, input string) ([]float32, error) {
	vector, err := s.modulesProvider.VectorFromInput(ctx, class, input)
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
