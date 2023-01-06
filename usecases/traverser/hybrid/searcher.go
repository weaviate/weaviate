//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hybrid

import (
	"context"
	"fmt"
	"strconv"

	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/sirupsen/logrus"
)

const DefaultLimit = 100

type Params struct {
	*searchparams.HybridSearch
	Keyword *searchparams.KeywordRanking
	Class   string
}

type sparseFunc func() ([]search.Result, error)

type denseFunc func([]float32) ([]search.Result, error)

type modulesProvider interface {
	VectorFromInput(ctx context.Context,
		className string, input string) ([]float32, error)
}

type Searcher struct {
	params          *Params
	logger          logrus.FieldLogger
	sparseFunc      sparseFunc
	denseFunc       denseFunc
	modulesProvider modulesProvider
}

func NewSearcher(params *Params, logger logrus.FieldLogger,
	sparse sparseFunc, dense denseFunc,
	modulesProvider modulesProvider,
) *Searcher {
	return &Searcher{
		logger:          logger,
		params:          params,
		sparseFunc:      sparse,
		denseFunc:       dense,
		modulesProvider: modulesProvider,
	}
}

func (s *Searcher) Search(ctx context.Context) ([]search.Result, error) {
	var (
		found   [][]search.Result
		weights []float64
	)

	if s.params.Query != "" {
		alpha := s.params.Alpha

		if alpha < 1 {
			res, err := s.sparseSearch()
			if err != nil {
				return nil, err
			}

			found = append(found, res)
			weights = append(weights, 1-alpha)
		}

		if alpha > 0 {
			res, err := s.denseSearch(ctx)
			if err != nil {
				return nil, err
			}

			found = append(found, res)
			weights = append(weights, alpha)
		}
	} else {
		ss := s.params.SubSearches
		for _, subsearch := range ss.([]searchparams.WeightedSearchResult) {
			res, weight, err := s.handleSubSearch(ctx, &subsearch)
			if err != nil {
				return nil, err
			}

			if res == nil {
				continue
			}

			found = append(found, res)
			weights = append(weights, weight)
		}
	}

	fused := FusionReciprocal(weights, found)

	if s.params.Limit >= 1 && (len(fused) > s.params.Limit) { //-1 is possible?
		s.logger.Debugf("found more hybrid search results than limit, "+
			"limiting %v results to %v\n",
			len(fused), s.params.Limit)
		fused = fused[:s.params.Limit]
	}

	return fused, nil
}

func (s *Searcher) sparseSearch() ([]search.Result, error) {
	res, err := s.sparseFunc()
	if err != nil {
		return nil, fmt.Errorf("sparse search: %w", err)
	}

	for i := range res {
		res[i].SecondarySortValue = res[i].Score
		res[i].ExplainScore = "(bm25)" + res[i].ExplainScore
	}
	return res, nil
}

func (s *Searcher) denseSearch(ctx context.Context) ([]search.Result, error) {
	vector, err := s.decideSearchVector(ctx)
	if err != nil {
		return nil, err
	}

	res, err := s.denseFunc(vector)
	if err != nil {
		return nil, fmt.Errorf("dense search: %w", err)
	}

	for i := range res {
		res[i].SecondarySortValue = 1 - res[i].Dist
		res[i].ExplainScore = fmt.Sprintf(
			"(vector) %v %v ", truncateVectorString(10, vector),
			res[i].ExplainScore)
	}
	return res, nil
}

func (s *Searcher) handleSubSearch(ctx context.Context,
	subsearch *searchparams.WeightedSearchResult,
) ([]search.Result, float64, error) {
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
) ([]search.Result, float64, error) {
	sp := subsearch.SearchParams.(searchparams.KeywordRanking)
	s.params.Keyword = &sp

	res, err := s.sparseFunc()
	if err != nil {
		return nil, 0, fmt.Errorf("sparse subsearch: %w", err)
	}

	for i := range res {
		scStr := res[i].AdditionalProperties["score"].(string)
		sc, _ := strconv.ParseFloat(scStr, 64)
		res[i].Score = float32(sc)
		res[i].ExplainScore = "(bm25)" + res[i].ExplainScore
	}

	return res, subsearch.Weight, nil
}

func (s *Searcher) nearTextSubSearch(ctx context.Context,
	subsearch *searchparams.WeightedSearchResult,
) ([]search.Result, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearTextParams)
	if s.modulesProvider == nil {
		return nil, 0, nil
	}

	vector, err := s.vectorFromModuleInput(ctx, s.params.Class, sp.Values[0])
	if err != nil {
		return nil, 0, err
	}

	res, err := s.denseFunc(vector)
	if err != nil {
		return nil, 0, err
	}

	for i := range res {
		res[i].ExplainScore = fmt.Sprintf("(vector) %v %v ",
			truncateVectorString(10, vector), res[i].ExplainScore)
	}

	return res, subsearch.Weight, nil
}

func (s *Searcher) nearVectorSubSearch(
	subsearch *searchparams.WeightedSearchResult,
) ([]search.Result, float64, error) {
	sp := subsearch.SearchParams.(searchparams.NearVector)

	res, err := s.denseFunc(sp.Vector)
	if err != nil {
		return nil, 0, err
	}

	for i := range res {
		res[i].ExplainScore = fmt.Sprintf("(vector) %v %v ",
			truncateVectorString(10, sp.Vector), res[i].ExplainScore)
	}

	return res, subsearch.Weight, nil
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
