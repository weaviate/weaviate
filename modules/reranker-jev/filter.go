//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modrerankerjev

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/reranker-jev/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	rerankmodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
)

const (
	minProbabilityHeader = "X-Jev-Min-Probability"
	minScoreHeader       = "X-Jev-Min-Score"
	fetchDepthHeader     = "X-Jev-Fetch-Depth"
	orderHeader          = "X-Jev-Order"
)

// RerankFetchDepth returns the candidates to fetch before the rerank: the
// depth from the request header or the class setting, or pageEnd when the
// page reaches further. The same maxDocuments bound as Rank applies, so a
// page that ends beyond it fails here instead of after the search. The
// explorer only asks when the query can be over-fetched; a search with
// grouping, autocut, boost or MMR ignores the header and the setting.
func (m *ReRankerJevModule) RerankFetchDepth(ctx context.Context, cfg moduletools.ClassConfig, pageEnd int,
) (int, error) {
	settings := config.NewClassSettings(cfg)
	maxDocuments := min(settings.MaxDocuments(), config.MaxDocumentsLimit)
	depth := settings.FetchDepth()
	if header := modulecomponents.GetValueFromContext(ctx, fetchDepthHeader); header != "" {
		value, err := strconv.Atoi(header)
		if err != nil || value < 0 || value > maxDocuments {
			return 0, fmt.Errorf("%s must be a whole number between 0 and maxDocuments %d, got %q",
				fetchDepthHeader, maxDocuments, header)
		}
		depth = value
	} else if depth < 0 || depth > maxDocuments {
		return 0, fmt.Errorf("fetchDepth must be between 0 and maxDocuments %d, got %d", maxDocuments, depth)
	}
	if depth == 0 {
		return 0, nil
	}
	if pageEnd > maxDocuments {
		return 0, fmt.Errorf("the page ends at %d, beyond maxDocuments %d: lower offset+limit or raise maxDocuments",
			pageEnd, maxDocuments)
	}
	return max(depth, pageEnd), nil
}

// withJevRerank replaces the search functions of the rerank property. The
// GraphQL parts of the shared reranker property stay as they are.
func withJevRerank(properties map[string]modulecapabilities.AdditionalProperty, ranker *rank.ReRankerProvider,
) map[string]modulecapabilities.AdditionalProperty {
	out := make(map[string]modulecapabilities.AdditionalProperty, len(properties))
	for name, property := range properties {
		if name == modulecomponents.AdditionalPropertyRerank {
			rerank := jevRerank(ranker)
			searchFunctions := modulecapabilities.AdditionalSearch{}
			if property.SearchFunctions.ExploreGet != nil {
				searchFunctions.ExploreGet = rerank
			}
			if property.SearchFunctions.ExploreList != nil {
				searchFunctions.ExploreList = rerank
			}
			property.SearchFunctions = searchFunctions
		}
		out[name] = property
	}
	return out
}

// jevRerank attaches Jev's answer to every result, orders the results as the
// class or the request asks, and drops those below the threshold. The answer
// is a probability for a yes/no question and a position on the rubric for a
// score question, and each has its own threshold.
func jevRerank(ranker *rank.ReRankerProvider) modulecapabilities.AdditionalPropertyFn {
	return func(ctx context.Context, in []search.Result, params any, limit *int,
		argumentModuleParams map[string]any, cfg moduletools.ClassConfig,
	) ([]search.Result, error) {
		rankParams, ok := params.(*rank.Params)
		if !ok {
			return nil, errors.New("wrong parameters")
		}
		// Threshold and order are resolved first so that an invalid one fails
		// the request before any document is sent to the API.
		threshold, err := threshold(ctx, cfg)
		if err != nil {
			return nil, err
		}
		order, err := resultOrder(ctx, cfg)
		if err != nil {
			return nil, err
		}

		scored, err := ranker.Score(ctx, cfg, in, rankParams)
		if err != nil {
			return nil, err
		}
		if order == config.OrderProbability {
			rank.SortByScore(scored)
		}
		if threshold == 0 {
			return scored, nil
		}

		kept := make([]search.Result, 0, len(scored))
		for _, result := range scored {
			score, err := rerankScore(result)
			if err != nil {
				return nil, err
			}
			if score >= threshold {
				kept = append(kept, result)
			}
		}
		return kept, nil
	}
}

// resultOrder returns the order from the request header and falls back to the
// class setting.
func resultOrder(ctx context.Context, cfg moduletools.ClassConfig) (string, error) {
	if header := modulecomponents.GetValueFromContext(ctx, orderHeader); header != "" {
		if !config.ValidOrder(header) {
			return "", fmt.Errorf("%s must be %q or %q, got %q",
				orderHeader, config.OrderSearch, config.OrderProbability, header)
		}
		return header, nil
	}
	order := config.NewClassSettings(cfg).Order()
	if !config.ValidOrder(order) {
		return "", fmt.Errorf("order must be %q or %q, got %q", config.OrderSearch, config.OrderProbability, order)
	}
	return order, nil
}

// threshold returns the lowest rerank score a result needs to be kept: the
// minimum score when the request asks a score question, else the minimum
// probability. Both are checked, so that a malformed header fails the
// request whichever question is asked.
func threshold(ctx context.Context, cfg moduletools.ClassConfig) (float64, error) {
	levels, err := config.ScoreLevels(ctx, cfg)
	if err != nil {
		return 0, err
	}
	probability, err := minProbability(ctx, cfg)
	if err != nil {
		return 0, err
	}
	score, err := minScore(ctx, cfg, len(levels))
	if err != nil {
		return 0, err
	}
	if len(levels) > 0 {
		return score, nil
	}
	return probability, nil
}

// minScore returns the threshold of a score question from the request header
// and falls back to the class setting. It cannot exceed the last level; with
// no levels the bound is the highest position any rubric can have.
func minScore(ctx context.Context, cfg moduletools.ClassConfig, levels int) (float64, error) {
	highest := float64(config.MaxScoreLevels - 1)
	if levels > 0 {
		highest = float64(levels - 1)
	}
	if header := modulecomponents.GetValueFromContext(ctx, minScoreHeader); header != "" {
		value, err := strconv.ParseFloat(header, 64)
		if err != nil || !(value >= 0 && value <= highest) {
			return 0, fmt.Errorf("%s must be a number between 0 and %v, got %q", minScoreHeader, highest, header)
		}
		return value, nil
	}
	value := config.NewClassSettings(cfg).MinScore()
	if !(value >= 0 && value <= highest) {
		return 0, fmt.Errorf("minScore must be between 0 and %v, got %v", highest, value)
	}
	return value, nil
}

// minProbability returns the threshold from the request header and falls back
// to the class setting.
func minProbability(ctx context.Context, cfg moduletools.ClassConfig) (float64, error) {
	if header := modulecomponents.GetValueFromContext(ctx, minProbabilityHeader); header != "" {
		value, err := strconv.ParseFloat(header, 64)
		if err != nil || !config.ValidProbability(value) {
			return 0, fmt.Errorf("%s must be a number between 0 and 1, got %q", minProbabilityHeader, header)
		}
		return value, nil
	}
	value := config.NewClassSettings(cfg).MinProbability()
	if !config.ValidProbability(value) {
		return 0, fmt.Errorf("minProbability must be between 0 and 1, got %v", value)
	}
	return value, nil
}

func rerankScore(result search.Result) (float64, error) {
	scores, ok := result.AdditionalProperties[modulecomponents.AdditionalPropertyRerank].([]*rerankmodels.RankResult)
	if !ok || len(scores) == 0 || scores[0] == nil || scores[0].Score == nil {
		return 0, fmt.Errorf("result %s has no rerank score", result.ID)
	}
	return *scores[0].Score, nil
}
