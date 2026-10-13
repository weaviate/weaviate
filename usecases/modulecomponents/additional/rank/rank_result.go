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

package rank

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	rerankmodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

// getScore scores the results and sorts them by score, highest first. Results
// with the same score keep the order the search gave them.
func (p *ReRankerProvider) getScore(ctx context.Context, cfg moduletools.ClassConfig,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	scored, err := p.Score(ctx, cfg, in, params)
	if err != nil || len(scored) == 0 {
		return scored, err
	}
	SortByScore(scored)
	return scored, nil
}

// SortByScore sorts scored results by their rerank score, highest first.
// Results with the same score keep their order.
func SortByScore(scored []search.Result) {
	sort.SliceStable(scored, func(i, j int) bool {
		apI := scored[i].AdditionalProperties["rerank"].([]*rerankmodels.RankResult)
		apJ := scored[j].AdditionalProperties["rerank"].([]*rerankmodels.RankResult)
		return *apI[0].Score > *apJ[0].Score
	})
}

// Score attaches the reranker's score to every result as the "rerank"
// additional property and keeps the order of the results.
func (p *ReRankerProvider) Score(ctx context.Context, cfg moduletools.ClassConfig,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	if len(in) == 0 {
		return nil, nil
	}
	if params == nil {
		return nil, fmt.Errorf("no params provided")
	}

	rankProperty := params.GetProperty()
	query := params.GetQuery()

	// check if user parameter values are valid
	if len(rankProperty) == 0 {
		return in, errors.New("no properties provided")
	}

	documents := make([]string, len(in))
	for i := range in { // for each result of the general GraphQL Query
		// get text property
		rankPropertyValue := ""
		schema := in[i].Object().Properties.(map[string]interface{})
		for property, value := range schema {
			if property == rankProperty {
				if valueString, ok := value.(string); ok {
					rankPropertyValue = valueString
				}
			}
		}
		documents[i] = rankPropertyValue
	}

	// rank results
	result, err := p.client.Rank(ctx, query, documents, cfg)
	if err != nil {
		return nil, fmt.Errorf("client rank: %w", err)
	}

	// add scores to results
	for i := range in {
		if in[i].AdditionalProperties == nil {
			in[i].AdditionalProperties = models.AdditionalProperties{}
		}
		in[i].AdditionalProperties["rerank"] = []*rerankmodels.RankResult{
			{
				Score: &result.DocumentScores[i].Score,
			},
		}
	}

	return in, nil
}
