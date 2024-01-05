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

func (p *ReRankerProvider) getScore(ctx context.Context, cfg moduletools.ClassConfig,
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
		return nil, fmt.Errorf("error ranking with cohere: %w", err)
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

	// sort the list
	sort.Slice(in, func(i, j int) bool {
		apI := in[i].AdditionalProperties["rerank"].([]*rerankmodels.RankResult)
		apJ := in[j].AdditionalProperties["rerank"].([]*rerankmodels.RankResult)

		// Sort in descending order, based on Score values
		return *apI[0].Score > *apJ[0].Score
	})
	return in, nil
}
