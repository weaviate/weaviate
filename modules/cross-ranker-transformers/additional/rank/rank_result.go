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

package rank

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	crossrankmodels "github.com/weaviate/weaviate/modules/cross-ranker-transformers/additional/models"
)

func (p *CrossRankerProvider) getScore(ctx context.Context,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	if len(in) == 0 {
		return nil, nil
	} else {
		if params == nil {
			return nil, fmt.Errorf("no params provided")
		}

		rankProperty := params.GetProperty()
		query := params.GetQuery()

		// check if user parameter values are valid
		if len(rankProperty) == 0 {
			return in, errors.New("no properties provided")
		}

		for i := range in { // for each result of the general GraphQL Query
			// get text property
			var rankPropertyValue = ""
			schema := in[i].Object().Properties.(map[string]interface{})
			for property, value := range schema {
				if valueString, ok := value.(string); ok {
					if property == rankProperty {
						rankPropertyValue = valueString
					}
				}
			}

			ap := in[i].AdditionalProperties
			if ap == nil {
				ap = models.AdditionalProperties{}
			}

			result, _ := p.client.Rank(ctx, rankPropertyValue, query) // didn't implement error yet
			ap["crossrank"] = []*crossrankmodels.RankResult{
				{
					Score: &result.Score,
				},
			}
			in[i].AdditionalProperties = ap
		}
	}
	//sort the list
	sort.Slice(in, func(i, j int) bool {
		apI := in[i].AdditionalProperties["crossrank"].([]*crossrankmodels.RankResult)
		apJ := in[j].AdditionalProperties["crossrank"].([]*crossrankmodels.RankResult)

		// Sort in descending order, based on Score values
		return *apI[0].Score > *apJ[0].Score
	})
	return in, nil
}

func (p *CrossRankerProvider) containsProperty(property string, properties []string) bool {
	if len(properties) == 0 {
		return true
	}
	for i := range properties {
		if properties[i] == property {
			return true
		}
	}
	return false
}
