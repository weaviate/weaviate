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

package common_filters

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/usecases/config"
)

func ExtractHybridSearch(source map[string]interface{}) (*searchparams.HybridSearch, error) {
	var subsearches []interface{}
	operands_i := source["operands"]
	if operands_i != nil {
		operands := operands_i.([]interface{})
		for _, operand := range operands {
			operandMap := operand.(map[string]interface{})
			subsearches = append(subsearches, operandMap)
		}
	}

	var weightedSearchResults []searchparams.WeightedSearchResult
	var args searchparams.HybridSearch
	for _, ss := range subsearches {
		subsearch := ss.(map[string]interface{})
		switch {
		case subsearch["sparseSearch"] != nil:
			bm25 := subsearch["sparseSearch"].(map[string]interface{})
			arguments := ExtractBM25(bm25)

			weightedSearchResults = append(weightedSearchResults, searchparams.WeightedSearchResult{
				SearchParams: arguments,
				Weight:       subsearch["weight"].(float64),
				Type:         "bm25",
			})
		case subsearch["nearText"] != nil:
			nearText := subsearch["nearText"].(map[string]interface{})
			arguments, _ := ExtractNearText(nearText)

			weightedSearchResults = append(weightedSearchResults, searchparams.WeightedSearchResult{
				SearchParams: arguments,
				Weight:       subsearch["weight"].(float64),
				Type:         "nearText",
			})

		case subsearch["nearVector"] != nil:
			nearVector := subsearch["nearVector"].(map[string]interface{})
			arguments, _ := ExtractNearVector(nearVector)

			weightedSearchResults = append(weightedSearchResults, searchparams.WeightedSearchResult{
				SearchParams: arguments,
				Weight:       subsearch["weight"].(float64),
				Type:         "nearVector",
			})

		default:
			return nil, fmt.Errorf("unknown subsearch type: %+v", subsearch)
		}
	}

	args.SubSearches = weightedSearchResults
	limit_i := source["limit"]
	if limit_i != nil {
		args.Limit = int(limit_i.(int))
	}

	alpha, ok := source["alpha"]
	if ok {
		args.Alpha = alpha.(float64)
	} else {
		args.Alpha = config.DefaultAlpha
	}

	if args.Alpha < 0 || args.Alpha > 1 {
		return nil, fmt.Errorf("alpha should be between 0.0 and 1.0")
	}

	query, ok := source["query"]
	if ok {
		args.Query = query.(string)
	}

	if _, ok := source["vector"]; ok {
		vector := source["vector"].([]interface{})
		args.Vector = make([]float32, len(vector))
		for i, value := range vector {
			args.Vector[i] = float32(value.(float64))
		}
	}

	args.Type = "hybrid"
	return &args, nil
}
