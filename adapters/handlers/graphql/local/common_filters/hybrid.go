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

package common_filters

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/entities/searchparams"
)

const DefaultAlpha = float64(0.75)
const (
	HybridRankedFusion = iota
	HybridRelativeScoreFusion
)
const HybridFusionDefault = HybridRelativeScoreFusion

func ExtractHybridSearch(source map[string]interface{}, explainScore bool) (*searchparams.HybridSearch, *dto.TargetCombination, error) {
	var subsearches []interface{}
	operandsI := source["operands"]
	if operandsI != nil {
		operands := operandsI.([]interface{})
		for _, operand := range operands {
			operandMap := operand.(map[string]interface{})
			subsearches = append(subsearches, operandMap)
		}
	}
	var args searchparams.HybridSearch
	targetVectors, combination, err := ExtractTargets(source)
	if err != nil {
		return &searchparams.HybridSearch{}, nil, err
	}
	args.TargetVectors = targetVectors

	namedSearchesI := source["searches"]
	if namedSearchesI != nil {
		namedSearchess := namedSearchesI.([]interface{})
		namedSearches := namedSearchess[0].(map[string]interface{})
		// TODO: add bm25 here too
		if namedSearches["nearText"] != nil {
			nearText := namedSearches["nearText"].(map[string]interface{})
			arguments, _ := ExtractNearText(nearText)

			args.NearTextParams = &arguments
		}

		if namedSearches["nearVector"] != nil {
			nearVector := namedSearches["nearVector"].(map[string]interface{})
			arguments, _, _ := ExtractNearVector(nearVector, targetVectors)
			// targetvectors need to be set in the hybrid search to be handled correctly, return an error if not set
			if targetVectors == nil && arguments.TargetVectors != nil {
				return nil, nil, fmt.Errorf("targetVectors need to be set in the hybrid search to be handled correctly")
			}

			args.NearVectorParams = &arguments

		}
	}

	var weightedSearchResults []searchparams.WeightedSearchResult

	for _, ss := range subsearches {
		subsearch := ss.(map[string]interface{})
		switch {
		case subsearch["sparseSearch"] != nil:
			bm25 := subsearch["sparseSearch"].(map[string]interface{})
			arguments := ExtractBM25(bm25, explainScore)

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
			arguments, _, _ := ExtractNearVector(nearVector, targetVectors)

			weightedSearchResults = append(weightedSearchResults, searchparams.WeightedSearchResult{
				SearchParams: arguments,
				Weight:       subsearch["weight"].(float64),
				Type:         "nearVector",
			})

		default:
			return nil, nil, fmt.Errorf("unknown subsearch type: %+v", subsearch)
		}
	}

	args.SubSearches = weightedSearchResults

	alpha, ok := source["alpha"]
	if ok {
		args.Alpha = alpha.(float64)
	} else {
		args.Alpha = DefaultAlpha
	}
	if args.Alpha < 0 || args.Alpha > 1 {
		return nil, nil, fmt.Errorf("alpha should be between 0.0 and 1.0")
	}

	vectorDistanceCutOff, ok := source["maxVectorDistance"]
	if ok {
		args.Distance = float32(vectorDistanceCutOff.(float64))
		args.WithDistance = true
	} else {
		args.WithDistance = false
	}

	query, ok := source["query"]
	if ok {
		args.Query = query.(string)
	}

	fusionType, ok := source["fusionType"]
	if ok {
		args.FusionAlgorithm = fusionType.(int)
	} else {
		args.FusionAlgorithm = HybridFusionDefault
	}

	switch vector := source["vector"].(type) {
	case nil:
		args.Vector = nil
	case []float32, [][]float32, models.C11yVector:
		args.Vector = vector
	case []interface{}:
		v := make([]float32, len(vector))
		for i, value := range vector {
			v[i] = float32(value.(float64))
		}
		args.Vector = v
	default:
		return nil, nil, fmt.Errorf("cannot parse vector: unrecognized vector type: %T", source["vector"])
	}

	if _, ok := source["properties"]; ok {
		properties := source["properties"].([]interface{})
		args.Properties = make([]string, len(properties))
		for i, value := range properties {
			args.Properties[i] = value.(string)
		}
	}

	operator, ok := source["bm25SearchOperator"]
	if ok {
		operator := operator.(map[string]interface{})
		args.SearchOperator = operator["operator"].(string)
		args.MinimumOrTokensMatch = int(operator["minimumOrTokensMatch"].(int))
	}

	args.Type = "hybrid"

	if args.NearTextParams != nil && args.NearVectorParams != nil {
		return nil, nil, fmt.Errorf("hybrid search cannot have both nearText and nearVector parameters")
	}
	if args.Vector != nil && args.NearTextParams != nil {
		return nil, nil, fmt.Errorf("cannot have both vector and nearTextParams")
	}
	if args.Vector != nil && args.NearVectorParams != nil {
		return nil, nil, fmt.Errorf("cannot have both vector and nearVectorParams")
	}

	return &args, combination, nil
}
