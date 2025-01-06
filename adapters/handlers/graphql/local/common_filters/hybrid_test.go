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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestHybrid(t *testing.T) {
	var nilweights []float32
	var ss []searchparams.WeightedSearchResult
	cases := []struct {
		input             map[string]interface{}
		output            *searchparams.HybridSearch
		outputCombination *dto.TargetCombination
		error             bool
	}{
		{
			input:             map[string]interface{}{"vector": []float32{1.0, 2.0, 3.0}},
			output:            &searchparams.HybridSearch{Vector: []float32{1.0, 2.0, 3.0}, SubSearches: ss, Type: "hybrid", Alpha: 0.75, FusionAlgorithm: 1},
			outputCombination: nil,
		},
		{
			input:             map[string]interface{}{"vector": []float32{1.0, 2.0, 3.0}, "targetVectors": []interface{}{"target1", "target2"}},
			output:            &searchparams.HybridSearch{Vector: []float32{1.0, 2.0, 3.0}, TargetVectors: []string{"target1", "target2"}, SubSearches: ss, Type: "hybrid", Alpha: 0.75, FusionAlgorithm: 1},
			outputCombination: &dto.TargetCombination{Type: dto.Minimum, Weights: nilweights},
		},
		{
			input:             map[string]interface{}{"targetVectors": []interface{}{"target1", "target2"}, "searches": []interface{}{map[string]interface{}{"nearVector": map[string]interface{}{"vector": []float32{float32(1.0), float32(2.0), float32(3.0)}}}}},
			output:            &searchparams.HybridSearch{NearVectorParams: &searchparams.NearVector{Vectors: []models.Vector{[]float32{1, 2, 3}, []float32{1, 2, 3}}}, TargetVectors: []string{"target1", "target2"}, SubSearches: ss, Type: "hybrid", Alpha: 0.75, FusionAlgorithm: 1},
			outputCombination: &dto.TargetCombination{Type: dto.Minimum, Weights: nilweights},
		},
		{
			input:             map[string]interface{}{"targetVectors": []interface{}{"target1", "target2"}, "searches": []interface{}{map[string]interface{}{"nearVector": map[string]interface{}{"vectorPerTarget": map[string]interface{}{"target1": []float32{1.0, 2.0, 3.0}, "target2": []float32{1.0, 2.0}}}}}},
			output:            &searchparams.HybridSearch{NearVectorParams: &searchparams.NearVector{Vectors: []models.Vector{[]float32{1, 2, 3}, []float32{1, 2}}}, TargetVectors: []string{"target1", "target2"}, SubSearches: ss, Type: "hybrid", Alpha: 0.75, FusionAlgorithm: 1},
			outputCombination: &dto.TargetCombination{Type: dto.Minimum, Weights: nilweights},
		},
	}

	for _, tt := range cases {
		t.Run("near vector", func(t *testing.T) {
			hybrid, outputCombination, err := ExtractHybridSearch(tt.input, false)
			if tt.error {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, tt.output, hybrid)
				require.Equal(t, tt.outputCombination, outputCombination)
			}
		})
	}
}
