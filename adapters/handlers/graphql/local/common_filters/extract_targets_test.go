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
)

func TestTargetExtraction(t *testing.T) {
	cases := []struct {
		name                  string
		source                map[string]interface{}
		expectTargetVectors   []string
		expectCombinationType *dto.TargetCombination
		wantErr               bool
	}{
		{
			name:                  "two target vectors with default",
			source:                map[string]interface{}{"targets": map[string]interface{}{"targetVectors": []interface{}{"a", "b"}}},
			expectTargetVectors:   []string{"a", "b"},
			expectCombinationType: &dto.TargetCombination{Type: dto.Minimum, Weights: []float32{0, 0}},
		},
		{
			name: "two target vectors with min",
			source: map[string]interface{}{
				"targets": map[string]interface{}{
					"targetVectors":     []interface{}{"a", "b"},
					"combinationMethod": dto.Minimum,
				},
			},
			expectTargetVectors:   []string{"a", "b"},
			expectCombinationType: &dto.TargetCombination{Type: dto.Minimum, Weights: []float32{0, 0}},
		},
		{
			name: "two target vectors with sum",
			source: map[string]interface{}{
				"targets": map[string]interface{}{
					"targetVectors":     []interface{}{"a", "b"},
					"combinationMethod": dto.Sum,
				},
			},
			expectTargetVectors:   []string{"a", "b"},
			expectCombinationType: &dto.TargetCombination{Type: dto.Sum, Weights: []float32{1.0, 1.0}},
		},
		{
			name: "two target vectors with average",
			source: map[string]interface{}{
				"targets": map[string]interface{}{
					"targetVectors":     []interface{}{"a", "b"},
					"combinationMethod": dto.Average,
				},
			},
			expectTargetVectors:   []string{"a", "b"},
			expectCombinationType: &dto.TargetCombination{Type: dto.Average, Weights: []float32{0.5, 0.5}},
		},
		{
			name: "two target vectors with manual weights",
			source: map[string]interface{}{
				"targets": map[string]interface{}{
					"targetVectors":     []interface{}{"a", "b"},
					"combinationMethod": dto.ManualWeights,
					"weights":           map[string]interface{}{"a": 0.5, "b": 0.25},
				},
			},
			expectTargetVectors:   []string{"a", "b"},
			expectCombinationType: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{0.5, 0.25}},
		},
		{
			name: "two target vectors with relative score",
			source: map[string]interface{}{
				"targets": map[string]interface{}{
					"targetVectors":     []interface{}{"a", "b"},
					"combinationMethod": dto.RelativeScore,
					"weights":           map[string]interface{}{"a": 0.5, "b": 0.25},
				},
			},
			expectTargetVectors:   []string{"a", "b"},
			expectCombinationType: &dto.TargetCombination{Type: dto.RelativeScore, Weights: []float32{0.5, 0.25}},
		},
		{
			name: "relative score, weights missmatch",
			source: map[string]interface{}{"targets": map[string]interface{}{
				"targetVectors":     []interface{}{"a", "b"},
				"combinationMethod": dto.RelativeScore,
				"weights":           map[string]interface{}{"a": 0.5},
			}},
			wantErr: true,
		},
		{
			name: "manual weights missmatch",
			source: map[string]interface{}{"targets": map[string]interface{}{
				"targetVectors":     []interface{}{"a", "b"},
				"combinationMethod": dto.ManualWeights,
				"weights":           map[string]interface{}{"a": 0.5},
			}},
			wantErr: true,
		},
		{
			name: "combination method type",
			source: map[string]interface{}{"targets": map[string]interface{}{
				"targetVectors":     []interface{}{"a", "b"},
				"combinationMethod": "wrong",
				"weights":           map[string]interface{}{"a": 0.5, "b": 0.25},
			}},
			wantErr: true,
		},
		{
			name: "target vector type",
			source: map[string]interface{}{"targets": map[string]interface{}{
				"targetVectors": "a",
			}},
			wantErr: true,
		},
		{
			name: "target vector entry type",
			source: map[string]interface{}{"targets": map[string]interface{}{
				"targetVectors": []interface{}{"a", 5},
			}},
			wantErr: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			targetVectors, combination, err := ExtractTargets(tt.source)
			if tt.wantErr {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, tt.expectTargetVectors, targetVectors)
				require.Equal(t, tt.expectCombinationType, combination)
			}
		})
	}
}
