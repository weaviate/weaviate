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

package nearDepth

import (
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/entities/dto"
)

func Test_extractNearDepthFn(t *testing.T) {
	type args struct {
		source map[string]interface{}
	}
	tests := []struct {
		name       string
		args       args
		want       interface{}
		wantTarget *dto.TargetCombination
	}{
		{
			name: "should extract properly with distance and depth params set",
			args: args{
				source: map[string]interface{}{
					"depth":    "base64;encoded",
					"distance": float64(0.9),
				},
			},
			want: &NearDepthParams{
				Depth:        "base64;encoded",
				Distance:     0.9,
				WithDistance: true,
			},
		},
		{
			name: "should extract properly with certainty and depth params set",
			args: args{
				source: map[string]interface{}{
					"depth":     "base64;encoded",
					"certainty": float64(0.9),
				},
			},
			want: &NearDepthParams{
				Depth:     "base64;encoded",
				Certainty: 0.9,
			},
		},
		{
			name: "should extract properly with only depth set",
			args: args{
				source: map[string]interface{}{
					"depth": "base64;encoded",
				},
			},
			want: &NearDepthParams{
				Depth: "base64;encoded",
			},
		},
		{
			name: "should extract properly with depth and targetVectors set",
			args: args{
				source: map[string]interface{}{
					"depth":         "base64;encoded",
					"targetVectors": []interface{}{"targetVector1"},
				},
			},
			want: &NearDepthParams{
				Depth:         "base64;encoded",
				TargetVectors: []string{"targetVector1"},
			},
			wantTarget: &dto.TargetCombination{Type: dto.Minimum},
		},
		{
			name: "should extract properly with depth and targets set",
			args: args{
				source: map[string]interface{}{
					"depth": "base64;encoded",
					"targets": map[string]interface{}{
						"targetVectors":     []interface{}{"targetVector1", "targetVector2"},
						"combinationMethod": dto.ManualWeights,
						"weights":           map[string]interface{}{"targetVector1": 0.5, "targetVector2": 0.5},
					},
				},
			},
			want: &NearDepthParams{
				Depth:         "base64;encoded",
				TargetVectors: []string{"targetVector1", "targetVector2"},
			},
			wantTarget: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{0.5, 0.5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, target, err := extractNearDepthFn(tt.args.source)
			if !reflect.DeepEqual(got, tt.want) || !reflect.DeepEqual(target, tt.wantTarget) || err != nil {
				t.Errorf("extractNearDepthFn() = %v, want %v", got, tt.want)
			}
		})
	}
}
