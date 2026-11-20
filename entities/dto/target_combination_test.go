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

package dto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTargetCombination(t *testing.T) {
	two := []string{"a", "b"}
	tests := []struct {
		name    string
		method  string
		targets []string
		weights map[string]float64
		want    *TargetCombination
		wantErr string
	}{
		{name: "no method, no target", want: nil},
		{name: "no method, one target", targets: []string{"a"}, want: nil},
		{name: "no method, two targets defaults", targets: two, want: &TargetCombination{Type: DefaultTargetCombinationType}},
		{name: "no method with weights", targets: two, weights: map[string]float64{"a": 1}, wantErr: "weights are only allowed with manualWeights or relativeScore"},
		{name: "sum", method: "sum", targets: two, want: &TargetCombination{Type: Sum, Weights: []float32{1, 1}}},
		{name: "average", method: "average", targets: two, want: &TargetCombination{Type: Average, Weights: []float32{0.5, 0.5}}},
		{name: "average on three targets", method: "average", targets: []string{"a", "b", "c"}, want: &TargetCombination{Type: Average, Weights: []float32{1.0 / 3, 1.0 / 3, 1.0 / 3}}},
		{name: "minimum", method: "minimum", targets: two, want: &TargetCombination{Type: Minimum}},
		{name: "sum on one target", method: "sum", targets: []string{"a"}, want: &TargetCombination{Type: Sum, Weights: []float32{1}}},
		{name: "sum with weights", method: "sum", targets: two, weights: map[string]float64{"a": 1, "b": 2}, wantErr: "weights are only allowed with manualWeights or relativeScore"},
		{name: "average with weights", method: "average", targets: two, weights: map[string]float64{"a": 1}, wantErr: "weights are only allowed"},
		{name: "minimum with weights", method: "minimum", targets: two, weights: map[string]float64{"a": 1}, wantErr: "weights are only allowed"},
		{
			name: "manualWeights positional in target order", method: "manualWeights", targets: []string{"b", "a"},
			weights: map[string]float64{"a": 0.25, "b": 0.75},
			want:    &TargetCombination{Type: ManualWeights, Weights: []float32{0.75, 0.25}},
		},
		{
			name: "relativeScore", method: "relativeScore", targets: two,
			weights: map[string]float64{"a": 1, "b": 2},
			want:    &TargetCombination{Type: RelativeScore, Weights: []float32{1, 2}},
		},
		{name: "manualWeights missing weight", method: "manualWeights", targets: two, weights: map[string]float64{"a": 1}, wantErr: `weight for target "b" not provided`},
		{name: "manualWeights no weights", method: "manualWeights", targets: two, wantErr: `weight for target "a" not provided`},
		{name: "relativeScore missing weight", method: "relativeScore", targets: two, weights: map[string]float64{"b": 1}, wantErr: `weight for target "a" not provided`},
		{name: "unknown weight key", method: "manualWeights", targets: two, weights: map[string]float64{"a": 1, "b": 1, "c": 1}, wantErr: `"c", which is not a target vector`},
		{name: "weight overflows float32", method: "manualWeights", targets: two, weights: map[string]float64{"a": 1, "b": 1e300}, wantErr: `weight for target "b" does not fit a 32-bit float`},
		{name: "unknown method", method: "max", targets: two, wantErr: `unknown target combination method "max"`},
		{name: "method is case sensitive", method: "Sum", targets: two, wantErr: `unknown target combination method "Sum"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTargetCombination(tt.method, tt.targets, tt.weights)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
