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

package distancer

import (
	"math"
	"testing"
)

var normalizeTests = []struct {
	name     string
	input    []float32
	expected []float32
}{
	{
		name:     "normal vector",
		input:    []float32{3, 4},
		expected: []float32{0.6, 0.8},
	},
	{
		name:     "zero vector",
		input:    []float32{0, 0, 0},
		expected: []float32{0, 0, 0},
	},
	{
		name:     "single element",
		input:    []float32{5},
		expected: []float32{1},
	},
	{
		name:     "negative values",
		input:    []float32{-3, -4},
		expected: []float32{-0.6, -0.8},
	},
	{
		name:     "already normalized",
		input:    []float32{0.6, 0.8},
		expected: []float32{0.6, 0.8},
	},
	{
		name:     "very large numbers",
		input:    []float32{1e16, 1e16, 1e16},
		expected: []float32{0.577350269, 0.577350269, 0.577350269},
	},
	{
		name:     "very small numbers",
		input:    []float32{1e-16, 1e-16, 1e-16},
		expected: []float32{0.577350269, 0.577350269, 0.577350269},
	},
	{
		name:     "mixed scales",
		input:    []float32{1e16, 1e-16, 1},
		expected: []float32{1, 0, 0},
	},
}

func compareVectors(t *testing.T, got, want []float32) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("length mismatch: got %d, want %d", len(got), len(want))
		return
	}
	for i := range got {
		if math.Abs(float64(got[i]-want[i])) > 1e-6 {
			t.Errorf("at index %d: got %f, want %f", i, got[i], want[i])
		}
	}
}

func TestNormalize(t *testing.T) {
	for _, tt := range normalizeTests {
		t.Run(tt.name, func(t *testing.T) {
			result := Normalize(tt.input)
			compareVectors(t, result, tt.expected)
		})
	}
}

func TestNormalizeInPlace(t *testing.T) {
	for _, tt := range normalizeTests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of the input slice to preserve the original
			input := make([]float32, len(tt.input))
			copy(input, tt.input)

			NormalizeInPlace(input)
			compareVectors(t, input, tt.expected)
		})
	}
}
