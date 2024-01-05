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

package vectorizer

import (
	"reflect"
	"testing"
)

func TestCosineSimilarity(t *testing.T) {
	type args struct {
		a, b []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			"Distance between identical vectors",
			args{
				a: []float32{1, 2, 3},
				b: []float32{1, 2, 3},
			},
			1.0,
		},
		{
			"Distance between similar vectors",
			args{
				a: []float32{1, 2, 3},
				b: []float32{2, 3, 4},
			},
			0.99258333,
		},
		{
			"Distance between opposite vectors",
			args{
				a: []float32{0, 1},
				b: []float32{0, -1},
			},
			-1.0,
		},
		{
			"Distance between perpendicular vectors",
			args{
				a: []float32{0, 1},
				b: []float32{1, 0},
			},
			0.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := cosineSim(tt.args.a, tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CombineVectors() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCosineSim_DifferentDimensions(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5}
	_, err := cosineSim(a, b)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func BenchmarkCosineSimilarity(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cosineSim([]float32{-0.214571, -0.605529, -0.335769, -0.185277, -0.212256, 0.478032, -0.536662, 0.298211},
			[]float32{-0.14713769, -0.06872862, 0.09911085, -0.06342313, 0.10092197, -0.06624051, -0.06812558, 0.07360107},
		)
	}
}
