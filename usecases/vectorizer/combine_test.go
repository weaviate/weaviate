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

func TestCombineVectors(t *testing.T) {
	type args struct {
		vectors [][]float32
	}
	tests := []struct {
		name string
		args args
		want []float32
	}{
		{
			"Combine simple vectors",
			args{
				vectors: [][]float32{
					{1, 2, 3},
					{2, 3, 4},
				},
			},
			[]float32{1.5, 2.5, 3.5},
		},
		{
			"Combine empty vectors",
			args{
				vectors: [][]float32{},
			},
			[]float32{},
		},
		{
			"Combine more complex vectors",
			args{
				vectors: [][]float32{
					{-0.214571, -0.605529, -0.335769, -0.185277, -0.212256, 0.478032, -0.536662, 0.298211},
					{-0.14713769, -0.06872862, 0.09911085, -0.06342313, 0.10092197, -0.06624051, -0.06812558, 0.07360107},
					{-0.18123996, -0.2089612, 0.03738429, -0.26224917, 0.18499854, -0.2620146, -0.12802331, -0.07601682},
					{-0.06659584, -0.17120242, 0.07603133, -0.07171547, 0.12537181, -0.19367254, -0.18376349, -0.05517439},
				},
			},
			[]float32{-0.15238613, -0.2636053, -0.030810636, -0.1456662, 0.049759082, -0.010973915, -0.2291436, 0.060155217},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CombineVectors(tt.args.vectors); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CombineVectors() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCombineVectorsWithWeights(t *testing.T) {
	type args struct {
		vectors [][]float32
		weights []float32
	}
	tests := []struct {
		name string
		args args
		want []float32
	}{
		{
			"Combine simple vectors with 0 weights",
			args{
				vectors: [][]float32{
					{1, 2, 3},
					{2, 3, 4},
				},
				weights: []float32{0, 0, 0},
			},
			[]float32{0, 0, 0},
		},
		{
			"Combine simple vectors with 1 weights",
			args{
				vectors: [][]float32{
					{1, 2, 3},
					{2, 3, 4},
				},
				weights: []float32{1, 1, 1},
			},
			[]float32{1.5, 2.5, 3.5},
		},
		{
			"Combine empty vectors",
			args{
				vectors: [][]float32{},
				weights: []float32{},
			},
			[]float32{},
		},
		{
			"Combine simple vectors without weights",
			args{
				vectors: [][]float32{
					{1, 2, 3},
					{2, 3, 4},
				},
				weights: nil,
			},
			[]float32{1.5, 2.5, 3.5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CombineVectorsWithWeights(tt.args.vectors, tt.args.weights); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CombineVectors() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkCombine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CombineVectors([][]float32{
			{-0.214571, -0.605529, -0.335769, -0.185277, -0.212256, 0.478032, -0.536662, 0.298211},
			{-0.14713769, -0.06872862, 0.09911085, -0.06342313, 0.10092197, -0.06624051, -0.06812558, 0.07360107},
			{-0.18123996, -0.2089612, 0.03738429, -0.26224917, 0.18499854, -0.2620146, -0.12802331, -0.07601682},
			{-0.06659584, -0.17120242, 0.07603133, -0.07171547, 0.12537181, -0.19367254, -0.18376349, -0.05517439},
		})
	}
}
