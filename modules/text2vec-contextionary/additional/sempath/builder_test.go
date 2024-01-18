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

package sempath

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
)

func TestSemanticPathBuilder(t *testing.T) {
	t.Skip("go1.20 change")
	c11y := &fakeC11y{}
	b := New(c11y)

	b.fixedSeed = 1000 // control randomness in unit test

	input := []search.Result{
		{
			ID:        "7fe919ed-2ef6-4087-856c-a307046bf895",
			ClassName: "Foo",
			Vector:    []float32{1, 0.1},
		},
	}
	searchVector := []float32{0.3, 0.3}

	c11y.neighbors = []*txt2vecmodels.NearestNeighbors{
		{
			Neighbors: []*txt2vecmodels.NearestNeighbor{
				{
					Concept: "good1",
					Vector:  []float32{0.5, 0.1},
				},
				{
					Concept: "good2",
					Vector:  []float32{0.7, 0.2},
				},
				{
					Concept: "good3",
					Vector:  []float32{0.9, 0.1},
				},
				{
					Concept: "good4",
					Vector:  []float32{0.55, 0.1},
				},
				{
					Concept: "good5",
					Vector:  []float32{0.77, 0.2},
				},
				{
					Concept: "good6",
					Vector:  []float32{0.99, 0.1},
				},
				{
					Concept: "bad1",
					Vector:  []float32{-0.1, -3},
				},
				{
					Concept: "bad2",
					Vector:  []float32{-0.15, -2.75},
				},
				{
					Concept: "bad3",
					Vector:  []float32{-0.22, -2.35},
				},
				{
					Concept: "bad4",
					Vector:  []float32{0.1, -3.3},
				},
				{
					Concept: "bad5",
					Vector:  []float32{0.15, -2.5},
				},
				{
					Concept: "bad6",
					Vector:  []float32{-0.4, -2.25},
				},
			},
		},
	}

	res, err := b.CalculatePath(input, &Params{SearchVector: searchVector})
	require.Nil(t, err)

	expectedPath := &txt2vecmodels.SemanticPath{
		Path: []*txt2vecmodels.SemanticPathElement{
			{
				Concept:          "good5",
				DistanceToNext:   ptFloat32(0.00029218197),
				DistanceToQuery:  0.13783735,
				DistanceToResult: 0.011904657,
			},
			{
				Concept:            "good2",
				DistanceToNext:     ptFloat32(0.014019072),
				DistanceToPrevious: ptFloat32(0.00029218197),
				DistanceToQuery:    0.12584269,
				DistanceToResult:   0.015912116,
			},
			{
				Concept:            "good3",
				DistanceToNext:     ptFloat32(4.9889088e-05),
				DistanceToPrevious: ptFloat32(0.014019072),
				DistanceToQuery:    0.21913117,
				DistanceToResult:   6.0379505e-05,
			},
			{
				Concept:            "good6",
				DistanceToNext:     ptFloat32(0.0046744347),
				DistanceToPrevious: ptFloat32(4.9889088e-05),
				DistanceToQuery:    0.2254098,
				DistanceToResult:   5.364418e-07,
			},
			{
				Concept:            "good1",
				DistanceToNext:     ptFloat32(0.00015383959),
				DistanceToPrevious: ptFloat32(0.0046744347),
				DistanceToQuery:    0.16794968,
				DistanceToResult:   0.004771471,
			},
			{
				Concept:            "good4",
				DistanceToPrevious: ptFloat32(0.00015383959),
				DistanceToQuery:    0.17780781,
				DistanceToResult:   0.003213048,
			},
		},
	}

	require.Len(t, res, 1)
	require.NotNil(t, res[0].AdditionalProperties)
	semanticPath, semanticPathOK := res[0].AdditionalProperties["semanticPath"]
	assert.True(t, semanticPathOK)
	semanticPathElement, semanticPathElementOK := semanticPath.(*txt2vecmodels.SemanticPath)
	assert.True(t, semanticPathElementOK)
	assert.Equal(t, expectedPath, semanticPathElement)
}

type fakeC11y struct {
	neighbors []*txt2vecmodels.NearestNeighbors
}

func (f *fakeC11y) MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*txt2vecmodels.NearestNeighbors, error) {
	return f.neighbors, nil
}

func ptFloat32(in float32) *float32 {
	return &in
}
