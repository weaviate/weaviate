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

package hybrid

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/search"
)

func TestFusionRelativeScore(t *testing.T) {
	cases := []struct {
		weights        []float64
		inputScores    [][]float32
		expectedScores []float32
		expectedOrder  []uint64
	}{
		{weights: []float64{0.5, 0.5}, inputScores: [][]float32{{1, 2, 3}, {0, 1, 2}}, expectedScores: []float32{1, 0.5, 0}, expectedOrder: []uint64{2, 1, 0}},
		{weights: []float64{0.5, 0.5}, inputScores: [][]float32{{0, 2, 0.1}, {0, 0.2, 2}}, expectedScores: []float32{0.55, 0.525, 0}, expectedOrder: []uint64{1, 2, 0}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{0.5, 0.5, 0}, {0, 0.01, 0.001}}, expectedScores: []float32{1, 0.75, 0.025}, expectedOrder: []uint64{1, 0, 2}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{}, {}}, expectedScores: []float32{}, expectedOrder: []uint64{}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1}, {}}, expectedScores: []float32{0.75}, expectedOrder: []uint64{0}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{}, {1}}, expectedScores: []float32{0.25}, expectedOrder: []uint64{0}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1, 2}, {}}, expectedScores: []float32{0.75, 0}, expectedOrder: []uint64{1, 0}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{}, {1, 2}}, expectedScores: []float32{0.25, 0}, expectedOrder: []uint64{1, 0}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1, 1}, {1, 2}}, expectedScores: []float32{1, 0.75}, expectedOrder: []uint64{1, 0}},
		{weights: []float64{1}, inputScores: [][]float32{{1, 2, 3}}, expectedScores: []float32{1, 0.5, 0}, expectedOrder: []uint64{2, 1, 0}},
		{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1, 2, 3, 4}, {1, 2, 3}}, expectedScores: []float32{0.75, 0.75, 0.375, 0}, expectedOrder: []uint64{3, 2, 1, 0}},
		{weights: []float64{0.75, 0.25, 0.1}, inputScores: [][]float32{{1, 2, 3, 4}, {1, 2, 3}, {4, 5}}, expectedScores: []float32{0.75, 0.75, 0.475, 0}, expectedOrder: []uint64{3, 2, 1, 0}},
	}
	for _, tt := range cases {
		t.Run("hybrid fusion", func(t *testing.T) {
			var results [][]*search.Result
			for i := range tt.inputScores {
				var result []*search.Result
				for j, score := range tt.inputScores[i] {
					docId := uint64(j)
					result = append(result, &search.Result{SecondarySortValue: score, DocID: &docId, ID: strfmt.UUID(fmt.Sprint(j))})
				}
				results = append(results, result)
			}
			fused := FusionRelativeScore(tt.weights, results, []string{"set1", "set2", "set2"}, true)
			fusedScores := []float32{} // don't use nil slice declaration, should be explicitly empty
			fusedOrder := []uint64{}

			for _, score := range fused {
				fusedScores = append(fusedScores, score.Score)
				fusedOrder = append(fusedOrder, *score.DocID)
			}

			assert.InDeltaSlice(t, tt.expectedScores, fusedScores, 0.0001)
			assert.Equal(t, tt.expectedOrder, fusedOrder)
		})
	}
}

func TestFusionRelativeScoreExplain(t *testing.T) {
	docId1 := uint64(1)
	docId2 := uint64(2)
	result1 := []*search.Result{
		{DocID: &docId1, SecondarySortValue: 0.5, ID: strfmt.UUID(fmt.Sprint(1))},
		{DocID: &docId2, SecondarySortValue: 0.1, ID: strfmt.UUID(fmt.Sprint(2))},
	}

	result2 := []*search.Result{
		{DocID: &docId1, SecondarySortValue: 2, ID: strfmt.UUID(fmt.Sprint(1))},
		{DocID: &docId2, SecondarySortValue: 1, ID: strfmt.UUID(fmt.Sprint(2))},
	}
	results := [][]*search.Result{result1, result2}
	fused := FusionRelativeScore([]float64{0.5, 0.5}, results, []string{"keyword", "vector"}, true)
	require.Contains(t, fused[0].ExplainScore, "(Result Set keyword) Document 1: original score 0.5, normalized score: 0.5")
	require.Contains(t, fused[0].ExplainScore, "(Result Set vector) Document 1: original score 2, normalized score: 0.5")
}

func TestFusionOrderRelative(t *testing.T) {
	docId1 := uint64(1)
	docId2 := uint64(2)

	result1 := []*search.Result{
		{DocID: &docId1, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(1))},
		{DocID: &docId2, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(2))},
	}

	result2 := []*search.Result{
		{DocID: &docId1, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(1))},
		{DocID: &docId2, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(2))},
	}

	results := [][]*search.Result{result1, result2}

	for _, desc := range []bool{true, false} {
		t.Run(fmt.Sprintf("descending %v", desc), func(t *testing.T) {
			fused1 := FusionRelativeScore([]float64{0.5, 0.5}, results, []string{"keyword", "vector"}, desc)

			// repeat multiple times to ensure order does not flip
			for i := 0; i < 10; i++ {
				fused2 := FusionRelativeScore([]float64{0.5, 0.5}, results, []string{"keyword", "vector"}, desc)
				for j := range fused1 {
					assert.Equal(t, fused1[j], fused2[j])
				}
			}
		})
	}
}

func TestFusionOrderRanked(t *testing.T) {
	docId1 := uint64(1)
	docId2 := uint64(2)

	result1 := []*search.Result{
		{DocID: &docId1, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(1))},
		{DocID: &docId2, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(2))},
	}

	result2 := []*search.Result{
		{DocID: &docId2, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(2))},
		{DocID: &docId1, SecondarySortValue: 0.5, Score: 0.5, ID: strfmt.UUID(fmt.Sprint(1))},
	}

	results := [][]*search.Result{result1, result2}

	fused1 := FusionRanked([]float64{0.5, 0.5}, results, []string{"keyword", "vector"})

	// repeat multiple times to ensure order does not flip
	for i := 0; i < 10; i++ {
		fused2 := FusionRanked([]float64{0.5, 0.5}, results, []string{"keyword", "vector"})
		for j := range fused1 {
			assert.Equal(t, fused1[j], fused2[j])
		}
	}
}
