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

package traverser

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
)

func uid(id uint64) strfmt.UUID {
	return strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("%d", id))).String())
}

func res(id uint64, distance float32) search.Result {
	return search.Result{DocID: &id, Dist: distance, ID: uid(id)}
}

func TestCombiner(t *testing.T) {
	logger, _ := test.NewNullLogger()
	searchesVectors := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 1, 0}, {0, 1, 0}} // not relevant for this test

	cases := []struct {
		name            string
		targets         []string
		in              [][]search.Result
		out             []search.Result
		joinMethod      *dto.TargetCombination
		missingElements map[strfmt.UUID][]string
	}{
		{
			name:       "no results (nil)",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:         nil,
			out:        []search.Result{},
		},
		{
			name:       "no results (empty)",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:         [][]search.Result{},
			out:        []search.Result{},
		},
		{
			name:       "single result",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:         [][]search.Result{{res(0, 0.5), res(1, 0.6)}},
			out:        []search.Result{res(0, 0.5), res(1, 0.6)},
		},
		{
			name:       "simple join",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:         [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5), res(1, 0.6)}},
			out:        []search.Result{res(0, 1), res(1, 1.2)},
		},
		{
			name:       "minimum",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Type: dto.Minimum},
			in:         [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5), res(1, 0.6)}},
			out:        []search.Result{res(0, 0.5), res(1, 0.6)},
		},
		{
			name:       "score fusion",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Type: dto.RelativeScore, Weights: map[string]float32{"target1": 0.5, "target2": 0.5}},
			in:         [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5), res(1, 0.6)}},
			out:        []search.Result{res(0, 0), res(1, 1)},
		},
		{
			name:       "score fusion with custom weights",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Type: dto.RelativeScore, Weights: map[string]float32{"target1": 1, "target2": 2}},
			in:         [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5), res(1, 0.6)}},
			out:        []search.Result{res(0, 0), res(1, 3)},
		},
		{
			name:       "missing document without target vector (min)",
			targets:    []string{"target1", "target2"},
			joinMethod: &dto.TargetCombination{Type: dto.Minimum},
			in:         [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5)}},
			out:        []search.Result{res(0, 0.5), res(1, 0.6)},
		},
		{
			name:            "missing document without target vector (weights)",
			targets:         []string{"target1", "target2"},
			joinMethod:      &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:              [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5)}},
			out:             []search.Result{res(0, 1)},
			missingElements: map[strfmt.UUID][]string{uid(1): {"target2"}},
		},
		{
			name:            "missing document without target vector that is not searched (weights)",
			targets:         []string{"target1", "target2"},
			joinMethod:      &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:              [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5)}},
			out:             []search.Result{res(0, 1), res(1, 2.6)},
			missingElements: map[strfmt.UUID][]string{uid(1): {"target3"}},
		},
		{
			name:            "missing document without target vector (score fusion)",
			targets:         []string{"target1", "target2"},
			joinMethod:      &dto.TargetCombination{Type: dto.RelativeScore, Weights: map[string]float32{"target1": 0.5, "target2": 0.5}},
			in:              [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5)}},
			out:             []search.Result{res(0, 1)},
			missingElements: map[strfmt.UUID][]string{uid(1): {"target2"}},
		},
		{
			name:       "many documents (weights)",
			targets:    []string{"target1", "target2", "target3", "target4"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 0.5, "target3": 0.25, "target4": 0.1}},
			in: [][]search.Result{
				{res(0, 0.5), res(1, 0.6), res(2, 0.8), res(3, 0.9)},
				{res(1, 0.2), res(0, 0.4), res(2, 0.6), res(5, 0.8)},
				{res(1, 0.2), res(2, 0.4), res(3, 0.6), res(4, 0.8)},
				{res(6, 0.1), res(0, 0.3), res(2, 0.7), res(3, 0.9)},
			},
			out: []search.Result{res(1, 0.95), res(0, 1.23), res(2, 1.27)},
		},
		{
			name:       "many documents missing entry (weights)",
			targets:    []string{"target1", "target2", "target3", "target4"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 0.5, "target3": 0.25, "target4": 0.1}},
			in: [][]search.Result{
				{res(0, 0.5), res(1, 0.6), res(2, 0.8), res(3, 0.9)},
				{res(1, 0.2), res(0, 0.4), res(2, 0.6), res(5, 0.8)},
				{res(1, 0.2), res(2, 0.4), res(3, 0.6), res(4, 0.8)},
				{res(6, 0.1), res(0, 0.3), res(2, 0.7), res(3, 0.9)},
			},
			out:             []search.Result{res(1, 0.95), res(2, 1.27)},
			missingElements: map[strfmt.UUID][]string{uid(0): {"target3"}},
		},
		{
			name:       "many documents (score fusion)",
			targets:    []string{"target1", "target2", "target3", "target4"},
			joinMethod: &dto.TargetCombination{Type: dto.RelativeScore, Weights: map[string]float32{"target1": 0.25, "target2": 0.25, "target3": 0.25, "target4": 0.25}},
			in: [][]search.Result{
				// 0:0, 1:0.2 2:0.6, 3:1.0
				{res(0, 0.5), res(1, 0.6), res(2, 0.8), res(3, 1.0)},
				// 1:0, 0:0.25, 2:0.75, 3:1.
				{res(1, 0.2), res(0, 0.3), res(2, 0.5), res(3, 0.6)},
				// 1:0, 2:0.1/3, 3:0.2/3, 0:1.
				{res(1, 0.2), res(2, 0.4), res(3, 0.6), res(0, 0.8)},
				// 1:0, 0:0.25, 2:0.75, 3:1.
				{res(1, 0.1), res(0, 0.3), res(2, 0.7), res(3, 0.9)},
			},
			out: []search.Result{res(1, 0.05), res(0, 0.375), res(2, 0.60833)},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			searcher := &fakeVectorSearcher{missingElements: tt.missingElements}

			params := dto.GetParams{TargetVectorCombination: tt.joinMethod, Pagination: &filters.Pagination{Limit: len(tt.out)}}

			results, err := CombineMultiTargetResults(context.Background(), searcher, logger, tt.in, params, tt.targets, searchesVectors[:len(tt.targets)])
			require.Nil(t, err)
			require.Len(t, results, len(tt.out))
			for i, r := range results {
				// we do not want to compare ExplainScore etc
				require.Equal(t, tt.out[i].ID, r.ID)
				require.InDelta(t, tt.out[i].Dist, r.Dist, 0.0001)
			}
		})
	}
}
