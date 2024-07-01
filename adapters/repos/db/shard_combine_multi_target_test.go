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

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
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
		missingElements map[uint64][]string
		targetDistance  float32
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
			missingElements: map[uint64][]string{1: {"target2"}},
		},
		{
			name:            "missing document without target vector that is not searched (weights)",
			targets:         []string{"target1", "target2"},
			joinMethod:      &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 1}},
			in:              [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5)}},
			out:             []search.Result{res(0, 1), res(1, 2.6)},
			missingElements: map[uint64][]string{1: {"target3"}},
		},
		{
			name:            "missing document without target vector (score fusion)",
			targets:         []string{"target1", "target2"},
			joinMethod:      &dto.TargetCombination{Type: dto.RelativeScore, Weights: map[string]float32{"target1": 0.5, "target2": 0.5}},
			in:              [][]search.Result{{res(0, 0.5), res(1, 0.6)}, {res(0, 0.5)}},
			out:             []search.Result{res(0, 1)},
			missingElements: map[uint64][]string{1: {"target2"}},
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
			name:       "many documents (weights) and target Distance",
			targets:    []string{"target1", "target2", "target3", "target4"},
			joinMethod: &dto.TargetCombination{Weights: map[string]float32{"target1": 1, "target2": 0.5, "target3": 0.25, "target4": 0.1}},
			in: [][]search.Result{
				{res(0, 0.5), res(1, 0.6), res(2, 0.8), res(3, 0.9)},
				{res(1, 0.2), res(0, 0.4), res(2, 0.6), res(5, 0.8)},
				{res(1, 0.2), res(2, 0.4), res(3, 0.6), res(4, 0.8)},
				{res(6, 0.1), res(0, 0.3), res(2, 0.7), res(3, 0.9)},
			},
			targetDistance: 1.25,
			out:            []search.Result{res(1, 0.95), res(0, 1.23)},
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
			missingElements: map[uint64][]string{0: {"target3"}},
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
			searcher := fakeS{missingElements: tt.missingElements}

			idsIn := make([][]uint64, len(tt.in))
			distsIn := make([][]float32, len(tt.in))
			for i := range tt.in {
				distsIn[i] = make([]float32, len(tt.in[i]))
				idsIn[i] = make([]uint64, len(tt.in[i]))
				for j := range tt.in[i] {
					distsIn[i][j] = tt.in[i][j].Dist
					idsIn[i][j] = *(tt.in[i][j].DocID)
				}
			}

			limit := len(tt.out)
			if tt.targetDistance > 0 {
				limit = 100
			}

			ids, dists, err := CombineMultiTargetResults(context.Background(), searcher, logger, idsIn, distsIn, tt.targets, searchesVectors[:len(tt.targets)], tt.joinMethod, limit, tt.targetDistance)
			require.Nil(t, err)
			require.Len(t, ids, len(tt.out))
			for i, id := range ids {
				// we do not want to compare ExplainScore etc
				require.Equal(t, *(tt.out[i].DocID), id)
				require.InDelta(t, tt.out[i].Dist, dists[i], 0.0001)
			}
		})
	}
}

type fakeS struct {
	missingElements map[uint64][]string
}

func (f fakeS) VectorDistanceForQuery(ctx context.Context, id uint64, searchVectors [][]float32, targetVectors []string) ([]float32, error) {
	returns := make([]float32, 0, len(targetVectors))
	for range targetVectors {
		returns = append(returns, 2)
	}

	missingTargets, ok := f.missingElements[id]
	if !ok {
		return returns, nil
	}

	for _, missingTarget := range missingTargets {
		for _, target := range targetVectors {
			if target == missingTarget {
				return nil, errors.Errorf("missing target %s", missingTarget)
			}
		}
	}
	return returns, nil
}
