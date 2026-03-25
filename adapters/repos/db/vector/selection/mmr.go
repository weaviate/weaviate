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

package selection

import (
	"context"
	"math"
)

// Scoring formula:
//
//	score = -lambda * queryDist + (1-lambda) * minDistToSelected
//
// lambda=1 pure relevance ranking, lambda=0 pure diversity.
type MMRSelector struct {
	distFn   func(a, b []float32) (float32, error)
	vecForID func(ctx context.Context, id uint64) ([]float32, error)
	k        int
	lambda   float32
}

func newMMRSelector(distFn func(a, b []float32) (float32, error), vecForID func(ctx context.Context, id uint64) ([]float32, error), k int, lambda float32) *MMRSelector {
	return &MMRSelector{distFn: distFn, vecForID: vecForID, k: k, lambda: lambda}
}

func (s *MMRSelector) Select(ctx context.Context, ids []uint64, queryDistances []float32) ([]uint64, []float32, error) {
	n := len(ids)
	k := s.k
	if n == 0 || k <= 0 {
		return nil, nil, nil
	}
	if k > n {
		k = n
	}

	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec, err := s.vecForID(ctx, ids[i])
		if err != nil {
			return nil, nil, err
		}
		vectors[i] = vec
	}

	bestIdx := 0
	for i := 1; i < n; i++ {
		if queryDistances[i] < queryDistances[bestIdx] {
			bestIdx = i
		}
	}

	selected := make([]uint64, 1, k)
	selectedDists := make([]float32, 1, k)
	selected[0] = ids[bestIdx]
	selectedDists[0] = queryDistances[bestIdx]

	if k == 1 {
		return selected, selectedDists, nil
	}

	removed := make([]bool, n)
	removed[bestIdx] = true

	// minDist[i] tracks the minimum distance from candidate i to any
	// already-selected candidate. Initialized to +Inf so the first update
	// is a simple overwrite.
	minDist := make([]float32, n)
	for i := range minDist {
		minDist[i] = float32(math.Inf(1))
	}

	lastSelectedVec := vectors[bestIdx]

	// Greedily select k-1 more candidates
	for round := 1; round < k; round++ {
		bestScore := -float32(math.MaxFloat32)
		bestIdx = -1

		for i := 0; i < n; i++ {
			if removed[i] {
				continue
			}

			dist, err := s.distFn(lastSelectedVec, vectors[i])
			if err != nil {
				return nil, nil, err
			}
			if dist < minDist[i] {
				minDist[i] = dist
			}

			// MMR score: prefer low query distance (relevant) and high min
			// distance to selected set (diverse).
			score := -s.lambda*queryDistances[i] + (1-s.lambda)*minDist[i]
			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}

		if bestIdx == -1 {
			break
		}

		selected = append(selected, ids[bestIdx])
		selectedDists = append(selectedDists, queryDistances[bestIdx])
		removed[bestIdx] = true
		lastSelectedVec = vectors[bestIdx]
	}

	return selected, selectedDists, nil
}
