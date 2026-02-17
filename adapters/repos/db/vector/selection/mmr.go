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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// mmr implements Maximal Marginal Relevance for diversifying search results.
// It greedily selects k items from ids that balance relevance (low query
// distance) with diversity (high distance from already-selected items).
//
// Scoring formula per candidate:
//
//	score = -lambda * queryDist + (1-lambda) * minDistToSelected
//
// lambda=1 gives pure relevance ranking, lambda=0 gives pure diversity.
func mmr(
	ctx context.Context,
	provider distancer.Provider,
	vecForID common.TempVectorForIDWithView[float32],
	ids []uint64,
	queryDistances []float32,
	k int,
	lambda float64,
	view common.BucketView,
) ([]uint64, []float32, error) {
	n := len(ids)
	if n == 0 || k <= 0 {
		return nil, nil, nil
	}
	if k > n {
		k = n
	}

	// Phase 1: Load all candidate vectors into contiguous memory.
	// A single VectorSlice is reused across all disk reads to minimize allocations.
	container := &common.VectorSlice{Buff8: make([]byte, 8)}

	firstVec, err := vecForID(ctx, ids[0], container, view)
	if err != nil {
		return nil, nil, err
	}
	dim := len(firstVec)

	vectors := make([]float32, n*dim)
	copy(vectors[:dim], firstVec)

	for i := 1; i < n; i++ {
		vec, err := vecForID(ctx, ids[i], container, view)
		if err != nil {
			return nil, nil, err
		}
		copy(vectors[i*dim:], vec)
	}

	// Phase 2: Greedy MMR selection.

	// First selection: lowest query distance (most relevant).
	// When S is empty the diversity term is undefined, so we pick by pure relevance.
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

	lastSelectedVec := vectors[bestIdx*dim : (bestIdx+1)*dim]
	lambdaF := float32(lambda)
	oneMinusLambdaF := float32(1 - lambda)

	// Greedily select k-1 more candidates. Each iteration fuses the
	// diversity distance update (against the last selected vector) with
	// scoring, so we touch each remaining candidate exactly once per round.
	for s := 1; s < k; s++ {
		bestScore := -float32(math.MaxFloat32)
		bestIdx = -1

		for i := range n {
			if removed[i] {
				continue
			}

			// Update min diversity distance against the last selected candidate.
			dist, err := provider.SingleDist(lastSelectedVec, vectors[i*dim:(i+1)*dim])
			if err != nil {
				return nil, nil, err
			}
			if dist < minDist[i] {
				minDist[i] = dist
			}

			// MMR score: prefer low query distance (relevant) and high min
			// distance to selected set (diverse).
			score := -lambdaF*queryDistances[i] + oneMinusLambdaF*minDist[i]
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
		lastSelectedVec = vectors[bestIdx*dim : (bestIdx+1)*dim]
	}

	return selected, selectedDists, nil
}
