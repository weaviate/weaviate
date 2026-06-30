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

package db

import (
	"context"
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	selector "github.com/weaviate/weaviate/adapters/repos/db/vector/selection"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// DiversifyResults runs the terminal MMR pass over an already-ranked candidate
// pool (post-fusion/retrieval and post-boost). It is the single MMR entry point
// for both the hybrid and pure-vector paths. relevanceFromDist selects the MMR
// relevance signal: true uses the raw vector distance on each result (pure vector
// search, no boost), false uses the min-max-normalized score (hybrid fusion, or
// whenever boost has re-scored the results). Vectorless candidates keep their rank.
func (db *DB) DiversifyResults(ctx context.Context, selection *searchparams.Selection,
	className, targetVector string, results []search.Result, relevanceFromDist bool,
) ([]search.Result, error) {
	if selection == nil || selection.MMR == nil || len(results) == 0 {
		return results, nil
	}

	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, fmt.Errorf("mmr: index for class %q not found", className)
	}
	distProv := distancerForConfig(idx.GetVectorIndexConfig(targetVector))
	return diversifyResults(ctx, selection, targetVector, distProv, results, relevanceFromDist)
}

// diversifyResults is the distancer-agnostic MMR core, testable without a live
// index. It returns the FULL diversified ordering of results; the caller sizes
// the candidate pool (its input) and paginates the output.
func diversifyResults(ctx context.Context, selection *searchparams.Selection,
	targetVector string, distProv distancer.Provider, results []search.Result, relevanceFromDist bool,
) ([]search.Result, error) {
	if selection == nil || selection.MMR == nil || len(results) == 0 {
		return results, nil
	}

	// vectoredPos[i] indexes into results; vecs[i] is its aligned vector.
	vectoredPos := make([]int, 0, len(results))
	vecs := make([][]float32, 0, len(results))
	hasVector := make([]bool, len(results))
	for i := range results {
		vec, ok := resultVector(&results[i], targetVector)
		if !ok {
			continue
		}
		hasVector[i] = true
		vectoredPos = append(vectoredPos, i)
		vecs = append(vecs, vec)
	}

	diverseCount := len(vectoredPos)

	if diverseCount == 0 {
		return results, nil
	}

	var queryDists []float32
	if relevanceFromDist {
		queryDists = make([]float32, len(vectoredPos))
		for i, pos := range vectoredPos {
			queryDists[i] = results[pos].Dist
		}
	} else {
		queryDists = normalizedScoreDistances(results, vectoredPos)
	}

	vecForID := func(_ context.Context, id uint64) ([]float32, error) {
		return vecs[id], nil
	}
	sel, err := selector.New(selection, distProv.SingleDist, vecForID, diverseCount)
	if err != nil {
		return nil, fmt.Errorf("hybrid mmr: %w", err)
	}
	if sel == nil {
		return results, nil
	}

	ids := make([]uint64, diverseCount)
	for i := range ids {
		ids[i] = uint64(i)
	}
	selectedIDs, _, err := sel.Select(ctx, ids, queryDists)
	if err != nil {
		return nil, fmt.Errorf("hybrid mmr: %w", err)
	}

	mmrOrder := make([]search.Result, len(selectedIDs))
	for j, id := range selectedIDs {
		mmrOrder[j] = results[vectoredPos[id]]
	}

	// Re-merge: vectorless docs keep their fused slot; vectored slots take the diversified order.
	out := make([]search.Result, 0, len(results))
	next := 0
	for i := range results {
		if hasVector[i] {
			if next < len(mmrOrder) {
				out = append(out, mmrOrder[next])
				next++
			}
		} else {
			out = append(out, results[i])
		}
	}
	return out, nil
}

// resultVector returns the []float32 for targetVector; multi-vectors are reported absent since MMR's distancer is []float32-only.
func resultVector(r *search.Result, targetVector string) ([]float32, bool) {
	if targetVector == "" {
		if len(r.Vector) > 0 {
			return r.Vector, true
		}
		return nil, false
	}
	v, ok := r.Vectors[targetVector]
	if !ok || v == nil {
		return nil, false
	}
	vec, ok := v.([]float32)
	if !ok || len(vec) == 0 {
		return nil, false
	}
	return vec, true
}

// normalizedScoreDistances min-max normalizes fused scores to [0,1] then inverts them into distances (lower = more relevant); equal scores yield 0.
func normalizedScoreDistances(results []search.Result, vectoredPos []int) []float32 {
	dists := make([]float32, len(vectoredPos))
	minScore := float32(math.MaxFloat32)
	maxScore := -float32(math.MaxFloat32)
	for _, pos := range vectoredPos {
		s := results[pos].Score
		if s < minScore {
			minScore = s
		}
		if s > maxScore {
			maxScore = s
		}
	}
	rangeScore := maxScore - minScore
	for i, pos := range vectoredPos {
		if rangeScore > 0 {
			norm := (results[pos].Score - minScore) / rangeScore
			dists[i] = 1 - norm
		} else {
			dists[i] = 0
		}
	}
	return dists
}
