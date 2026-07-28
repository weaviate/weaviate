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

// DiversifyResults runs the terminal MMR pass. relevanceFromDist=true uses raw
// vector distance (pure vector); false uses the normalized fused score.
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

func diversifyResults(ctx context.Context, selection *searchparams.Selection,
	targetVector string, distProv distancer.Provider, results []search.Result, relevanceFromDist bool,
) ([]search.Result, error) {
	if selection == nil || selection.MMR == nil || len(results) == 0 {
		return results, nil
	}

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
	// Both callers keep only the first MMR.Limit output slots, so only the
	// vectored slots within that page need a diversified order. Selection scans
	// all candidates per emitted item, so a full ordering would cost
	// O(window²) distance evaluations instead of O(page × window).
	sel, err := selector.New(selection, distProv.SingleDist, vecForID, pageVectorSlots(hasVector, selection.MMR, len(results)))
	if err != nil {
		return nil, fmt.Errorf("mmr: %w", err)
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
		return nil, fmt.Errorf("mmr: %w", err)
	}

	// Fill the slots past the page with the unselected candidates in their
	// original relevance order, preserving the full-length output contract.
	if len(selectedIDs) < diverseCount {
		chosen := make([]bool, diverseCount)
		for _, id := range selectedIDs {
			chosen[id] = true
		}
		for i := 0; i < diverseCount; i++ {
			if !chosen[i] {
				selectedIDs = append(selectedIDs, uint64(i))
			}
		}
	}

	mmrOrder := make([]search.Result, len(selectedIDs))
	for j, id := range selectedIDs {
		mmrOrder[j] = results[vectoredPos[id]]
	}

	// Vectorless docs keep their fused slot; vectored slots take the diversified order.
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

// pageVectorSlots counts the vectored slots within the first MMR.Limit output
// positions — the number of diversified candidates the page can consume.
// Vectorless results keep their slot, so positions map 1:1 to the input.
func pageVectorSlots(hasVector []bool, mmr *searchparams.SelectionMMR, n int) int {
	limit := n
	if l := int(mmr.Limit); l > 0 && l < limit {
		limit = l
	}
	slots := 0
	for i := 0; i < limit; i++ {
		if hasVector[i] {
			slots++
		}
	}
	return slots
}

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
