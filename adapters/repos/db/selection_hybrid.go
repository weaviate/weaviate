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

// DiversifyResults runs a post-fusion MMR pass over an already-fused hybrid
// result set. Unlike the pure-vector coordinator pass (applySelectionOnObjects),
// the relevance signal here is the fused hybrid score rather than a raw query
// distance, so the two cannot share an implementation.
//
// Candidates without a usable vector for targetVector (e.g. BM25-only hits in a
// collection where some objects were never vectorized) cannot be placed in the
// vector space. They are excluded from the diversity computation and re-merged
// at their original fused rank, so a high-relevance keyword hit is never demoted
// purely for lacking a vector.
//
// results must already be sorted by descending fused score. The returned slice
// is truncated to selection.MMR.Limit.
func (db *DB) DiversifyResults(ctx context.Context, selection *searchparams.Selection,
	className, targetVector string, results []search.Result,
) ([]search.Result, error) {
	if selection == nil || selection.MMR == nil || len(results) == 0 {
		return results, nil
	}

	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, fmt.Errorf("hybrid mmr: index for class %q not found", className)
	}
	distProv := distancerForConfig(idx.GetVectorIndexConfig(targetVector))
	return diversifyResults(ctx, selection, targetVector, distProv, results)
}

// diversifyResults holds the distancer-agnostic MMR re-ranking logic so it can
// be unit-tested without a live index. See DiversifyResults for semantics.
func diversifyResults(ctx context.Context, selection *searchparams.Selection,
	targetVector string, distProv distancer.Provider, results []search.Result,
) ([]search.Result, error) {
	if selection == nil || selection.MMR == nil || len(results) == 0 {
		return results, nil
	}

	k := int(selection.MMR.Limit)
	if k <= 0 {
		return results, nil
	}

	// Partition: vectoredPos holds indices into results that carry a usable
	// vector; the aligned vecs slice holds those vectors for direct lookup.
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

	// Nothing diversifiable: keep fused order, just truncate.
	if diverseCount == 0 {
		return truncateResults(results, k), nil
	}

	// Relevance term for MMR is derived from the fused score, normalized to
	// [0,1] across the vectored set and inverted into a "distance" (lower =
	// more relevant).
	queryDists := normalizedScoreDistances(results, vectoredPos)

	// Rank ALL vectored candidates so vectorless docs can later consume some of
	// the k output slots. selector.New bakes k from selection.MMR.Limit, so we
	// clone the Selection with the limit widened to the full vectored count and
	// truncate to the real k during the re-merge below.
	rankAll := *selection
	mmrAll := *selection.MMR
	mmrAll.Limit = uint32(diverseCount)
	rankAll.MMR = &mmrAll

	vecForID := func(_ context.Context, id uint64) ([]float32, error) {
		return vecs[id], nil
	}
	sel, err := selector.New(&rankAll, distProv.SingleDist, vecForID, diverseCount)
	if err != nil {
		return nil, fmt.Errorf("hybrid mmr: %w", err)
	}
	if sel == nil {
		return truncateResults(results, k), nil
	}

	ids := make([]uint64, diverseCount)
	for i := range ids {
		ids[i] = uint64(i)
	}
	selectedIDs, _, err := sel.Select(ctx, ids, queryDists)
	if err != nil {
		return nil, fmt.Errorf("hybrid mmr: %w", err)
	}

	// mmrOrder is the diversified sequence of vectored results.
	mmrOrder := make([]search.Result, len(selectedIDs))
	for j, id := range selectedIDs {
		mmrOrder[j] = results[vectoredPos[id]]
	}

	// Re-merge: walk the original fused order. Vectorless docs keep their slot;
	// vectored slots are filled from the diversified order. Stop at k.
	out := make([]search.Result, 0, k)
	next := 0
	for i := range results {
		if len(out) >= k {
			break
		}
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

// resultVector extracts the []float32 vector for targetVector from a result, if
// present. Multi-vectors ([][]float32) are reported as absent: MMR's distancer
// operates on []float32 only, so such candidates are treated as vectorless.
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

// normalizedScoreDistances builds the MMR relevance term for the vectored
// subset: fused scores min-max normalized to [0,1] then inverted so that lower
// values mean higher relevance. When all scores are equal, every candidate gets
// distance 0 (equally relevant), leaving diversity as the sole signal.
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

func truncateResults(results []search.Result, k int) []search.Result {
	if k > 0 && len(results) > k {
		return results[:k]
	}
	return results
}
