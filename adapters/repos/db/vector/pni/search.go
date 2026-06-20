//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package pni

import (
	"math/bits"
	"sort"
)

// Search performs progressive elimination search.
func (idx *Index) Search(query []float32, opts SearchOptions) SearchResult {
	if idx.docCount == 0 {
		return SearchResult{}
	}

	// Encode query
	queryCodes := idx.encoder.Encode(query)

	// Initialize all docs as alive
	alive := make([]int, idx.docCount)
	for i := 0; i < idx.docCount; i++ {
		alive[i] = i
	}
	aliveCount := idx.docCount

	// Accumulated Hamming distances
	accDist := make([]int, idx.docCount)

	// Stats tracking
	stats := SearchStats{
		CandidatesPerSegment: make([]int, 0, idx.segmentCount),
	}

	// Target candidates
	targetCandidates := opts.TargetCandidates
	if targetCandidates <= 0 {
		targetCandidates = 500
	}

	rescoreLimit := opts.RescoreLimit
	if rescoreLimit <= 0 {
		rescoreLimit = targetCandidates
	}

	// Progressive elimination
	for seg := 0; seg < idx.segmentCount; seg++ {
		queryCode := queryCodes[seg]
		segmentData := idx.segments[seg]

		// Compute distances for alive candidates
		for i := 0; i < aliveCount; i++ {
			docID := alive[i]
			accDist[docID] += bits.OnesCount64(queryCode ^ segmentData[docID])
		}
		stats.DistancesComputed += int64(aliveCount)

		// Record candidates before pruning
		stats.CandidatesPerSegment = append(stats.CandidatesPerSegment, aliveCount)

		// Check if we can stop
		dimsProcessed := (seg + 1) * 64
		if dimsProcessed > idx.dims {
			dimsProcessed = idx.dims
		}

		canStop := seg+1 >= opts.MinSegmentsBeforeStop && aliveCount <= targetCandidates
		if canStop {
			stats.SegmentsProcessed = seg + 1
			break
		}

		// Prune if we have more candidates than target
		if aliveCount > targetCandidates {
			aliveCount = pruneToK(alive[:aliveCount], accDist, targetCandidates)
		}

		stats.SegmentsProcessed = seg + 1
	}

	// Limit rescore candidates
	if aliveCount > rescoreLimit {
		aliveCount = pruneToK(alive[:aliveCount], accDist, rescoreLimit)
	}

	stats.FinalCandidates = aliveCount

	// Float rescore
	candidates := alive[:aliveCount]
	result := idx.rescore(query, candidates, opts.K, &stats)
	result.Stats = stats

	return result
}

// SearchAdaptive performs search with adaptive pruning policy.
func (idx *Index) SearchAdaptive(query []float32, policy AdaptivePolicy, k int) SearchResult {
	if idx.docCount == 0 {
		return SearchResult{}
	}

	// Encode query
	queryCodes := idx.encoder.Encode(query)

	// Initialize all docs as alive
	alive := make([]int, idx.docCount)
	for i := 0; i < idx.docCount; i++ {
		alive[i] = i
	}
	aliveCount := idx.docCount

	// Accumulated Hamming distances
	accDist := make([]int, idx.docCount)

	// Stats tracking
	stats := SearchStats{
		CandidatesPerSegment: make([]int, 0, idx.segmentCount),
	}

	// Progressive elimination with adaptive policy
	for seg := 0; seg < idx.segmentCount; seg++ {
		queryCode := queryCodes[seg]
		segmentData := idx.segments[seg]

		// Compute distances for alive candidates
		for i := 0; i < aliveCount; i++ {
			docID := alive[i]
			accDist[docID] += bits.OnesCount64(queryCode ^ segmentData[docID])
		}
		stats.DistancesComputed += int64(aliveCount)

		// Record candidates before pruning
		stats.CandidatesPerSegment = append(stats.CandidatesPerSegment, aliveCount)

		// Compute adaptive target
		dimsProcessed := (seg + 1) * 64
		if dimsProcessed > idx.dims {
			dimsProcessed = idx.dims
		}
		target := computeAdaptiveTarget(policy, dimsProcessed, idx.docCount)

		// Prune if we have more candidates than target
		if aliveCount > target && target > 0 {
			aliveCount = pruneToK(alive[:aliveCount], accDist, target)
		}

		stats.SegmentsProcessed = seg + 1
	}

	stats.FinalCandidates = aliveCount

	// Float rescore
	candidates := alive[:aliveCount]
	result := idx.rescore(query, candidates, k, &stats)
	result.Stats = stats

	return result
}

// rescore performs float distance computation and returns top-k.
func (idx *Index) rescore(query []float32, candidates []int, k int, stats *SearchStats) SearchResult {
	if len(candidates) == 0 {
		return SearchResult{}
	}

	// Compute float distances
	type scoredDoc struct {
		id   uint64
		dist float32
	}
	scoredDocs := make([]scoredDoc, len(candidates))

	for i, docID := range candidates {
		dist := cosineDistance(query, idx.vectors[docID])
		scoredDocs[i] = scoredDoc{uint64(docID), dist}
	}
	stats.RescoresComputed = len(candidates)

	// Sort by distance
	sort.Slice(scoredDocs, func(i, j int) bool {
		return scoredDocs[i].dist < scoredDocs[j].dist
	})

	// Return top-k
	if k > len(scoredDocs) {
		k = len(scoredDocs)
	}

	result := SearchResult{
		IDs:       make([]uint64, k),
		Distances: make([]float32, k),
	}
	for i := 0; i < k; i++ {
		result.IDs[i] = scoredDocs[i].id
		result.Distances[i] = scoredDocs[i].dist
	}

	return result
}

// cosineDistance computes 1 - cos_similarity for normalized vectors.
// For angular distance: dist = 1 - dot(a, b) when vectors are normalized.
func cosineDistance(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return 1.0 - dot
}
