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

package hnsw

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

const maxBruteForceVectors = 50_000

// BuildAdaptiveEFTable runs the offline calibration to build the adaptive ef table.
// sampleQueries: sample query vectors (typically 200)
// groundTruth: brute-force top-k results for each sample query (docIDs)
// k: number of nearest neighbors used for ground truth
// targetRecall: the desired recall level (e.g. 0.95)
// meanVec, varianceVec: per-dimension statistics of the dataset
func (h *hnsw) BuildAdaptiveEFTable(ctx context.Context,
	sampleQueries [][]float32, groundTruth [][]uint64,
	k int, targetRecall float32,
	meanVec, varianceVec []float64,
) error {
	if len(sampleQueries) == 0 {
		return fmt.Errorf("no sample queries provided")
	}
	if len(sampleQueries) != len(groundTruth) {
		return fmt.Errorf("sample queries and ground truth must have the same length")
	}

	numQueries := len(sampleQueries)
	statsLen := statisticsLength(h.maximumConnectionsLayerZero)

	// Phase 1: Run all queries at ef=k to collect scores.
	type queryInfo struct {
		index int
		score float32
	}
	queries := make([]queryInfo, numQueries)
	for i := 0; i < numQueries; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		searchVec := h.normalizeVec(sampleQueries[i])
		ac := &adaptiveConfig{StatsLen: statsLen}
		_, _, err := h.knnSearchByVector(ctx, searchVec, k, k, nil, ac)
		if err != nil {
			return fmt.Errorf("score collection query=%d: %w", i, err)
		}
		score := computeScore(searchVec, ac.CollectedDistances, meanVec, varianceVec)
		queries[i] = queryInfo{index: i, score: score}
	}

	// Phase 2: Group queries by integer score (0-100), matching the paper's
	// per-score binning for fine-grained EF calibration.
	scoreGroups := make(map[int][]queryInfo)
	for _, q := range queries {
		s := int(q.score)
		if s < 0 {
			s = 0
		}
		if s > 100 {
			s = 100
		}
		scoreGroups[s] = append(scoreGroups[s], q)
	}

	scoreKeys := make([]int, 0, len(scoreGroups))
	for s := range scoreGroups {
		scoreKeys = append(scoreKeys, s)
	}
	sort.Ints(scoreKeys)

	// Phase 3: For each score group, progressively increase ef until average recall >= target.
	var table []efTableEntry
	for _, binScore := range scoreKeys {
		binQueries := scoreGroups[binScore]

		var efRecalls []efRecall
		ef := k
		for ef <= efUpperBound {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			var totalRecall float32
			for _, qi := range binQueries {
				searchVec := h.normalizeVec(sampleQueries[qi.index])
				ids, _, err := h.knnSearchByVector(ctx, searchVec, k, ef, nil, nil)
				if err != nil {
					return fmt.Errorf("search at ef=%d query=%d: %w", ef, qi.index, err)
				}
				totalRecall += computeRecall(ids, groundTruth[qi.index], k)
			}
			avgRecall := totalRecall / float32(len(binQueries))

			efRecalls = append(efRecalls, efRecall{EF: ef, Recall: avgRecall})

			if avgRecall >= targetRecall {
				break
			}

			// Increase ef: 1.5x growth for finer granularity
			nextEF := int(math.Ceil(float64(ef) * 1.5))
			if nextEF <= ef {
				nextEF = ef + 1
			}
			ef = nextEF
		}

		table = append(table, efTableEntry{
			Score:     binScore,
			EFRecalls: efRecalls,
		})

		h.logger.WithField("score", binScore).
			WithField("queries", len(binQueries)).
			WithField("final_ef", efRecalls[len(efRecalls)-1].EF).
			WithField("final_recall", efRecalls[len(efRecalls)-1].Recall).
			Info("adaptive ef: score group")
	}

	sort.Slice(table, func(i, j int) bool {
		return table[i].Score < table[j].Score
	})

	wae := computeWAE(table, targetRecall)

	cfg := &adaptiveEfConfig{
		MeanVec:      meanVec,
		VarianceVec:  varianceVec,
		TargetRecall: targetRecall,
		WAE:          wae,
		Table:        table,
	}
	cfg.buildSketch()

	h.logger.WithField("wae", wae).WithField("num_bins", len(table)).Info("adaptive ef: calibration summary")
	for _, entry := range table {
		h.logger.WithField("score", entry.Score).
			WithField("estimated_ef", cfg.estimateEf(float32(entry.Score))).
			Info("adaptive ef: table entry")
	}

	h.adaptiveEf.Store(cfg)

	if err := h.StoreAdaptiveEFConfig(cfg); err != nil {
		h.logger.WithError(err).Warn("failed to persist adaptive ef config to metadata")
	}

	return nil
}

// computeRecall computes the recall of a search result against ground truth.
func computeRecall(resultIDs []uint64, groundTruth []uint64, k int) float32 {
	if k == 0 || len(groundTruth) == 0 {
		return 0
	}

	gtSet := make(map[uint64]struct{}, k)
	limit := k
	if limit > len(groundTruth) {
		limit = len(groundTruth)
	}
	for _, id := range groundTruth[:limit] {
		gtSet[id] = struct{}{}
	}

	hits := 0
	for _, id := range resultIDs {
		if _, ok := gtSet[id]; ok {
			hits++
		}
	}

	return float32(hits) / float32(limit)
}

// computeWAE computes the weighted average ef: the average of the minimum ef
// achieving the target recall across all score groups, weighted by the number
// of queries in each group (approximated here as uniform).
func computeWAE(table []efTableEntry, targetRecall float32) int {
	if len(table) == 0 {
		return calibrationK
	}

	var totalEF int
	var count int
	for _, entry := range table {
		minEF := 0
		for _, er := range entry.EFRecalls {
			if er.Recall >= targetRecall {
				minEF = er.EF
				break
			}
			minEF = er.EF // fallback to last
		}
		if minEF > 0 {
			totalEF += minEF
			count++
		}
	}

	if count == 0 {
		return calibrationK
	}
	return totalEF / count
}

// sampleNodeIDs samples up to n valid (non-nil, non-tombstone) node IDs from
// h.nodes using a linear congruential generator (LCG) to iterate a pseudo-random
// permutation of [0, m) where m is the next power of 2 >= nodesLen. Values >= nodesLen
// are skipped. This avoids allocating a slice over the entire node array (which may
// be hundreds of millions of entries) and uses O(1) state with no seen map.
//
// The LCG has full period m by the Hull-Dobell theorem: m is a power of 2,
// c is odd (coprime to m), and a ≡ 1 (mod 4).
func sampleNodeIDs(h *hnsw, rng *rand.Rand, nodesLen int, n int) []uint64 {
	if n > nodesLen {
		n = nodesLen
	}

	// m = next power of 2 >= nodesLen
	m := uint64(1)
	for m < uint64(nodesLen) {
		m <<= 1
	}

	// LCG parameters: x_{n+1} = (a*x + c) mod m
	// a ≡ 1 (mod 4), c is odd → guarantees full period m.
	a := uint64(4*rng.Intn(int(m)/4+1)) + 1 // random a ≡ 1 (mod 4)
	c := uint64(2*rng.Intn(int(m)/2+1)) + 1 // random odd c
	x := uint64(rng.Intn(int(m)))           // random starting point

	ids := make([]uint64, 0, n)
	for range m {
		x = (a*x + c) & (m - 1) // mod m via bitmask since m is power of 2

		if x >= uint64(nodesLen) {
			continue
		}

		id := x
		h.shardedNodeLocks.RLock(id)
		exists := int(id) < len(h.nodes) && h.nodes[id] != nil
		h.shardedNodeLocks.RUnlock(id)

		if exists && !h.hasTombstone(id) {
			ids = append(ids, id)
			if len(ids) >= n {
				return ids
			}
		}
	}

	return ids
}

// CalibrateAdaptiveEF is the self-contained calibration method that performs
// all phases: sampling vectors, computing statistics, brute-force ground truth,
// and building the adaptive ef table.
func (h *hnsw) CalibrateAdaptiveEF(ctx context.Context, targetRecall float32) error {
	if h.adaptiveEfCalibrating.Load() {
		// Already marked in-progress by the caller — just ensure cleanup.
	} else if !h.adaptiveEfCalibrating.CompareAndSwap(false, true) {
		return fmt.Errorf("calibration already in progress")
	}
	defer h.adaptiveEfCalibrating.Store(false)

	rng := rand.New(rand.NewSource(42))
	k := calibrationK

	// Phase 1: Sample node IDs directly from h.nodes using random probing.
	// This avoids allocating a slice of all node IDs which could be huge (500M+).
	h.RLock()
	nodesLen := len(h.nodes)
	h.RUnlock()

	if nodesLen < 2 {
		return fmt.Errorf("not enough vectors to calibrate adaptive ef (need at least 2, got %d)", nodesLen)
	}

	bruteForceIDs := sampleNodeIDs(h, rng, nodesLen, maxBruteForceVectors)
	if len(bruteForceIDs) < 2 {
		return fmt.Errorf("not enough valid vectors to calibrate adaptive ef (got %d)", len(bruteForceIDs))
	}

	sampleN := numSampleQueries
	if sampleN > len(bruteForceIDs) {
		sampleN = len(bruteForceIDs)
	}
	sampleIDs := bruteForceIDs[:sampleN]

	h.logger.
		WithField("nodes_len", nodesLen).
		WithField("sample_queries", len(sampleIDs)).
		WithField("brute_force_pool", len(bruteForceIDs)).
		Info("adaptive ef: phase 1 complete, sampled node IDs")

	// Phase 2: Compute statistics (mean/variance) using Welford's online algorithm
	var dims int
	var meanVec []float64
	var m2Vec []float64
	statsCount := 0

	for _, id := range bruteForceIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		vec, err := h.vectorForID(ctx, id)
		if err != nil || len(vec) == 0 {
			continue
		}

		if statsCount == 0 {
			dims = len(vec)
			meanVec = make([]float64, dims)
			m2Vec = make([]float64, dims)
		}

		statsCount++
		for d := 0; d < dims; d++ {
			delta := float64(vec[d]) - meanVec[d]
			meanVec[d] += delta / float64(statsCount)
			delta2 := float64(vec[d]) - meanVec[d]
			m2Vec[d] += delta * delta2
		}
	}

	if statsCount < 2 {
		return fmt.Errorf("not enough valid vectors for statistics (got %d)", statsCount)
	}

	varianceVec := make([]float64, dims)
	for d := 0; d < dims; d++ {
		varianceVec[d] = m2Vec[d] / float64(statsCount)
	}

	h.logger.
		WithField("stats_count", statsCount).
		WithField("dims", dims).
		Info("adaptive ef: phase 2 complete, computed statistics")

	// Phase 3: Compute approximate ground truth using HNSW search at max ef.
	// Using ef=efUpperBound gives high recall and avoids brute-forcing
	// the entire index.
	sampleQueries := make([][]float32, 0, len(sampleIDs))
	groundTruth := make([][]uint64, 0, len(sampleIDs))

	for _, qID := range sampleIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		queryVec, err := h.vectorForID(ctx, qID)
		if err != nil || len(queryVec) == 0 {
			continue
		}
		queryVec = h.normalizeVec(queryVec)

		gt, _, err := h.knnSearchByVector(ctx, queryVec, k, efUpperBound, nil, nil)
		if err != nil {
			continue
		}

		sampleQueries = append(sampleQueries, queryVec)
		groundTruth = append(groundTruth, gt)
	}

	if len(sampleQueries) == 0 {
		return fmt.Errorf("no valid sample queries after ground truth computation")
	}

	h.logger.
		WithField("valid_queries", len(sampleQueries)).
		Info("adaptive ef: phase 3 complete, computed ground truth")

	// Phase 4: Build the adaptive ef table
	return h.BuildAdaptiveEFTable(ctx, sampleQueries, groundTruth, k, targetRecall, meanVec, varianceVec)
}
