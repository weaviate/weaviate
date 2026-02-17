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
	"math"
)

const (
	numBins          = 5
	efUpperBound     = 5000 // ef=5000 is used as an upper bound for max practical recall
	numSampleQueries = 250
	calibrationK     = 10 // by default we brute force k=10
	adaptiveMinEf    = 64 // min ef during discovery phase
)

// Pre calculated z-scores for quantiles 0.001, 0.002, 0.003, 0.004, 0.005 from inverse normal CDF.
// These are the left-tail z-scores (negative values for small quantiles).
var quantileZScores = [numBins]float64{
	-3.0902, // 0.001
	-2.8782, // 0.002
	-2.7478, // 0.003
	-2.6521, // 0.004
	-2.5758, // 0.005
}

var binWeights = [numBins]float64{
	100.0,
	100.0 * math.Exp(-1),
	100.0 * math.Exp(-2),
	100.0 * math.Exp(-3),
	100.0 * math.Exp(-4),
}

// adaptiveEfConfig holds precomputed statistics and the ef-estimation table.
type adaptiveEfConfig struct {
	MeanVec           []float64      `json:"meanVec"`
	VarianceVec       []float64      `json:"varianceVec"`
	TargetRecall      float32        `json:"targetRecall"`
	WeightedAverageEf int            `json:"weightedAverageEf"`
	Table             []efTableEntry `json:"table"` // sorted by score
	Links             [101]int       `json:"-"`     // rebuilt via buildSketch()
}

// efTableEntry holds the ef-recall pairs for a given integer score.
type efTableEntry struct {
	Score      int        `json:"score"`
	QueryCount int        `json:"queryCount"`
	EFRecalls  []efRecall `json:"efRecalls"`
}

// efRecall records the recall achieved at a given ef value.
type efRecall struct {
	EF     int     `json:"ef"`
	Recall float32 `json:"recall"`
}

// computeScore computes the query difficulty score for a query vector
// given a set of distances collected during the initial search phase.
// For cosine distance (bottom-based, lower is better):
//
//	mean_dist = 1 - dot(q, meanVec)
//	var_dist = dot(q^2, varianceVec)
//	std = sqrt(var_dist)
//	Bin thresholds at quantiles using z-scores from the standard normal.
//	Count distances below each threshold, compute weighted score.
func computeScore(queryVec []float32, distances []float32, meanVec, varianceVec []float64) float32 {
	if len(distances) == 0 {
		return 0
	}

	dims := len(queryVec)
	if dims == 0 {
		return 0
	}

	// Compute mean distance and variance for cosine distance.
	// For cosine distance with normalized vectors: dist = 1 - dot(q, v)
	// Expected distance: E[dist] = 1 - dot(q, meanVec)
	// Variance: Var[dist] = dot(q^2, varianceVec)
	var dotQMean float64
	var varDist float64
	for d := 0; d < dims; d++ {
		qd := float64(queryVec[d])
		dotQMean += qd * meanVec[d]
		varDist += qd * qd * varianceVec[d]
	}
	meanDist := 1.0 - dotQMean
	std := math.Sqrt(varDist)

	if std < 1e-12 {
		return 0
	}

	// Compute bin thresholds: threshold[i] = meanDist + zScore[i] * std
	// Since z-scores are negative, thresholds are below the mean (left tail).
	var thresholds [numBins]float64
	for i := 0; i < numBins; i++ {
		thresholds[i] = meanDist + quantileZScores[i]*std
	}

	// Count distances falling below each threshold.
	// Bins are checked from the tightest (smallest threshold) to loosest.
	// A distance counted in bin i is NOT counted in bin i+1 (first match wins).
	var counts [numBins]int
	n := len(distances)
	for _, d := range distances {
		fd := float64(d)
		for i := 0; i < numBins; i++ {
			if fd < thresholds[i] {
				counts[i]++
				break
			}
		}
	}

	// Weighted score: sum of (count[i] / n) * weight[i]
	var score float64
	fn := float64(n)
	for i := 0; i < numBins; i++ {
		score += (float64(counts[i]) / fn) * binWeights[i]
	}

	return float32(score)
}

// estimateAdaptiveEf looks up the estimated ef for a given score using the sketch.
// It averages the ef values for the neighboring integer scores for smoothing.
func (cfg *adaptiveEfConfig) estimateAdaptiveEf(score float32) int {
	intScore := int(score)
	if intScore < 0 {
		intScore = 0
	}
	if intScore > 100 {
		intScore = 100
	}

	if intScore < 1 || intScore >= 100 {
		return cfg.lookupAdaptiveEf(intScore)
	}

	// Average of neighbors for smoothing
	ef1 := cfg.lookupAdaptiveEf(intScore - 1)
	ef2 := cfg.lookupAdaptiveEf(intScore)
	ef3 := cfg.lookupAdaptiveEf(intScore + 1)
	return (ef1 + ef2 + ef3) / 3
}

// lookupAdaptiveEf returns the ef value for a given integer score using the table.
func (cfg *adaptiveEfConfig) lookupAdaptiveEf(intScore int) int {
	if intScore < 0 {
		intScore = 0
	}
	if intScore > 100 {
		intScore = 100
	}

	idx := cfg.Links[intScore]
	if idx < 0 || idx >= len(cfg.Table) {
		return cfg.WeightedAverageEf
	}

	entry := cfg.Table[idx]
	for _, er := range entry.EFRecalls {
		if er.Recall >= cfg.TargetRecall {
			ef := er.EF
			if ef < cfg.WeightedAverageEf {
				ef = cfg.WeightedAverageEf
			}
			return ef
		}
	}

	// If no ef achieved the target recall, return the last (highest) ef
	if len(entry.EFRecalls) > 0 {
		return entry.EFRecalls[len(entry.EFRecalls)-1].EF
	}
	return cfg.WeightedAverageEf
}

// buildSketch builds the Links array that maps each integer score 0-100
// to the nearest entry in the table.
func (cfg *adaptiveEfConfig) buildSketch() {
	if len(cfg.Table) == 0 {
		return
	}

	for s := 0; s <= 100; s++ {
		bestIdx := 0
		bestDist := abs(s - cfg.Table[0].Score)
		for i := 1; i < len(cfg.Table); i++ {
			d := abs(s - cfg.Table[i].Score)
			if d < bestDist {
				bestDist = d
				bestIdx = i
			}
		}
		cfg.Links[s] = bestIdx
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// statisticsLength computes the number of initial nodes to visit
// before computing the difficulty score. This is approximately:
// 1 + M0 + (M0-1) * M0
// where M0 is the max connections at layer zero.
func statisticsLength(maxConnectionsLayerZero int) int {
	m0 := maxConnectionsLayerZero
	return 1 + m0 + (m0-1)*m0
}

// adaptiveStatisticsLength computes the statistics length based on target recall.
// For lower recall targets, we can use fewer samples to reduce unnecessary
// distance computations, but we must be conservative to maintain scoring accuracy.
func adaptiveStatisticsLength(maxConnectionsLayerZero int, targetRecall float32) int {
	m0 := maxConnectionsLayerZero
	fullTwoHop := 1 + m0 + (m0-1)*m0

	// Full 2-hop for high recall (>= 0.95)
	if targetRecall >= 0.95 {
		return fullTwoHop
	}

	// For medium recall (0.90-0.95), use 2-hop but only explore half the neighbors
	// This balances accuracy and performance
	if targetRecall >= 0.90 {
		return 1 + m0 + (m0-1)*m0/2
	}

	// For lower recall (< 0.90), use 1.5-hop
	return 1 + m0 + m0/2
}

// adaptiveSearchParams computes the initialEF and statsLen for an adaptive
// search given the calibrated config and index parameters.
func adaptiveSearchParams(cfg *adaptiveEfConfig, maxConnectionsLayerZero, k int) (int, int) {
	initialEF := int(float64(cfg.WeightedAverageEf))
	if initialEF < adaptiveMinEf {
		initialEF = adaptiveMinEf
	}
	if initialEF < k {
		initialEF = k
	}
	if initialEF > efUpperBound {
		initialEF = efUpperBound
	}

	statsLen := adaptiveStatisticsLength(maxConnectionsLayerZero, cfg.TargetRecall)
	return initialEF, statsLen
}

// underlyingVectorIndex is an optional interface that wrapped indexes
// (e.g. dynamic index) can implement to expose the underlying index.
type underlyingVectorIndex interface {
	UnderlyingVectorIndex() interface{}
}
