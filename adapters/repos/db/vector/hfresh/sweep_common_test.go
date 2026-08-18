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

//go:build budgetsweep || lottesweep
// +build budgetsweep lottesweep

// Helpers shared by the budget-sweep and LOTTE-sweep diagnostic benchmarks.

package hfresh

// computeRecall returns recall@k of results against groundTruth.
func computeRecall(results []uint64, groundTruth []uint64, k int) float64 {
	if len(groundTruth) == 0 || len(results) == 0 {
		return 0.0
	}

	truthSet := make(map[uint64]bool)
	limit := k
	if limit > len(groundTruth) {
		limit = len(groundTruth)
	}
	for i := 0; i < limit; i++ {
		truthSet[groundTruth[i]] = true
	}

	hits := 0
	resultLimit := k
	if resultLimit > len(results) {
		resultLimit = len(results)
	}
	for i := 0; i < resultLimit; i++ {
		if truthSet[results[i]] {
			hits++
		}
	}

	return float64(hits) / float64(limit)
}
