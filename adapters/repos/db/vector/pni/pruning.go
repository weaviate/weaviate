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

// pruneToK prunes candidates to keep only the best k by accumulated distance.
// Uses quickselect (partial sort) for O(n) average complexity.
// Returns the new count of alive candidates.
//
// Parameters:
//   - alive: slice of document IDs that are still candidates
//   - accDist: accumulated distances for all documents (indexed by docID)
//   - k: number of candidates to keep
//
// After this call, alive[0:k] contains the k best candidates (unordered).
func pruneToK(alive []int, accDist []int, k int) int {
	n := len(alive)
	if n <= k {
		return n
	}

	// Quickselect to find the k-th smallest distance
	// After this, alive[0:k] will contain candidates with distance <= pivot
	quickselectK(alive, accDist, 0, n-1, k)

	return k
}

// quickselectK partially sorts alive so that the k smallest elements
// (by accDist) are in alive[0:k].
func quickselectK(alive []int, accDist []int, left, right, k int) {
	for left < right {
		pivotIdx := partition(alive, accDist, left, right)

		if pivotIdx == k {
			return
		} else if pivotIdx < k {
			left = pivotIdx + 1
		} else {
			right = pivotIdx - 1
		}
	}
}

// partition partitions alive[left:right+1] around a pivot.
// Returns the final position of the pivot.
func partition(alive []int, accDist []int, left, right int) int {
	// Use median-of-three pivot selection for better performance
	mid := left + (right-left)/2

	// Sort left, mid, right
	if accDist[alive[left]] > accDist[alive[mid]] {
		alive[left], alive[mid] = alive[mid], alive[left]
	}
	if accDist[alive[mid]] > accDist[alive[right]] {
		alive[mid], alive[right] = alive[right], alive[mid]
	}
	if accDist[alive[left]] > accDist[alive[mid]] {
		alive[left], alive[mid] = alive[mid], alive[left]
	}

	// Use mid as pivot, move to right-1
	pivot := accDist[alive[mid]]
	alive[mid], alive[right-1] = alive[right-1], alive[mid]

	// Handle small partitions
	if right-left < 3 {
		return left
	}

	i := left
	j := right - 1

	for {
		// Find element >= pivot from left
		for i++; accDist[alive[i]] < pivot; i++ {
		}

		// Find element <= pivot from right
		for j--; accDist[alive[j]] > pivot; j-- {
		}

		if i >= j {
			break
		}

		alive[i], alive[j] = alive[j], alive[i]
	}

	// Restore pivot
	alive[i], alive[right-1] = alive[right-1], alive[i]

	return i
}

// computeAdaptiveTarget computes the target candidate count based on
// adaptive policy and current dimensions processed.
func computeAdaptiveTarget(policy AdaptivePolicy, dimsProcessed, totalDocs int) int {
	// Find the applicable checkpoint
	var target float64
	found := false

	for dims, val := range policy.Checkpoints {
		if dimsProcessed >= dims {
			if !found || dims > dimsProcessed {
				target = val
				found = true
			}
		}
	}

	if !found {
		return totalDocs // No pruning yet
	}

	// Interpret target
	if target <= 1.0 {
		// Fraction
		return int(target * float64(totalDocs))
	}
	// Absolute count
	return int(target)
}
