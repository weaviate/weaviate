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

// SearchOptions configures the progressive search behavior.
type SearchOptions struct {
	// TargetCandidates is the number of candidates to keep after pruning.
	// Search stops when alive <= TargetCandidates and MinSegmentsBeforeStop is satisfied.
	TargetCandidates int

	// MinSegmentsBeforeStop is the minimum number of segments to process
	// before allowing early termination.
	MinSegmentsBeforeStop int

	// RescoreLimit caps the maximum candidates for float rescore.
	// If 0, uses TargetCandidates.
	RescoreLimit int

	// K is the number of final results to return.
	K int
}

// SearchResult holds the search results.
type SearchResult struct {
	// IDs are the document IDs of the top-k results.
	IDs []uint64

	// Distances are the float distances of the top-k results.
	Distances []float32

	// Stats contains search statistics for observability.
	Stats SearchStats
}

// SearchStats provides observability into the search process.
type SearchStats struct {
	// SegmentsProcessed is how many segments were processed before stopping.
	SegmentsProcessed int

	// CandidatesPerSegment tracks candidates remaining after each segment.
	CandidatesPerSegment []int

	// FinalCandidates is the number of candidates that went to float rescore.
	FinalCandidates int

	// DistancesComputed is the total segment distances computed.
	DistancesComputed int64

	// RescoresComputed is the number of float rescores performed.
	RescoresComputed int
}

// AdaptivePolicy defines adaptive pruning thresholds per dimension checkpoint.
type AdaptivePolicy struct {
	// Checkpoints maps cumulative dimensions to keep fraction or absolute count.
	// Example: {64: 0.30, 128: 0.10, 256: 0.02, 512: 1000}
	// Fraction (0-1) means keep that percentage.
	// Integer > 1 means keep that absolute count.
	Checkpoints map[int]float64
}

// DefaultSearchOptions returns sensible defaults.
func DefaultSearchOptions(k int) SearchOptions {
	return SearchOptions{
		TargetCandidates:      500,
		MinSegmentsBeforeStop: 4, // At least 256 dims
		RescoreLimit:          0, // Use TargetCandidates
		K:                     k,
	}
}

// DefaultAdaptivePolicy returns the adaptive policy from the spec.
func DefaultAdaptivePolicy() AdaptivePolicy {
	return AdaptivePolicy{
		Checkpoints: map[int]float64{
			64:  0.30, // 30% after 64 dims (1 segment)
			128: 0.10, // 10% after 128 dims (2 segments)
			256: 0.02, // 2% after 256 dims (4 segments)
			512: 1000, // 1000 absolute after 512 dims (8 segments)
		},
	}
}
