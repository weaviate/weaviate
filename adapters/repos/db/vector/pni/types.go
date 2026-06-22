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

// MarginPolicy defines Policy E: best-distance + margin pruning.
// Keep all candidates whose accumulated distance is within margin of the best.
type MarginPolicy struct {
	// Name is the policy name for reporting.
	Name string

	// MarginSchedule defines the margin at each segment.
	// If nil, uses ConstantMargin.
	MarginSchedule func(seg int, dimsProcessed int) int

	// ConstantMargin is used when MarginSchedule is nil.
	ConstantMargin int

	// MinSegmentsBeforeStop is the minimum segments before early stop.
	MinSegmentsBeforeStop int

	// TargetCandidates for early stopping (stop when candidates <= this).
	// If 0, no early stopping based on candidate count.
	TargetCandidates int

	// FullRQ1 if true, process all segments without early stopping.
	FullRQ1 bool

	// RescoreLimit caps maximum candidates for float rescore.
	// If 0, no limit.
	RescoreLimit int

	// K is the number of final results.
	K int
}

// MarginSearchStats extends SearchStats with Policy E specific stats.
type MarginSearchStats struct {
	SearchStats

	// BestDistPerSegment tracks the best accumulated distance after each segment.
	BestDistPerSegment []int

	// ThresholdPerSegment tracks the threshold (best + margin) after each segment.
	ThresholdPerSegment []int

	// PrunedPerSegment tracks candidates pruned at each segment.
	PrunedPerSegment []int
}

// MarginSearchResult extends SearchResult with Policy E specific stats.
type MarginSearchResult struct {
	SearchResult

	// MarginStats contains Policy E specific statistics.
	MarginStats MarginSearchStats
}

// Predefined margin policies

// PolicyE1 returns constant margin = 8.
func PolicyE1(k int) MarginPolicy {
	return MarginPolicy{
		Name:                  "E1_margin8",
		ConstantMargin:        8,
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE2 returns constant margin = 12.
func PolicyE2(k int) MarginPolicy {
	return MarginPolicy{
		Name:                  "E2_margin12",
		ConstantMargin:        12,
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE3 returns constant margin = 16.
func PolicyE3(k int) MarginPolicy {
	return MarginPolicy{
		Name:                  "E3_margin16",
		ConstantMargin:        16,
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE4 returns constant margin = 24.
func PolicyE4(k int) MarginPolicy {
	return MarginPolicy{
		Name:                  "E4_margin24",
		ConstantMargin:        24,
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE5 returns adaptive margin = max(8, segments * 2).
func PolicyE5(k int) MarginPolicy {
	return MarginPolicy{
		Name: "E5_adaptive_8_2x",
		MarginSchedule: func(seg int, dimsProcessed int) int {
			m := (seg + 1) * 2
			if m < 8 {
				return 8
			}
			return m
		},
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE6 returns adaptive margin = max(12, segments * 2).
func PolicyE6(k int) MarginPolicy {
	return MarginPolicy{
		Name: "E6_adaptive_12_2x",
		MarginSchedule: func(seg int, dimsProcessed int) int {
			m := (seg + 1) * 2
			if m < 12 {
				return 12
			}
			return m
		},
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE7 returns adaptive margin = max(16, segments * 3).
func PolicyE7(k int) MarginPolicy {
	return MarginPolicy{
		Name: "E7_adaptive_16_3x",
		MarginSchedule: func(seg int, dimsProcessed int) int {
			m := (seg + 1) * 3
			if m < 16 {
				return 16
			}
			return m
		},
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE8 returns dimension-based margin schedule.
func PolicyE8(k int) MarginPolicy {
	return MarginPolicy{
		Name: "E8_dims_schedule",
		MarginSchedule: func(seg int, dimsProcessed int) int {
			switch {
			case dimsProcessed <= 64:
				return 8
			case dimsProcessed <= 128:
				return 12
			case dimsProcessed <= 256:
				return 16
			case dimsProcessed <= 512:
				return 24
			case dimsProcessed <= 768:
				return 32
			case dimsProcessed <= 1024:
				return 40
			default:
				return 48
			}
		},
		MinSegmentsBeforeStop: 4,
		TargetCandidates:      500,
		K:                     k,
	}
}

// PolicyE_FullRQ1 wraps a margin policy to use full RQ1 (no early stopping).
func PolicyE_FullRQ1(base MarginPolicy) MarginPolicy {
	base.Name = base.Name + "_fullRQ1"
	base.FullRQ1 = true
	base.TargetCandidates = 0 // No early stopping
	return base
}
