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

package compactv2

import (
	"container/heap"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// UnifiedMerger performs an n-way merge of sorted commit logs using the IteratorLike interface.
// This allows merging both regular iterators (from .sorted files) and snapshot iterators.
type UnifiedMerger struct {
	iterators     []IteratorLike
	heap          *unifiedIteratorHeap
	logger        logrus.FieldLogger
	mergedGlobals []Commit
}

// NewUnifiedMerger creates a new unified merger with the given iterators.
// Iterators should be ordered by precedence: higher index = more recent log.
func NewUnifiedMerger(iterators []IteratorLike, logger logrus.FieldLogger) (*UnifiedMerger, error) {
	if len(iterators) == 0 {
		return nil, errors.New("at least one iterator required")
	}

	// Build heap with all non-exhausted iterators
	h := &unifiedIteratorHeap{}
	for _, it := range iterators {
		if !it.Exhausted() {
			heap.Push(h, it)
		}
	}

	merger := &UnifiedMerger{
		iterators: iterators,
		heap:      h,
		logger:    logger,
	}

	// Merge global commits from all iterators
	merger.mergedGlobals = merger.mergeGlobalCommits()

	return merger, nil
}

// mergeGlobalCommits merges global commits from all iterators.
// Takes the most recent commit of each type, processing from lowest to highest precedence.
func (m *UnifiedMerger) mergeGlobalCommits() []Commit {
	var lastPQ *AddPQCommit
	var lastSQ *AddSQCommit
	var lastRQ *AddRQCommit
	var lastBRQ *AddBRQCommit
	var lastMuvera *AddMuveraCommit
	var lastEntryPoint *SetEntryPointMaxLevelCommit
	lastResetIndexID := -1 // Track which iterator had the last reset

	// Process iterators from lowest precedence (oldest) to highest (newest)
	for _, it := range m.iterators {
		globalCommits := it.GlobalCommits()

		for _, c := range globalCommits {
			switch ct := c.(type) {
			case *ResetIndexCommit:
				// Reset clears everything before it
				lastResetIndexID = it.ID()
				// Clear all previous global state
				lastPQ = nil
				lastSQ = nil
				lastRQ = nil
				lastBRQ = nil
				lastMuvera = nil
				lastEntryPoint = nil

			case *AddPQCommit:
				// Only keep if after last reset (or no reset)
				if lastResetIndexID < it.ID() {
					lastPQ = ct
				}

			case *AddSQCommit:
				if lastResetIndexID < it.ID() {
					lastSQ = ct
				}

			case *AddRQCommit:
				if lastResetIndexID < it.ID() {
					lastRQ = ct
				}

			case *AddBRQCommit:
				if lastResetIndexID < it.ID() {
					lastBRQ = ct
				}

			case *AddMuveraCommit:
				if lastResetIndexID < it.ID() {
					lastMuvera = ct
				}

			case *SetEntryPointMaxLevelCommit:
				if lastResetIndexID < it.ID() {
					lastEntryPoint = ct
				}
			}
		}
	}

	// Build result in canonical order
	result := make([]Commit, 0)

	// Compression commits first (only one type should be present)
	if lastPQ != nil {
		result = append(result, lastPQ)
	}
	if lastSQ != nil {
		result = append(result, lastSQ)
	}
	if lastRQ != nil {
		result = append(result, lastRQ)
	}
	if lastBRQ != nil {
		result = append(result, lastBRQ)
	}

	// Muvera
	if lastMuvera != nil {
		result = append(result, lastMuvera)
	}

	// EntryPoint
	if lastEntryPoint != nil {
		result = append(result, lastEntryPoint)
	}

	return result
}

// GlobalCommits returns the merged global commits.
func (m *UnifiedMerger) GlobalCommits() []Commit {
	return m.mergedGlobals
}

// Next returns the next merged node commits.
// Returns nil when all iterators are exhausted.
func (m *UnifiedMerger) Next() (*NodeCommits, error) {
	if m.heap.Len() == 0 {
		return nil, nil
	}

	// Get the iterator with the lowest current node ID
	minIt := heap.Pop(m.heap).(IteratorLike)
	minNodeID := minIt.Current().NodeID

	// Collect all iterators with the same node ID
	iteratorsForNode := []IteratorLike{minIt}
	for m.heap.Len() > 0 && (*m.heap)[0].Current().NodeID == minNodeID {
		iteratorsForNode = append(iteratorsForNode, heap.Pop(m.heap).(IteratorLike))
	}

	// Merge commits from all iterators for this node
	mergedCommits, err := m.mergeNodeCommits(minNodeID, iteratorsForNode)
	if err != nil {
		return nil, errors.Wrapf(err, "merge commits for node %d", minNodeID)
	}

	// Advance all iterators that were processed
	for _, it := range iteratorsForNode {
		hasNext, err := it.Next()
		if err != nil {
			return nil, errors.Wrapf(err, "advance iterator %d", it.ID())
		}
		if hasNext {
			heap.Push(m.heap, it)
		}
	}

	return mergedCommits, nil
}

// mergeNodeCommits merges commits for a single node from multiple iterators.
// Iterators are processed in order of precedence (highest ID first = most recent).
func (m *UnifiedMerger) mergeNodeCommits(nodeID uint64, iterators []IteratorLike) (*NodeCommits, error) {
	// Sort iterators by ID in descending order (highest precedence first)
	sortedIterators := make([]IteratorLike, len(iterators))
	copy(sortedIterators, iterators)
	for i := 0; i < len(sortedIterators)-1; i++ {
		for j := i + 1; j < len(sortedIterators); j++ {
			if sortedIterators[i].ID() < sortedIterators[j].ID() {
				sortedIterators[i], sortedIterators[j] = sortedIterators[j], sortedIterators[i]
			}
		}
	}

	merger := newCommitMerger(nodeID, m.logger)

	// Process commits from highest precedence to lowest
	for _, it := range sortedIterators {
		commits := it.Current().Commits
		for _, c := range commits {
			if err := merger.addCommit(c); err != nil {
				return nil, err
			}
		}
	}

	return merger.result(), nil
}

// unifiedIteratorHeap is a min-heap of IteratorLike ordered by current node ID.
type unifiedIteratorHeap []IteratorLike

func (h unifiedIteratorHeap) Len() int { return len(h) }

func (h unifiedIteratorHeap) Less(i, j int) bool {
	return h[i].Current().NodeID < h[j].Current().NodeID
}

func (h unifiedIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *unifiedIteratorHeap) Push(x any) {
	*h = append(*h, x.(IteratorLike))
}

func (h *unifiedIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
