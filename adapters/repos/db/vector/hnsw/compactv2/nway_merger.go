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

package compactv2

import (
	"container/heap"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// NWayMerger performs an n-way merge of sorted commit logs into a single
// sorted stream of [NodeCommits].
//
// The merger uses a min-heap ordered by node ID to efficiently select
// the next node across all input iterators. When multiple iterators have
// commits for the same node ID, commits are merged using precedence rules:
// higher iterator ID = more recent data = higher precedence.
//
// The merger accepts any [IteratorLike] implementation, allowing both
// [Iterator] (from .sorted files) and [SnapshotIterator] (from .snapshot
// files) to be combined. This enables merging a snapshot with subsequent
// WAL files into a new snapshot.
//
// Global commits (compression data, entrypoint) are merged separately via
// [NWayMerger.GlobalCommits], taking the most recent value of each type.
//
// Usage:
//
//	merger, _ := NewNWayMerger(iterators, logger)
//	globalCommits := merger.GlobalCommits()
//	for {
//	    nodeCommits, _ := merger.Next()
//	    if nodeCommits == nil {
//	        break // exhausted
//	    }
//	    // process nodeCommits
//	}
type NWayMerger struct {
	iterators     []IteratorLike
	heap          *iteratorHeap
	logger        logrus.FieldLogger
	mergedGlobals []Commit
}

// NewNWayMerger creates a new n-way merger with the given iterators.
// Iterators should be ordered by precedence: higher index = more recent log.
func NewNWayMerger(iterators []IteratorLike, logger logrus.FieldLogger) (*NWayMerger, error) {
	if len(iterators) == 0 {
		return nil, errors.New("at least one iterator required")
	}

	// Build heap with all non-exhausted iterators
	h := &iteratorHeap{}
	for _, it := range iterators {
		if !it.Exhausted() {
			heap.Push(h, it)
		}
	}

	merger := &NWayMerger{
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
func (m *NWayMerger) mergeGlobalCommits() []Commit {
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
func (m *NWayMerger) GlobalCommits() []Commit {
	return m.mergedGlobals
}

// Next returns the next merged node commits.
// Returns nil when all iterators are exhausted.
func (m *NWayMerger) Next() (*NodeCommits, error) {
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
func (m *NWayMerger) mergeNodeCommits(nodeID uint64, iterators []IteratorLike) (*NodeCommits, error) {
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

	// Process commits from highest precedence to lowest.
	// finishIterator is called after each iterator so that ClearLinks from a
	// lower-precedence iterator cannot erase links contributed by higher-precedence ones.
	for _, it := range sortedIterators {
		commits := it.Current().Commits
		for _, c := range commits {
			if err := merger.addCommit(c); err != nil {
				return nil, err
			}
		}
		merger.finishIterator()
	}

	return merger.result(), nil
}

// iteratorHeap is a min-heap of iterators ordered by current node ID.
type iteratorHeap []IteratorLike

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	return h[i].Current().NodeID < h[j].Current().NodeID
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x any) {
	*h = append(*h, x.(IteratorLike))
}

func (h *iteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// commitMerger handles merging commits for a single node.
//
// Links are tracked in two phases to correctly handle ClearLinksCommit:
//   - pendingLinks: accumulates links from the iterator currently being processed.
//   - linksPerLevel: holds finalized links from already-processed (higher-precedence) iterators.
//
// After all commits from one iterator are fed via addCommit, call finishIterator() to
// move pendingLinks into linksPerLevel. This boundary lets ClearLinks erase only the
// current iterator's own accumulated links without touching data contributed by newer
// iterators.
type commitMerger struct {
	nodeID             uint64
	logger             logrus.FieldLogger
	deleted            bool
	addNode            *AddNodeCommit
	linksPerLevel      map[uint16][]uint64 // finalized links from already-processed iterators
	pendingLinks       map[uint16][]uint64 // links accumulating from the current iterator
	linksReplaced      map[uint16]bool     // true if links were replaced at this level
	addTombstone       bool
	removeTombstone    bool
	seenReplaceAtLevel map[uint16]bool // true once a Replace or Clear has been seen at a level
	seenClearLinks     bool            // true once ClearLinks has been seen from any iterator
}

func newCommitMerger(nodeID uint64, logger logrus.FieldLogger) *commitMerger {
	return &commitMerger{
		nodeID:             nodeID,
		logger:             logger,
		linksPerLevel:      make(map[uint16][]uint64),
		pendingLinks:       make(map[uint16][]uint64),
		linksReplaced:      make(map[uint16]bool),
		seenReplaceAtLevel: make(map[uint16]bool),
	}
}

// finishIterator must be called after all commits for one iterator have been fed
// via addCommit. It merges pendingLinks into linksPerLevel so that the next
// (lower-precedence) iterator's ClearLinks cannot erase these finalized links.
func (m *commitMerger) finishIterator() {
	for level, pending := range m.pendingLinks {
		existing := m.linksPerLevel[level]
		m.linksPerLevel[level] = append(pending, existing...)
	}
	m.pendingLinks = make(map[uint16][]uint64)
}

func (m *commitMerger) addCommit(c Commit) error {
	switch ct := c.(type) {
	case *DeleteNodeCommit:
		// DeleteNode means we can drop all previous data for this node
		m.deleted = true
		m.addNode = nil
		m.linksPerLevel = make(map[uint16][]uint64)
		m.pendingLinks = make(map[uint16][]uint64)
		m.linksReplaced = make(map[uint16]bool)
		m.seenReplaceAtLevel = make(map[uint16]bool)
		m.seenClearLinks = false

	case *AddNodeCommit:
		if !m.deleted && m.addNode == nil {
			// Keep the first AddNode we see (highest precedence)
			m.addNode = ct
		}

	case *ReplaceLinksAtLevelCommit:
		if !m.deleted && !m.seenReplaceAtLevel[ct.Level] && !m.seenClearLinks {
			// First replace at this level - prepend to any pending links from this iterator,
			// then finishIterator will prepend the whole pending block before finalized links.
			// Mark as replaced to stop accumulation from older iterators.
			existing := m.pendingLinks[ct.Level]
			m.pendingLinks[ct.Level] = append(ct.Targets, existing...)
			m.linksReplaced[ct.Level] = true
			m.seenReplaceAtLevel[ct.Level] = true
		}

	case *AddLinksAtLevelCommit:
		if !m.deleted {
			if m.seenReplaceAtLevel[ct.Level] || m.seenClearLinks {
				// A newer iterator already owns this level (or all levels) — stop accumulating.
			} else {
				// Prepend to pending links (we're going newest to oldest within this iterator).
				existing := m.pendingLinks[ct.Level]
				m.pendingLinks[ct.Level] = append(ct.Targets, existing...)
			}
		}

	case *AddLinkAtLevelCommit:
		if !m.deleted {
			if m.seenReplaceAtLevel[ct.Level] || m.seenClearLinks {
				// A newer iterator already owns this level (or all levels) — stop accumulating.
			} else {
				// Prepend to pending links (we're going newest to oldest within this iterator).
				existing := m.pendingLinks[ct.Level]
				m.pendingLinks[ct.Level] = append([]uint64{ct.Target}, existing...)
			}
		}

	case *ClearLinksAtLevelCommit:
		if !m.deleted && !m.seenReplaceAtLevel[ct.Level] {
			// Wipe any pending links accumulated at this level within the current iterator
			// (they are older than this clear within the same file).
			delete(m.pendingLinks, ct.Level)
			// Finalized links from newer iterators (in linksPerLevel) are unaffected.
			// Mark as replaced to stop accumulation from older iterators.
			m.linksReplaced[ct.Level] = true
			m.seenReplaceAtLevel[ct.Level] = true
		}

	case *ClearLinksCommit:
		if !m.deleted {
			// Wipe pending links: any links accumulated so far within the current iterator
			// are superseded by this clear (which is newer within the same file).
			m.pendingLinks = make(map[uint16][]uint64)
			// Finalized links (linksPerLevel) came from higher-precedence iterators and must
			// be preserved. Seal each such level so that older iterators cannot modify them.
			for level := range m.linksPerLevel {
				m.seenReplaceAtLevel[level] = true
			}
			// Block all further link additions from older iterators at any level.
			m.seenClearLinks = true
		}

	case *AddTombstoneCommit:
		m.addTombstone = true

	case *RemoveTombstoneCommit:
		m.removeTombstone = true

	default:
		return errors.Errorf("unexpected commit type for node %d: %T", m.nodeID, c)
	}

	return nil
}

func (m *commitMerger) result() *NodeCommits {
	commits := make([]Commit, 0)

	// If node was deleted, only return the delete commit
	if m.deleted {
		commits = append(commits, &DeleteNodeCommit{ID: m.nodeID})

		// Tombstone operations are still relevant even for deleted nodes
		if m.addTombstone && !m.removeTombstone {
			commits = append(commits, &AddTombstoneCommit{ID: m.nodeID})
		}
		if m.removeTombstone && !m.addTombstone {
			commits = append(commits, &RemoveTombstoneCommit{ID: m.nodeID})
		}
		// If both add and remove, they cancel out (no-op)

		return &NodeCommits{
			NodeID:  m.nodeID,
			Commits: commits,
		}
	}

	// Tombstone operations (no-op if both add and remove)
	if m.addTombstone && !m.removeTombstone {
		commits = append(commits, &AddTombstoneCommit{ID: m.nodeID})
	}
	if m.removeTombstone && !m.addTombstone {
		commits = append(commits, &RemoveTombstoneCommit{ID: m.nodeID})
	}

	// Add node commit
	if m.addNode != nil {
		commits = append(commits, m.addNode)
	}

	// Add link commits (sorted by level)
	levels := make([]uint16, 0, len(m.linksPerLevel))
	for level := range m.linksPerLevel {
		levels = append(levels, level)
	}
	// Sort levels
	for i := 0; i < len(levels)-1; i++ {
		for j := i + 1; j < len(levels); j++ {
			if levels[i] > levels[j] {
				levels[i], levels[j] = levels[j], levels[i]
			}
		}
	}

	for _, level := range levels {
		links := m.linksPerLevel[level]
		if len(links) == 0 {
			continue
		}

		if m.linksReplaced[level] {
			commits = append(commits, &ReplaceLinksAtLevelCommit{
				Source:  m.nodeID,
				Level:   level,
				Targets: links,
			})
		} else {
			commits = append(commits, &AddLinksAtLevelCommit{
				Source:  m.nodeID,
				Level:   level,
				Targets: links,
			})
		}
	}

	return &NodeCommits{
		NodeID:  m.nodeID,
		Commits: commits,
	}
}
