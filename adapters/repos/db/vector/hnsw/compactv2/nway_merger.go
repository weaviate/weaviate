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

// NWayMerger performs an n-way merge of sorted commit logs.
// It takes n iterators and merges them into a single sorted stream.
type NWayMerger struct {
	iterators      []*Iterator
	heap           *iteratorHeap
	logger         logrus.FieldLogger
	mergedGlobals  []Commit
}

// NewNWayMerger creates a new n-way merger with the given iterators.
// Iterators should be ordered by precedence: higher index = more recent log.
func NewNWayMerger(iterators []*Iterator, logger logrus.FieldLogger) (*NWayMerger, error) {
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
	minIt := heap.Pop(m.heap).(*Iterator)
	minNodeID := minIt.Current().NodeID

	// Collect all iterators with the same node ID
	iteratorsForNode := []*Iterator{minIt}
	for m.heap.Len() > 0 && (*m.heap)[0].Current().NodeID == minNodeID {
		iteratorsForNode = append(iteratorsForNode, heap.Pop(m.heap).(*Iterator))
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
func (m *NWayMerger) mergeNodeCommits(nodeID uint64, iterators []*Iterator) (*NodeCommits, error) {
	// Sort iterators by ID in descending order (highest precedence first)
	sortedIterators := make([]*Iterator, len(iterators))
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

// iteratorHeap is a min-heap of iterators ordered by current node ID.
type iteratorHeap []*Iterator

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	return h[i].Current().NodeID < h[j].Current().NodeID
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*Iterator))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// commitMerger handles merging commits for a single node.
type commitMerger struct {
	nodeID             uint64
	logger             logrus.FieldLogger
	deleted            bool
	addNode            *AddNodeCommit
	linksPerLevel      map[uint16][]uint64 // merged links
	linksReplaced      map[uint16]bool     // true if links were replaced at this level
	addTombstone       bool
	removeTombstone    bool
	seenReplaceAtLevel map[uint16]bool // track if we've seen a replace at each level
}

func newCommitMerger(nodeID uint64, logger logrus.FieldLogger) *commitMerger {
	return &commitMerger{
		nodeID:             nodeID,
		logger:             logger,
		linksPerLevel:      make(map[uint16][]uint64),
		linksReplaced:      make(map[uint16]bool),
		seenReplaceAtLevel: make(map[uint16]bool),
	}
}

func (m *commitMerger) addCommit(c Commit) error {
	switch ct := c.(type) {
	case *DeleteNodeCommit:
		// DeleteNode means we can drop all previous data for this node
		m.deleted = true
		m.addNode = nil
		m.linksPerLevel = make(map[uint16][]uint64)
		m.linksReplaced = make(map[uint16]bool)
		m.seenReplaceAtLevel = make(map[uint16]bool)

	case *AddNodeCommit:
		if !m.deleted && m.addNode == nil {
			// Keep the first AddNode we see (highest precedence)
			m.addNode = ct
		}

	case *ReplaceLinksAtLevelCommit:
		if !m.deleted && !m.seenReplaceAtLevel[ct.Level] {
			// First replace at this level - prepend to any existing links (which came from newer logs)
			// and mark as replaced to stop accumulation from older logs
			existing := m.linksPerLevel[ct.Level]
			m.linksPerLevel[ct.Level] = append(ct.Targets, existing...)
			m.linksReplaced[ct.Level] = true
			m.seenReplaceAtLevel[ct.Level] = true
		}

	case *AddLinksAtLevelCommit:
		if !m.deleted {
			if m.seenReplaceAtLevel[ct.Level] {
				// We've already seen a replace, stop accumulating (older data is obsolete)
				// Don't add these links
			} else {
				// No replace seen yet, prepend to existing links (we're going newest to oldest)
				existing := m.linksPerLevel[ct.Level]
				m.linksPerLevel[ct.Level] = append(ct.Targets, existing...)
			}
		}

	case *AddLinkAtLevelCommit:
		if !m.deleted {
			if m.seenReplaceAtLevel[ct.Level] {
				// We've already seen a replace, stop accumulating (older data is obsolete)
				// Don't add this link
			} else {
				// No replace seen yet, prepend to existing links (we're going newest to oldest)
				existing := m.linksPerLevel[ct.Level]
				m.linksPerLevel[ct.Level] = append([]uint64{ct.Target}, existing...)
			}
		}

	case *ClearLinksAtLevelCommit:
		if !m.deleted && !m.seenReplaceAtLevel[ct.Level] {
			// Clear acts like replace - keep any links from newer logs (already in linksPerLevel)
			// and stop accumulation from older logs
			m.linksReplaced[ct.Level] = true
			m.seenReplaceAtLevel[ct.Level] = true
		}

	case *ClearLinksCommit:
		if !m.deleted {
			m.linksPerLevel = make(map[uint16][]uint64)
			m.linksReplaced = make(map[uint16]bool)
			m.seenReplaceAtLevel = make(map[uint16]bool)
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
