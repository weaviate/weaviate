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
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// SnapshotIterator converts a snapshot into a commit stream for the N-way merger.
// It implements the same interface pattern as Iterator, allowing snapshots to be
// merged with sorted commit logs using the existing NWayMerger infrastructure.
//
// The snapshot is read fully into memory as a DeserializationResult, then the
// iterator generates commits in sorted order (by node ID) to match the format
// expected by the merger.
type SnapshotIterator struct {
	result        *ent.DeserializationResult
	id            int // Iterator precedence ID
	globalCommits []Commit
	currentNode   *NodeCommits
	nodeIndex     int // Current position in nodes array
	exhausted     bool
	logger        logrus.FieldLogger
}

// NewSnapshotIterator creates a new iterator from a snapshot file.
// The id parameter is used for merge precedence (higher ID = more recent data).
// Typically snapshots should have the lowest ID since they represent the oldest state.
func NewSnapshotIterator(path string, id int, logger logrus.FieldLogger) (*SnapshotIterator, error) {
	return NewSnapshotIteratorWithFS(path, id, logger, common.NewOSFS())
}

// NewSnapshotIteratorWithFS creates a new iterator from a snapshot file using a custom filesystem.
func NewSnapshotIteratorWithFS(path string, id int, logger logrus.FieldLogger, fs common.FS) (*SnapshotIterator, error) {
	reader := NewSnapshotReader(logger)
	result, err := reader.ReadFromFileWithFS(path, fs)
	if err != nil {
		return nil, errors.Wrapf(err, "read snapshot file %s", path)
	}

	return NewSnapshotIteratorFromResult(result, id, logger)
}

// NewSnapshotIteratorFromResult creates a new iterator from a DeserializationResult.
// This is useful for testing or when the snapshot is already loaded.
func NewSnapshotIteratorFromResult(result *ent.DeserializationResult, id int, logger logrus.FieldLogger) (*SnapshotIterator, error) {
	it := &SnapshotIterator{
		result:    result,
		id:        id,
		nodeIndex: -1, // Will be incremented on first advance
		logger:    logger,
	}

	// Build global commits from snapshot metadata
	it.globalCommits = it.buildGlobalCommits()

	// Advance to the first node
	if err := it.advance(); err != nil {
		return nil, err
	}

	return it, nil
}

// ID returns the iterator's ordinal ID (used for merge precedence).
func (it *SnapshotIterator) ID() int {
	return it.id
}

// GlobalCommits returns all global (non-node-specific) commits from the snapshot.
// This includes entrypoint, compression data, and Muvera encoder data.
func (it *SnapshotIterator) GlobalCommits() []Commit {
	return it.globalCommits
}

// Current returns the commits for the current node.
// Returns nil if the iterator is exhausted.
func (it *SnapshotIterator) Current() *NodeCommits {
	return it.currentNode
}

// Exhausted returns true if there are no more nodes to read.
func (it *SnapshotIterator) Exhausted() bool {
	return it.exhausted
}

// Next advances the iterator to the next node.
// Returns true if there is a next node, false if exhausted.
func (it *SnapshotIterator) Next() (bool, error) {
	if it.exhausted {
		return false, nil
	}

	if err := it.advance(); err != nil {
		return false, err
	}

	return !it.exhausted, nil
}

// buildGlobalCommits creates the global commits from snapshot metadata.
func (it *SnapshotIterator) buildGlobalCommits() []Commit {
	commits := make([]Commit, 0)

	// Compression commits (only one should be present)
	if it.result.CompressionPQData() != nil {
		commits = append(commits, &AddPQCommit{Data: it.result.CompressionPQData()})
	}
	if it.result.CompressionSQData() != nil {
		commits = append(commits, &AddSQCommit{Data: it.result.CompressionSQData()})
	}
	if it.result.CompressionRQData() != nil {
		commits = append(commits, &AddRQCommit{Data: it.result.CompressionRQData()})
	}
	if it.result.CompressionBRQData() != nil {
		commits = append(commits, &AddBRQCommit{Data: it.result.CompressionBRQData()})
	}

	// Muvera encoder
	if it.result.MuveraEnabled() && it.result.EncoderMuvera() != nil {
		commits = append(commits, &AddMuveraCommit{Data: it.result.EncoderMuvera()})
	}

	// Entrypoint
	if it.result.EntrypointChanged() {
		commits = append(commits, &SetEntryPointMaxLevelCommit{
			Entrypoint: it.result.Entrypoint(),
			Level:      it.result.Level(),
		})
	}

	return commits
}

// advance moves to the next non-nil node and builds its commits.
func (it *SnapshotIterator) advance() error {
	// If we've already exhausted nodes, check for tombstones beyond the array
	if it.nodeIndex >= len(it.result.Graph.Nodes) {
		beyondNode := it.findNextTombstoneBeyondNodes()
		if beyondNode != nil {
			it.currentNode = beyondNode
			return nil
		}
		it.exhausted = true
		it.currentNode = nil
		return nil
	}

	// Find the next non-nil node
	for {
		it.nodeIndex++

		// Check if we've exhausted all nodes
		if it.nodeIndex >= len(it.result.Graph.Nodes) {
			// Check if there are tombstones beyond the nodes array
			beyondNode := it.findNextTombstoneBeyondNodes()
			if beyondNode != nil {
				it.currentNode = beyondNode
				return nil
			}
			it.exhausted = true
			it.currentNode = nil
			return nil
		}

		node := it.result.Graph.Nodes[it.nodeIndex]
		nodeID := uint64(it.nodeIndex)

		// Check for tombstone at this position (even if node is nil)
		_, hasTombstone := it.result.Graph.Tombstones[nodeID]

		// If node is nil but has a tombstone, generate tombstone commit only
		if node == nil {
			if hasTombstone {
				it.currentNode = &NodeCommits{
					NodeID:  nodeID,
					Commits: []Commit{&AddTombstoneCommit{ID: nodeID}},
				}
				return nil
			}
			// Skip nil nodes without tombstones
			continue
		}

		// Build commits for this node
		it.currentNode = it.buildNodeCommits(nodeID, node, hasTombstone)
		return nil
	}
}

// buildNodeCommits creates commits for a single node from snapshot state.
func (it *SnapshotIterator) buildNodeCommits(nodeID uint64, node *ent.Vertex, hasTombstone bool) *NodeCommits {
	commits := make([]Commit, 0)

	// AddNode commit (with level)
	commits = append(commits, &AddNodeCommit{
		ID:    nodeID,
		Level: uint16(node.Level),
	})

	// ReplaceLinksAtLevel commits for each level
	// Snapshots represent absolute state, so all links are "replace" operations
	if node.Connections != nil {
		iter := node.Connections.Iterator()
		for iter.Next() {
			level, links := iter.Current()
			if len(links) > 0 {
				// Make a copy of the links slice to avoid reference issues
				linksCopy := make([]uint64, len(links))
				copy(linksCopy, links)

				commits = append(commits, &ReplaceLinksAtLevelCommit{
					Source:  nodeID,
					Level:   uint16(level),
					Targets: linksCopy,
				})
			}
		}
	}

	// AddTombstone commit if applicable
	if hasTombstone {
		commits = append(commits, &AddTombstoneCommit{ID: nodeID})
	}

	return &NodeCommits{
		NodeID:  nodeID,
		Commits: commits,
	}
}

// findNextTombstoneBeyondNodes finds tombstones that are beyond the nodes array.
// These need to be emitted as separate commits since they have no associated node data.
func (it *SnapshotIterator) findNextTombstoneBeyondNodes() *NodeCommits {
	// This is a simplified implementation - in a full implementation you might
	// want to cache and sort the tombstone IDs beyond the nodes array.
	// For now, we iterate through tombstones to find the smallest one beyond nodes.

	var smallestID uint64
	found := false
	nodeLen := uint64(len(it.result.Graph.Nodes))

	for id := range it.result.Graph.Tombstones {
		if id >= nodeLen {
			if !found || id < smallestID {
				smallestID = id
				found = true
			}
		}
	}

	if !found {
		return nil
	}

	// Remove the tombstone from the map so we don't emit it again
	delete(it.result.Graph.Tombstones, smallestID)

	return &NodeCommits{
		NodeID:  smallestID,
		Commits: []Commit{&AddTombstoneCommit{ID: smallestID}},
	}
}

// AsIterators wraps SnapshotIterator(s) in a form usable by NWayMerger.
// This is a helper function since NWayMerger expects []*Iterator but we have
// a different iterator type. We use interface matching through duck typing.

// IteratorLike defines the interface expected by the N-way merger.
// Both Iterator and SnapshotIterator implement this interface.
type IteratorLike interface {
	ID() int
	GlobalCommits() []Commit
	Current() *NodeCommits
	Exhausted() bool
	Next() (bool, error)
}

// VerifyIteratorLikeInterface ensures SnapshotIterator implements IteratorLike.
// This is a compile-time check.
var (
	_ IteratorLike = (*SnapshotIterator)(nil)
	_ IteratorLike = (*Iterator)(nil)
)
