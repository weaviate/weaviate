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
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// SortedWriter writes a .sorted commit log from a DeserializationResult.
// The resulting log has the following structure:
//  1. Global commits (compression, muvera, entrypoint) - these have no node ID
//  2. Node-specific commits, sorted by node ID:
//     - For each node ID (in order):
//     - DeleteNode (if node was deleted)
//     - AddTombstone (if tombstone exists for this node)
//     - RemoveTombstone (if tombstone was removed for this node)
//     - AddNode (if node exists and level > 0)
//     - Links (ReplaceLinksAtLevel or AddLinksAtLevel)
//
// This structure enables efficient stream-merging of multiple sorted logs.
type SortedWriter struct {
	writer *WALWriter
	logger logrus.FieldLogger
}

// NewSortedWriter creates a new sorted log writer.
func NewSortedWriter(w io.Writer, logger logrus.FieldLogger) *SortedWriter {
	return &SortedWriter{
		writer: NewWALWriter(w),
		logger: logger,
	}
}

// WriteAll writes a complete sorted log from a deserialization result.
func (s *SortedWriter) WriteAll(res *ent.DeserializationResult) error {
	// Phase 1: Write all global (non-node-specific) commits first
	if err := s.writeGlobalCommits(res); err != nil {
		return errors.Wrap(err, "write global commits")
	}

	// Phase 2: Write node-specific commits, sorted by node ID
	// After this point, the rest of the file is perfectly sorted by node ID
	if err := s.writeNodeCommits(res); err != nil {
		return errors.Wrap(err, "write node commits")
	}

	return nil
}

// writeGlobalCommits writes all commits that are not node-specific.
// These must be written before any node-specific commits.
func (s *SortedWriter) writeGlobalCommits(res *ent.DeserializationResult) error {
	// Write compression data
	if res.Compressed() {
		if res.CompressionPQData() != nil {
			if err := s.writer.WriteAddPQ(res.CompressionPQData()); err != nil {
				return errors.Wrap(err, "write PQ compression")
			}
		} else if res.CompressionSQData() != nil {
			if err := s.writer.WriteAddSQ(res.CompressionSQData()); err != nil {
				return errors.Wrap(err, "write SQ compression")
			}
		} else if res.CompressionRQData() != nil {
			if err := s.writer.WriteAddRQ(res.CompressionRQData()); err != nil {
				return errors.Wrap(err, "write RQ compression")
			}
		} else if res.CompressionBRQData() != nil {
			if err := s.writer.WriteAddBRQ(res.CompressionBRQData()); err != nil {
				return errors.Wrap(err, "write BRQ compression")
			}
		}
	}

	// Write Muvera data
	if res.MuveraEnabled() {
		if err := s.writer.WriteAddMuvera(res.EncoderMuvera()); err != nil {
			return errors.Wrap(err, "write Muvera encoder")
		}
	}

	// Write entrypoint
	if res.EntrypointChanged() {
		if err := s.writer.WriteSetEntryPointMaxLevel(res.Entrypoint(), res.Level()); err != nil {
			return errors.Wrap(err, "write entrypoint")
		}
	}

	return nil
}

// writeNodeCommits writes all node-specific commits in node ID order.
// For each node, it writes tombstones, deletions, node data, and links.
func (s *SortedWriter) writeNodeCommits(res *ent.DeserializationResult) error {
	// Helper function to write tombstone operations for a given node ID
	writeTombstoneOps := func(nodeID uint64) error {
		_, hasTombstone := res.Graph.Tombstones[nodeID]
		_, tombstoneDeleted := res.Graph.TombstonesDeleted[nodeID]

		// Consolidate tombstone add/remove operations:
		// If both add and remove exist, they cancel out (noop)
		writeTombstone := hasTombstone && !tombstoneDeleted
		writeRemoveTombstone := tombstoneDeleted && !hasTombstone

		if writeTombstone {
			if err := s.writer.WriteAddTombstone(nodeID); err != nil {
				return errors.Wrapf(err, "write tombstone for node %d", nodeID)
			}
		}

		if writeRemoveTombstone {
			if err := s.writer.WriteRemoveTombstone(nodeID); err != nil {
				return errors.Wrapf(err, "write remove tombstone for node %d", nodeID)
			}
		}

		return nil
	}

	// Phase 1: Iterate through all possible node IDs in the nodes array
	for nodeID := uint64(0); nodeID < uint64(len(res.Graph.Nodes)); nodeID++ {
		node := res.Graph.Nodes[nodeID]

		// Check if this node was deleted
		_, isDeleted := res.Graph.NodesDeleted[nodeID]

		// If the node was deleted, write deletion info and tombstone info, then skip
		if isDeleted {
			if err := s.writer.WriteDeleteNode(nodeID); err != nil {
				return errors.Wrapf(err, "write delete node %d", nodeID)
			}

			// Write tombstone operations even for deleted nodes
			if err := writeTombstoneOps(nodeID); err != nil {
				return err
			}

			continue
		}

		// Write tombstone operations for this node (even if node is nil)
		// A nil node in this log doesn't mean it doesn't exist - it may have been
		// created in a previous log. This log may only contain tombstone operations.
		if err := writeTombstoneOps(nodeID); err != nil {
			return err
		}

		// If node is nil (no node operations in this log), skip writing node/link data
		if node == nil {
			continue
		}

		// Write the node itself (if level > 0)
		// Nodes are implicitly added when they are first linked, if the level is
		// not zero we know this node was new. If the level is zero it doesn't
		// matter if it gets added explicitly or implicitly.
		if node.Level > 0 {
			if err := s.writer.WriteAddNode(nodeID, uint16(node.Level)); err != nil {
				return errors.Wrapf(err, "write node %d", nodeID)
			}
		}

		// Write links for this node
		if node.Connections != nil {
			iter := node.Connections.Iterator()
			for iter.Next() {
				level, links := iter.Current()

				// Truncate if too many connections
				if len(links) > math.MaxUint16 {
					s.logger.WithField("action", "write_sorted_log").
						WithField("node_id", nodeID).
						WithField("level", level).
						WithField("original_length", len(links)).
						WithField("maximum_length", math.MaxUint16).
						Warning("length of connections would overflow uint16, cutting off")
					links = links[:math.MaxUint16]
				}

				// Check if we should replace or add links
				if res.ReplaceLinks(nodeID, uint16(level)) {
					if err := s.writer.WriteReplaceLinksAtLevel(nodeID, uint16(level), links); err != nil {
						return errors.Wrapf(err, "write replace links for node %d at level %d", nodeID, level)
					}
				} else {
					if err := s.writer.WriteAddLinksAtLevel(nodeID, uint16(level), links); err != nil {
						return errors.Wrapf(err, "write add links for node %d at level %d", nodeID, level)
					}
				}
			}
		}
	}

	// Phase 2: Write tombstone operations for any node IDs beyond the nodes array
	// Collect all tombstone IDs beyond the array and process them in sorted order
	maxNodeID := uint64(len(res.Graph.Nodes))
	beyondIDs := make([]uint64, 0)

	for id := range res.Graph.Tombstones {
		if id >= maxNodeID {
			beyondIDs = append(beyondIDs, id)
		}
	}

	for id := range res.Graph.TombstonesDeleted {
		if id >= maxNodeID {
			// Check if not already in beyondIDs
			found := false
			for _, existingID := range beyondIDs {
				if existingID == id {
					found = true
					break
				}
			}
			if !found {
				beyondIDs = append(beyondIDs, id)
			}
		}
	}

	// Sort the IDs to maintain sorted order
	if len(beyondIDs) > 0 {
		// Simple insertion sort (good for small lists)
		for i := 1; i < len(beyondIDs); i++ {
			key := beyondIDs[i]
			j := i - 1
			for j >= 0 && beyondIDs[j] > key {
				beyondIDs[j+1] = beyondIDs[j]
				j--
			}
			beyondIDs[j+1] = key
		}

		// Write tombstone operations for these IDs
		for _, nodeID := range beyondIDs {
			if err := writeTombstoneOps(nodeID); err != nil {
				return err
			}
		}
	}

	return nil
}
