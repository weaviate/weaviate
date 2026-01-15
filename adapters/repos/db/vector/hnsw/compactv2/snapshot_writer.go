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
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
)


const (
	// snapshotVersionV3 is the version of the snapshot format we write.
	snapshotVersionV3 = 3

	// defaultBlockSize is the default size of each block in the snapshot body (4MB).
	defaultBlockSize = 4 * 1024 * 1024
)

// SnapshotWriter writes HNSW state to the V3 snapshot format.
// It converts commit-based data from the merger into absolute state snapshots.
//
// The V3 snapshot format consists of:
//   - Metadata header: version, checksum, metadata size, and metadata content
//   - Body: fixed-size blocks (4MB each) containing node data with checksums
//
// MVP Limitations:
//   - No compression support (PQ, SQ, RQ, BRQ)
//   - No Muvera encoder support
type SnapshotWriter struct {
	w         io.Writer
	blockSize int64

	// Metadata collected before writing
	entrypoint uint64
	level      uint16

	// Node state accumulated during writing
	nodes      []*nodeState
	tombstones map[uint64]struct{}
	maxNodeID  uint64
}

// nodeState represents the absolute state of a single node.
type nodeState struct {
	level       uint16
	connections [][]uint64 // connections per level
}

// NewSnapshotWriter creates a new snapshot writer.
func NewSnapshotWriter(w io.Writer) *SnapshotWriter {
	return &SnapshotWriter{
		w:          w,
		blockSize:  defaultBlockSize,
		tombstones: make(map[uint64]struct{}),
	}
}

// NewSnapshotWriterWithBlockSize creates a new snapshot writer with a custom block size.
// This is primarily useful for testing with smaller block sizes.
func NewSnapshotWriterWithBlockSize(w io.Writer, blockSize int64) *SnapshotWriter {
	return &SnapshotWriter{
		w:          w,
		blockSize:  blockSize,
		tombstones: make(map[uint64]struct{}),
	}
}

// SetEntrypoint sets the entrypoint and max level for the snapshot.
func (s *SnapshotWriter) SetEntrypoint(entrypoint uint64, level uint16) {
	s.entrypoint = entrypoint
	s.level = level
}

// AddNode adds a node with its connections to the snapshot.
// Nodes must be added in ascending node ID order.
func (s *SnapshotWriter) AddNode(nodeID uint64, level uint16, connections [][]uint64, hasTombstone bool) {
	// Expand nodes slice if needed using exponential growth
	s.ensureNodesCapacity(nodeID)

	s.nodes[nodeID] = &nodeState{
		level:       level,
		connections: connections,
	}

	if hasTombstone {
		s.tombstones[nodeID] = struct{}{}
	}

	if nodeID > s.maxNodeID {
		s.maxNodeID = nodeID
	}
}

// ensureNodesCapacity grows the nodes slice to accommodate nodeID using exponential growth.
func (s *SnapshotWriter) ensureNodesCapacity(nodeID uint64) {
	required := int(nodeID + 1)
	if required <= len(s.nodes) {
		return
	}

	// If we have enough capacity, just extend the length (no allocation)
	if required <= cap(s.nodes) {
		s.nodes = s.nodes[:required]
		return
	}

	// Need to allocate - calculate new capacity with exponential growth
	newCap := cap(s.nodes)
	if newCap == 0 {
		newCap = 1024 // initial capacity
	}
	for newCap < required {
		newCap *= 2
	}

	// Grow the slice
	newNodes := make([]*nodeState, required, newCap)
	copy(newNodes, s.nodes)
	s.nodes = newNodes
}

// AddTombstone marks a node as having a tombstone without adding node data.
// This is used for nodes that were deleted but still need tombstone tracking.
func (s *SnapshotWriter) AddTombstone(nodeID uint64) {
	// Expand nodes slice if needed to include this tombstone
	s.ensureNodesCapacity(nodeID)

	s.tombstones[nodeID] = struct{}{}
	if nodeID > s.maxNodeID {
		s.maxNodeID = nodeID
	}
}

// Flush writes all accumulated state to the snapshot file.
func (s *SnapshotWriter) Flush() error {
	if err := s.writeMetadata(); err != nil {
		return errors.Wrap(err, "write metadata")
	}

	if err := s.writeBody(); err != nil {
		return errors.Wrap(err, "write body")
	}

	return nil
}

// writeMetadata writes the snapshot metadata header.
func (s *SnapshotWriter) writeMetadata() error {
	var buf bytes.Buffer

	// Metadata content
	_ = writeUint64(&buf, s.entrypoint)
	_ = writeUint16(&buf, s.level)

	// isCompressed = false (MVP limitation)
	_ = writeBool(&buf, false)

	// isEncoded = false (MVP limitation)
	_ = writeBool(&buf, false)

	// Node count
	_ = writeUint32(&buf, uint32(len(s.nodes)))

	// Compute checksum of metadata
	metadataSize := uint32(buf.Len())

	hasher := crc32.NewIEEE()
	_ = binary.Write(hasher, binary.LittleEndian, uint8(snapshotVersionV3))
	_ = binary.Write(hasher, binary.LittleEndian, metadataSize)
	_, _ = hasher.Write(buf.Bytes())

	// Write header: version | checksum | metadataSize | metadata
	if err := writeByte(s.w, snapshotVersionV3); err != nil {
		return err
	}
	if err := binary.Write(s.w, binary.LittleEndian, hasher.Sum32()); err != nil {
		return err
	}
	if err := binary.Write(s.w, binary.LittleEndian, metadataSize); err != nil {
		return err
	}
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// writeBody writes the snapshot body in fixed-size blocks.
func (s *SnapshotWriter) writeBody() error {
	if len(s.nodes) == 0 {
		return nil
	}

	var block bytes.Buffer
	var nodeBuf bytes.Buffer

	hasher := crc32.NewIEEE()
	hw := io.MultiWriter(&block, hasher)

	maxBlockSize := int(s.blockSize - 8) // reserve 8 bytes for checksum and block length

	// Write first node ID at the start of the first block
	if err := writeUint64(hw, 0); err != nil {
		return err
	}

	for nodeID := uint64(0); nodeID < uint64(len(s.nodes)); nodeID++ {
		nodeBuf.Reset()

		node := s.nodes[nodeID]
		if node != nil {
			_, hasTombstone := s.tombstones[nodeID]
			if hasTombstone {
				_ = writeByte(&nodeBuf, 1) // exists with tombstone
			} else {
				_ = writeByte(&nodeBuf, 2) // exists without tombstone
			}

			_ = writeUint32(&nodeBuf, uint32(node.level))

			// Pack connections into binary format
			connData, err := s.packConnections(node.level, node.connections)
			if err != nil {
				return errors.Wrapf(err, "pack connections for node %d", nodeID)
			}
			_ = writeUint32(&nodeBuf, uint32(len(connData)))
			_, _ = nodeBuf.Write(connData)
		} else {
			// Check if this is a tombstone-only entry
			if _, hasTombstone := s.tombstones[nodeID]; hasTombstone {
				// Node was deleted but has tombstone - write as nil
				// (tombstone info is tracked separately)
				_ = writeByte(&nodeBuf, 0)
			} else {
				// nil node
				_ = writeByte(&nodeBuf, 0)
			}
		}

		// Check if node fits in current block
		if nodeBuf.Len()+block.Len() < maxBlockSize {
			_, err := hw.Write(nodeBuf.Bytes())
			if err != nil {
				return err
			}
			continue
		}

		// Node doesn't fit - flush current block and start new one
		if err := s.flushBlock(&block, hasher, maxBlockSize); err != nil {
			return err
		}

		// Reset for new block
		block.Reset()
		hasher.Reset()
		hw = io.MultiWriter(&block, hasher)

		// Write node ID at start of new block
		if err := writeUint64(hw, nodeID); err != nil {
			return err
		}

		// Write the node data to the new block
		_, err := hw.Write(nodeBuf.Bytes())
		if err != nil {
			return err
		}
	}

	// Flush final block if it has data
	if block.Len() > 0 {
		if err := s.flushBlock(&block, hasher, maxBlockSize); err != nil {
			return err
		}
	}

	return nil
}

// flushBlock writes a complete block to the output.
func (s *SnapshotWriter) flushBlock(block *bytes.Buffer, hasher hash.Hash32, maxBlockSize int) error {
	blockLen := block.Len()

	// Pad block to maxBlockSize
	padding := make([]byte, maxBlockSize-blockLen)
	_, _ = block.Write(padding)
	_, _ = hasher.Write(padding)

	// Write block length at end
	if err := writeUint32(block, uint32(blockLen)); err != nil {
		return err
	}
	_ = binary.Write(hasher, binary.LittleEndian, uint32(blockLen))

	// Write checksum first, then block data
	checksum := hasher.Sum32()
	if err := writeUint32(s.w, checksum); err != nil {
		return err
	}
	if _, err := s.w.Write(block.Bytes()); err != nil {
		return err
	}

	return nil
}

// packConnections converts connections to the packed binary format.
// This uses the packedconn package to ensure compatibility with the snapshot reader.
func (s *SnapshotWriter) packConnections(level uint16, connections [][]uint64) ([]byte, error) {
	if len(connections) == 0 {
		return nil, nil
	}

	// Use packedconn to create properly formatted connection data
	pc, err := packedconn.NewWithElements(connections)
	if err != nil {
		return nil, errors.Wrapf(err, "create packed connections for node with level %d", level)
	}

	return pc.Data(), nil
}

// WriteFromMerger writes snapshot data from an n-way merger.
// This converts commit-based data to absolute state and writes it as a snapshot.
func (s *SnapshotWriter) WriteFromMerger(merger *NWayMerger) error {
	// Extract global state from merged commits
	for _, c := range merger.GlobalCommits() {
		switch ct := c.(type) {
		case *SetEntryPointMaxLevelCommit:
			s.SetEntrypoint(ct.Entrypoint, ct.Level)
		}
		// Note: compression and muvera commits are ignored in MVP
	}

	// Process all nodes from the merger
	for {
		nodeCommits, err := merger.Next()
		if err != nil {
			return errors.Wrap(err, "read next node from merger")
		}
		if nodeCommits == nil {
			// No more nodes
			break
		}

		// Convert commits to absolute state
		state := s.commitsToNodeState(nodeCommits)
		if state != nil {
			s.AddNode(nodeCommits.NodeID, state.level, state.connections, state.hasTombstone)
		} else if state == nil {
			// Check if there's just a tombstone for this node
			for _, c := range nodeCommits.Commits {
				if _, ok := c.(*AddTombstoneCommit); ok {
					s.AddTombstone(nodeCommits.NodeID)
					break
				}
			}
		}
	}

	return s.Flush()
}

// commitsToNodeState converts a set of commits for a node into absolute state.
// Returns nil if the node was deleted or has no meaningful state.
func (s *SnapshotWriter) commitsToNodeState(nc *NodeCommits) *nodeStateWithTombstone {
	var level uint16
	var hasLevel bool
	connections := make(map[uint16][]uint64)
	deleted := false
	hasTombstone := false

	for _, c := range nc.Commits {
		switch ct := c.(type) {
		case *DeleteNodeCommit:
			deleted = true

		case *AddNodeCommit:
			if !deleted {
				level = ct.Level
				hasLevel = true
			}

		case *AddTombstoneCommit:
			hasTombstone = true

		case *RemoveTombstoneCommit:
			hasTombstone = false

		case *ReplaceLinksAtLevelCommit:
			if !deleted {
				connections[ct.Level] = ct.Targets
			}

		case *AddLinksAtLevelCommit:
			if !deleted {
				existing := connections[ct.Level]
				connections[ct.Level] = append(existing, ct.Targets...)
			}

		case *AddLinkAtLevelCommit:
			if !deleted {
				existing := connections[ct.Level]
				connections[ct.Level] = append(existing, ct.Target)
			}

		case *ClearLinksAtLevelCommit:
			if !deleted {
				connections[ct.Level] = nil
			}

		case *ClearLinksCommit:
			if !deleted {
				connections = make(map[uint16][]uint64)
			}
		}
	}

	// If node was deleted, return nil (no node data to write)
	if deleted {
		return nil
	}

	// If we have no level and no connections, this might be a tombstone-only node
	if !hasLevel && len(connections) == 0 {
		if hasTombstone {
			return &nodeStateWithTombstone{hasTombstone: true}
		}
		return nil
	}

	// Determine max level from connections if not explicitly set
	maxLevel := level
	for l := range connections {
		if l > maxLevel {
			maxLevel = l
		}
	}

	// Build connections slice
	connSlice := make([][]uint64, maxLevel+1)
	for l := uint16(0); l <= maxLevel; l++ {
		if conns, ok := connections[l]; ok {
			connSlice[l] = conns
		}
	}

	return &nodeStateWithTombstone{
		level:       maxLevel,
		connections: connSlice,
		hasTombstone: hasTombstone,
	}
}

// nodeStateWithTombstone extends nodeState with tombstone tracking.
type nodeStateWithTombstone struct {
	level        uint16
	connections  [][]uint64
	hasTombstone bool
}

// Note: Helper functions writeByte, writeBool, writeUint16, writeUint32, writeUint64
// are defined in wal_writer.go and shared across the package.
