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
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
)

func TestSnapshotWriter_EmptySnapshot(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	err := sw.Flush()
	require.NoError(t, err)

	// Verify we can read the header
	data := buf.Bytes()
	require.Greater(t, len(data), 9, "should have at least header bytes")

	// Check version
	assert.Equal(t, byte(snapshotVersionV3), data[0])
}

func TestSnapshotWriter_SingleNode(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 1)

	// Add a single node with connections
	connections := [][]uint64{
		{1, 2, 3},    // level 0
		{4, 5},       // level 1
	}
	sw.AddNode(0, 1, connections, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Verify metadata
	data := buf.Bytes()
	verifySnapshotMetadata(t, data, 0, 1, 1)
}

func TestSnapshotWriter_MultipleNodes(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(5, 2)

	// Add multiple nodes
	for i := uint64(0); i < 10; i++ {
		connections := [][]uint64{
			{(i + 1) % 10, (i + 2) % 10}, // level 0
		}
		sw.AddNode(i, 0, connections, false)
	}

	err := sw.Flush()
	require.NoError(t, err)

	// Verify metadata
	data := buf.Bytes()
	verifySnapshotMetadata(t, data, 5, 2, 10)
}

func TestSnapshotWriter_WithTombstones(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	// Add node with tombstone
	connections := [][]uint64{{1, 2}}
	sw.AddNode(0, 0, connections, true)

	// Add standalone tombstone (deleted node)
	sw.AddTombstone(5)

	err := sw.Flush()
	require.NoError(t, err)

	data := buf.Bytes()
	// Should have nodes array of size 6 (0 through 5)
	verifySnapshotMetadata(t, data, 0, 0, 6)
}

func TestSnapshotWriter_SmallBlockSize(t *testing.T) {
	// Use a very small block size to test block splitting
	var buf bytes.Buffer
	sw := NewSnapshotWriterWithBlockSize(&buf, 128)
	sw.SetEntrypoint(0, 0)

	// Add enough nodes to force multiple blocks
	for i := uint64(0); i < 20; i++ {
		connections := [][]uint64{
			{(i + 1) % 20, (i + 2) % 20, (i + 3) % 20},
		}
		sw.AddNode(i, 0, connections, false)
	}

	err := sw.Flush()
	require.NoError(t, err)

	// Just verify it doesn't crash and produces output
	data := buf.Bytes()
	require.Greater(t, len(data), 128, "should have multiple blocks")
}

func TestSnapshotWriter_BlockChecksums(t *testing.T) {
	var buf bytes.Buffer
	// Use small block size to get at least one complete block
	sw := NewSnapshotWriterWithBlockSize(&buf, 256)
	sw.SetEntrypoint(0, 0)

	// Add enough data to fill at least one block
	for i := uint64(0); i < 50; i++ {
		connections := [][]uint64{{(i + 1) % 50}}
		sw.AddNode(i, 0, connections, false)
	}

	err := sw.Flush()
	require.NoError(t, err)

	data := buf.Bytes()

	// Find where body starts (after metadata)
	metadataSize := binary.LittleEndian.Uint32(data[5:9])
	bodyStart := 9 + int(metadataSize)

	if bodyStart < len(data) {
		// Verify first block checksum
		blockChecksum := binary.LittleEndian.Uint32(data[bodyStart : bodyStart+4])
		blockData := data[bodyStart+4 : bodyStart+256]

		hasher := crc32.NewIEEE()
		hasher.Write(blockData)
		expectedChecksum := hasher.Sum32()

		assert.Equal(t, expectedChecksum, blockChecksum, "block checksum should match")
	}
}

func TestSnapshotWriter_PackedConnections(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 2)

	// Test with various connection patterns
	connections := [][]uint64{
		{1, 2, 3, 4, 5},           // level 0 - small IDs
		{10, 20, 30},              // level 1
		{100, 200},                // level 2
	}
	sw.AddNode(0, 2, connections, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Verify packed connections can be read back
	packed, err := sw.packConnections(2, connections)
	require.NoError(t, err)
	require.NotNil(t, packed)

	// Use packedconn to decode and verify
	pc := packedconn.NewWithData(packed)
	assert.Equal(t, uint8(3), pc.Layers())

	level0 := pc.GetLayer(0)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, level0)

	level1 := pc.GetLayer(1)
	assert.Equal(t, []uint64{10, 20, 30}, level1)

	level2 := pc.GetLayer(2)
	assert.Equal(t, []uint64{100, 200}, level2)
}

func TestSnapshotWriter_CommitsToNodeState(t *testing.T) {
	sw := NewSnapshotWriter(nil)

	t.Run("simple node", func(t *testing.T) {
		nc := &NodeCommits{
			NodeID: 5,
			Commits: []Commit{
				&AddNodeCommit{ID: 5, Level: 2},
				&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2, 3}},
				&ReplaceLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{10}},
			},
		}

		state := sw.commitsToNodeState(nc)
		require.NotNil(t, state)
		assert.Equal(t, uint16(2), state.level)
		assert.Equal(t, []uint64{1, 2, 3}, state.connections[0])
		assert.Equal(t, []uint64{10}, state.connections[1])
		assert.False(t, state.hasTombstone)
	})

	t.Run("deleted node", func(t *testing.T) {
		nc := &NodeCommits{
			NodeID: 5,
			Commits: []Commit{
				&DeleteNodeCommit{ID: 5},
			},
		}

		state := sw.commitsToNodeState(nc)
		assert.Nil(t, state)
	})

	t.Run("node with tombstone", func(t *testing.T) {
		nc := &NodeCommits{
			NodeID: 5,
			Commits: []Commit{
				&AddNodeCommit{ID: 5, Level: 1},
				&AddTombstoneCommit{ID: 5},
				&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1}},
			},
		}

		state := sw.commitsToNodeState(nc)
		require.NotNil(t, state)
		assert.True(t, state.hasTombstone)
	})

	t.Run("add links accumulation", func(t *testing.T) {
		nc := &NodeCommits{
			NodeID: 5,
			Commits: []Commit{
				&AddNodeCommit{ID: 5, Level: 0},
				&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
				&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{3, 4}},
				&AddLinkAtLevelCommit{Source: 5, Level: 0, Target: 5},
			},
		}

		state := sw.commitsToNodeState(nc)
		require.NotNil(t, state)
		assert.Equal(t, []uint64{1, 2, 3, 4, 5}, state.connections[0])
	})

	t.Run("tombstone only node", func(t *testing.T) {
		nc := &NodeCommits{
			NodeID: 5,
			Commits: []Commit{
				&AddTombstoneCommit{ID: 5},
			},
		}

		state := sw.commitsToNodeState(nc)
		require.NotNil(t, state)
		assert.True(t, state.hasTombstone)
		assert.Empty(t, state.connections)
	})
}

// verifySnapshotMetadata checks the snapshot header values
func verifySnapshotMetadata(t *testing.T, data []byte, expectedEntrypoint uint64, expectedLevel uint16, expectedNodeCount uint32) {
	t.Helper()

	require.Greater(t, len(data), 9, "data too short for header")

	// Version
	assert.Equal(t, byte(snapshotVersionV3), data[0])

	// Read checksum and metadata size
	checksum := binary.LittleEndian.Uint32(data[1:5])
	metadataSize := binary.LittleEndian.Uint32(data[5:9])

	require.Greater(t, len(data), int(9+metadataSize), "data too short for metadata")

	// Verify checksum
	hasher := crc32.NewIEEE()
	hasher.Write([]byte{snapshotVersionV3})
	binary.Write(hasher, binary.LittleEndian, metadataSize)
	hasher.Write(data[9 : 9+metadataSize])
	assert.Equal(t, hasher.Sum32(), checksum, "metadata checksum mismatch")

	// Read metadata
	metadata := data[9 : 9+metadataSize]
	offset := 0

	// Entrypoint
	entrypoint := binary.LittleEndian.Uint64(metadata[offset:])
	offset += 8
	assert.Equal(t, expectedEntrypoint, entrypoint)

	// Level
	level := binary.LittleEndian.Uint16(metadata[offset:])
	offset += 2
	assert.Equal(t, expectedLevel, level)

	// isCompressed (should be false for MVP)
	assert.Equal(t, byte(0), metadata[offset])
	offset++

	// isEncoded (should be false for MVP)
	assert.Equal(t, byte(0), metadata[offset])
	offset++

	// Node count
	nodeCount := binary.LittleEndian.Uint32(metadata[offset:])
	assert.Equal(t, expectedNodeCount, nodeCount)
}
