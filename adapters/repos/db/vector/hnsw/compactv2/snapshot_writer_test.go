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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
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
		{1, 2, 3}, // level 0
		{4, 5},    // level 1
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
		{1, 2, 3, 4, 5}, // level 0 - small IDs
		{10, 20, 30},    // level 1
		{100, 200},      // level 2
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

func TestSnapshotRoundTrip(t *testing.T) {
	// Write a snapshot with multiple nodes and tombstones
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(5, 2)

	// Add nodes with various levels and connections
	testNodes := []struct {
		id          uint64
		level       uint16
		connections [][]uint64
		tombstone   bool
	}{
		{0, 0, [][]uint64{{1, 2, 3}}, false},
		{1, 1, [][]uint64{{0, 2}, {5}}, false},
		{2, 0, [][]uint64{{0, 1}}, true}, // with tombstone
		// node 3 is nil
		// node 4 is nil
		{5, 2, [][]uint64{{0, 1}, {1}, {1}}, false}, // entrypoint
	}

	for _, n := range testNodes {
		sw.AddNode(n.id, n.level, n.connections, n.tombstone)
	}

	err := sw.Flush()
	require.NoError(t, err)

	// Read it back
	reader := bytes.NewReader(buf.Bytes())
	sr := NewSnapshotReader(logrus.New())
	result, err := sr.Read(reader)
	require.NoError(t, err)

	// Verify metadata
	assert.Equal(t, uint64(5), result.Graph.Entrypoint)
	assert.Equal(t, uint16(2), result.Graph.Level)
	assert.True(t, result.EntrypointChanged())
	assert.False(t, result.Compressed())
	assert.False(t, result.MuveraEnabled())

	// Verify nodes
	assert.Equal(t, 6, len(result.Graph.Nodes)) // nodes 0-5

	// Verify each node
	for _, expected := range testNodes {
		node := result.Graph.Nodes[expected.id]
		require.NotNil(t, node, "node %d should exist", expected.id)
		assert.Equal(t, expected.id, node.ID)
		assert.Equal(t, int(expected.level), node.Level)

		// Verify connections
		for level, expectedConns := range expected.connections {
			actualConns := node.Connections.GetLayer(uint8(level))
			assert.Equal(t, expectedConns, actualConns, "node %d level %d connections", expected.id, level)
		}
	}

	// Verify nil nodes
	assert.Nil(t, result.Graph.Nodes[3])
	assert.Nil(t, result.Graph.Nodes[4])

	// Verify tombstones
	assert.Equal(t, 1, len(result.Graph.Tombstones))
	_, hasTombstone := result.Graph.Tombstones[2]
	assert.True(t, hasTombstone, "node 2 should have tombstone")
}

func TestSnapshotReader_ChecksumValidation(t *testing.T) {
	// Write a valid snapshot
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)
	sw.AddNode(0, 0, [][]uint64{{1}}, false)
	err := sw.Flush()
	require.NoError(t, err)

	// Corrupt the metadata checksum
	data := buf.Bytes()
	data[1] ^= 0xFF // flip bits in checksum

	reader := bytes.NewReader(data)
	sr := NewSnapshotReader(logrus.New())
	_, err = sr.Read(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checksum")
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

func TestSnapshotRoundTrip_WithSQCompression(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	// Set SQ compression data
	sqData := &compression.SQData{
		A:          0.5,
		B:          1.5,
		Dimensions: 128,
	}
	sw.SetSQData(sqData)

	// Add a simple node
	sw.AddNode(0, 0, [][]uint64{{1, 2}}, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Read it back
	reader := bytes.NewReader(buf.Bytes())
	sr := NewSnapshotReader(logrus.New())
	result, err := sr.Read(reader)
	require.NoError(t, err)

	// Verify compression data
	assert.True(t, result.Compressed())
	require.NotNil(t, result.CompressionSQData())
	assert.Equal(t, sqData.A, result.CompressionSQData().A)
	assert.Equal(t, sqData.B, result.CompressionSQData().B)
	assert.Equal(t, sqData.Dimensions, result.CompressionSQData().Dimensions)

	// Verify node data is also correct
	assert.Equal(t, 1, len(result.Graph.Nodes))
	assert.NotNil(t, result.Graph.Nodes[0])
}

func TestSnapshotRoundTrip_WithRQCompression(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	// Set RQ compression data
	rqData := &compression.RQData{
		InputDim: 128,
		Bits:     4,
		Rotation: compression.FastRotation{
			OutputDim: 64,
			Rounds:    2,
			Swaps: [][]compression.Swap{
				{{I: 0, J: 1}, {I: 2, J: 3}},
				{{I: 4, J: 5}, {I: 6, J: 7}},
			},
			Signs: [][]float32{
				make([]float32, 64),
				make([]float32, 64),
			},
		},
	}
	// Fill signs with some test data
	for i := range rqData.Rotation.Signs {
		for j := range rqData.Rotation.Signs[i] {
			rqData.Rotation.Signs[i][j] = float32(j) * 0.1
		}
	}
	// Pad swaps to match outputDim/2
	for i := range rqData.Rotation.Swaps {
		for len(rqData.Rotation.Swaps[i]) < int(rqData.Rotation.OutputDim/2) {
			rqData.Rotation.Swaps[i] = append(rqData.Rotation.Swaps[i], compression.Swap{I: 0, J: 1})
		}
	}
	sw.SetRQData(rqData)

	// Add a simple node
	sw.AddNode(0, 0, [][]uint64{{1}}, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Read it back
	reader := bytes.NewReader(buf.Bytes())
	sr := NewSnapshotReader(logrus.New())
	result, err := sr.Read(reader)
	require.NoError(t, err)

	// Verify compression data
	assert.True(t, result.Compressed())
	require.NotNil(t, result.CompressionRQData())
	assert.Equal(t, rqData.InputDim, result.CompressionRQData().InputDim)
	assert.Equal(t, rqData.Bits, result.CompressionRQData().Bits)
	assert.Equal(t, rqData.Rotation.OutputDim, result.CompressionRQData().Rotation.OutputDim)
	assert.Equal(t, rqData.Rotation.Rounds, result.CompressionRQData().Rotation.Rounds)
}

func TestSnapshotRoundTrip_WithBRQCompression(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	// Set BRQ compression data
	brqData := &compression.BRQData{
		InputDim: 128,
		Rotation: compression.FastRotation{
			OutputDim: 64,
			Rounds:    2,
			Swaps: [][]compression.Swap{
				make([]compression.Swap, 32),
				make([]compression.Swap, 32),
			},
			Signs: [][]float32{
				make([]float32, 64),
				make([]float32, 64),
			},
		},
		Rounding: make([]float32, 64),
	}
	// Fill with test data
	for i := range brqData.Rotation.Swaps {
		for j := range brqData.Rotation.Swaps[i] {
			brqData.Rotation.Swaps[i][j] = compression.Swap{I: uint16(j), J: uint16(j + 1)}
		}
	}
	for i := range brqData.Rotation.Signs {
		for j := range brqData.Rotation.Signs[i] {
			brqData.Rotation.Signs[i][j] = float32(j) * 0.01
		}
	}
	for i := range brqData.Rounding {
		brqData.Rounding[i] = float32(i) * 0.001
	}
	sw.SetBRQData(brqData)

	// Add a simple node
	sw.AddNode(0, 0, [][]uint64{{1}}, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Read it back
	reader := bytes.NewReader(buf.Bytes())
	sr := NewSnapshotReader(logrus.New())
	result, err := sr.Read(reader)
	require.NoError(t, err)

	// Verify compression data
	assert.True(t, result.Compressed())
	require.NotNil(t, result.CompressionBRQData())
	assert.Equal(t, brqData.InputDim, result.CompressionBRQData().InputDim)
	assert.Equal(t, brqData.Rotation.OutputDim, result.CompressionBRQData().Rotation.OutputDim)
	assert.Equal(t, brqData.Rotation.Rounds, result.CompressionBRQData().Rotation.Rounds)
	assert.Equal(t, brqData.Rounding, result.CompressionBRQData().Rounding)
}

func TestSnapshotRoundTrip_WithMuvera(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	// Set Muvera data
	muveraData := &multivector.MuveraData{
		Dimensions:   8,
		NumClusters:  2,
		KSim:         2,
		DProjections: 2,
		Repetitions:  2,
		Gaussians:    make([][][]float32, 2),
		S:            make([][][]float32, 2),
	}
	// Initialize gaussians and S matrices
	for i := uint32(0); i < muveraData.Repetitions; i++ {
		muveraData.Gaussians[i] = make([][]float32, muveraData.KSim)
		for j := uint32(0); j < muveraData.KSim; j++ {
			muveraData.Gaussians[i][j] = make([]float32, muveraData.Dimensions)
			for k := uint32(0); k < muveraData.Dimensions; k++ {
				muveraData.Gaussians[i][j][k] = float32(i*100+j*10) + float32(k)*0.1
			}
		}
		muveraData.S[i] = make([][]float32, muveraData.DProjections)
		for j := uint32(0); j < muveraData.DProjections; j++ {
			muveraData.S[i][j] = make([]float32, muveraData.Dimensions)
			for k := uint32(0); k < muveraData.Dimensions; k++ {
				muveraData.S[i][j][k] = float32(i*200+j*20) + float32(k)*0.2
			}
		}
	}
	sw.SetMuveraData(muveraData)

	// Add a simple node
	sw.AddNode(0, 0, [][]uint64{{1}}, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Read it back
	reader := bytes.NewReader(buf.Bytes())
	sr := NewSnapshotReader(logrus.New())
	result, err := sr.Read(reader)
	require.NoError(t, err)

	// Verify Muvera data
	assert.True(t, result.MuveraEnabled())
	require.NotNil(t, result.EncoderMuvera())
	assert.Equal(t, muveraData.Dimensions, result.EncoderMuvera().Dimensions)
	assert.Equal(t, muveraData.NumClusters, result.EncoderMuvera().NumClusters)
	assert.Equal(t, muveraData.KSim, result.EncoderMuvera().KSim)
	assert.Equal(t, muveraData.DProjections, result.EncoderMuvera().DProjections)
	assert.Equal(t, muveraData.Repetitions, result.EncoderMuvera().Repetitions)

	// Verify gaussians content
	for i := range muveraData.Gaussians {
		for j := range muveraData.Gaussians[i] {
			assert.Equal(t, muveraData.Gaussians[i][j], result.EncoderMuvera().Gaussians[i][j])
		}
	}
}

func TestSnapshotRoundTrip_WithCompressionAndMuvera(t *testing.T) {
	var buf bytes.Buffer
	sw := NewSnapshotWriter(&buf)
	sw.SetEntrypoint(0, 0)

	// Set SQ compression
	sqData := &compression.SQData{
		A:          0.25,
		B:          0.75,
		Dimensions: 64,
	}
	sw.SetSQData(sqData)

	// Set Muvera data
	muveraData := &multivector.MuveraData{
		Dimensions:   4,
		NumClusters:  1,
		KSim:         1,
		DProjections: 1,
		Repetitions:  1,
		Gaussians:    [][][]float32{{{1.0, 2.0, 3.0, 4.0}}},
		S:            [][][]float32{{{0.1, 0.2, 0.3, 0.4}}},
	}
	sw.SetMuveraData(muveraData)

	// Add a node
	sw.AddNode(0, 0, [][]uint64{{1, 2, 3}}, false)

	err := sw.Flush()
	require.NoError(t, err)

	// Read it back
	reader := bytes.NewReader(buf.Bytes())
	sr := NewSnapshotReader(logrus.New())
	result, err := sr.Read(reader)
	require.NoError(t, err)

	// Verify both compression and Muvera are present
	assert.True(t, result.Compressed())
	assert.True(t, result.MuveraEnabled())
	require.NotNil(t, result.CompressionSQData())
	require.NotNil(t, result.EncoderMuvera())

	// Verify SQ data
	assert.Equal(t, sqData.A, result.CompressionSQData().A)
	assert.Equal(t, sqData.B, result.CompressionSQData().B)

	// Verify Muvera data
	assert.Equal(t, muveraData.Dimensions, result.EncoderMuvera().Dimensions)
	assert.Equal(t, muveraData.Gaussians[0][0], result.EncoderMuvera().Gaussians[0][0])
}

func TestSnapshotWriter_CompressionHelpers(t *testing.T) {
	t.Run("isCompressed returns false when no compression set", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		assert.False(t, sw.isCompressed())
	})

	t.Run("isCompressed returns true for PQ", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		sw.SetPQData(&compression.PQData{})
		assert.True(t, sw.isCompressed())
	})

	t.Run("isCompressed returns true for SQ", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		sw.SetSQData(&compression.SQData{})
		assert.True(t, sw.isCompressed())
	})

	t.Run("isCompressed returns true for RQ", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		sw.SetRQData(&compression.RQData{})
		assert.True(t, sw.isCompressed())
	})

	t.Run("isCompressed returns true for BRQ", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		sw.SetBRQData(&compression.BRQData{})
		assert.True(t, sw.isCompressed())
	})

	t.Run("isMuveraEnabled returns false when no muvera set", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		assert.False(t, sw.isMuveraEnabled())
	})

	t.Run("isMuveraEnabled returns true when muvera set", func(t *testing.T) {
		sw := NewSnapshotWriter(nil)
		sw.SetMuveraData(&multivector.MuveraData{})
		assert.True(t, sw.isMuveraEnabled())
	})
}
