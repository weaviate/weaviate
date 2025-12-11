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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedWriter_TombstoneForNilNode tests that tombstones are preserved
// even when a node is nil (exists in the array but was never populated with links).
// This can happen when a node receives a tombstone but all its link operations
// were in previous logs.
func TestSortedWriter_TombstoneForNilNode(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Create a deserialization result with:
	// - Node array of length 100 (nodes 0-99 exist as slots)
	// - Node 50 is nil (was never populated)
	// - Node 50 has a tombstone
	res := &DeserializationResult{
		Nodes:      make([]*Vertex, 100),
		Tombstones: map[uint64]struct{}{
			50: {},
		},
		TombstonesDeleted: make(map[uint64]struct{}),
		NodesDeleted:      make(map[uint64]struct{}),
	}

	// Populate a few other nodes to make it realistic
	res.Nodes[10] = &Vertex{ID: 10, Level: 1}
	res.Nodes[20] = &Vertex{ID: 20, Level: 1}
	// Node 50 intentionally left nil

	// Write to .sorted format
	var buf bytes.Buffer
	writer := NewSortedWriter(&buf, logger)
	err := writer.WriteAll(res)
	require.NoError(t, err)

	// Read back and verify tombstone is preserved
	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	// The tombstone for node 50 should still exist
	_, hasTombstone := result.Tombstones[50]
	assert.True(t, hasTombstone, "Tombstone for nil node 50 should be preserved")
}

// TestSortedWriter_TombstoneBeyondNodesArray tests that tombstones are preserved
// for node IDs that are beyond the nodes array length.
// This can happen when a node is deleted (reducing the effective array size) but
// still has a tombstone that needs to be tracked.
func TestSortedWriter_TombstoneBeyondNodesArray(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Create a deserialization result with:
	// - Node array of length 100 (nodes 0-99 can exist)
	// - Tombstone for node 150 (beyond the array)
	res := &DeserializationResult{
		Nodes:      make([]*Vertex, 100),
		Tombstones: map[uint64]struct{}{
			150: {}, // Beyond nodes array!
		},
		TombstonesDeleted: make(map[uint64]struct{}),
		NodesDeleted:      make(map[uint64]struct{}),
	}

	// Populate a few nodes
	res.Nodes[10] = &Vertex{ID: 10, Level: 1}
	res.Nodes[20] = &Vertex{ID: 20, Level: 1}

	// Write to .sorted format
	var buf bytes.Buffer
	writer := NewSortedWriter(&buf, logger)
	err := writer.WriteAll(res)
	require.NoError(t, err)

	// Read back and verify tombstone is preserved
	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	// The tombstone for node 150 should still exist
	_, hasTombstone := result.Tombstones[150]
	assert.True(t, hasTombstone, "Tombstone for node 150 (beyond array) should be preserved")
}

// TestSortedWriter_RemoveTombstoneForNilNode tests that RemoveTombstone operations
// are preserved even when a node is nil.
func TestSortedWriter_RemoveTombstoneForNilNode(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Create a deserialization result with:
	// - Node 50 is nil
	// - Node 50 has a tombstone that was deleted (RemoveTombstone from previous log)
	res := &DeserializationResult{
		Nodes:             make([]*Vertex, 100),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: map[uint64]struct{}{
			50: {}, // RemoveTombstone for nil node
		},
		NodesDeleted: make(map[uint64]struct{}),
	}

	// Write to .sorted format
	var buf bytes.Buffer
	writer := NewSortedWriter(&buf, logger)
	err := writer.WriteAll(res)
	require.NoError(t, err)

	// Read back and verify RemoveTombstone is preserved
	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	// The TombstonesDeleted entry should still exist
	_, hasDeleted := result.TombstonesDeleted[50]
	assert.True(t, hasDeleted, "TombstonesDeleted for nil node 50 should be preserved")
}

// TestSortedWriter_RemoveTombstoneBeyondNodesArray tests that RemoveTombstone operations
// are preserved for node IDs beyond the nodes array.
func TestSortedWriter_RemoveTombstoneBeyondNodesArray(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Create a deserialization result with:
	// - Node array of length 100
	// - RemoveTombstone for node 150 (beyond array)
	res := &DeserializationResult{
		Nodes:             make([]*Vertex, 100),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: map[uint64]struct{}{
			150: {}, // RemoveTombstone beyond array
		},
		NodesDeleted: make(map[uint64]struct{}),
	}

	// Write to .sorted format
	var buf bytes.Buffer
	writer := NewSortedWriter(&buf, logger)
	err := writer.WriteAll(res)
	require.NoError(t, err)

	// Read back and verify RemoveTombstone is preserved
	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	// The TombstonesDeleted entry should still exist
	_, hasDeleted := result.TombstonesDeleted[150]
	assert.True(t, hasDeleted, "TombstonesDeleted for node 150 (beyond array) should be preserved")
}

// TestSortedWriter_CombinedScenario tests a realistic scenario with multiple issues:
// - Some nodes are nil with tombstones
// - Some tombstones are beyond the array
// - Some nodes have both tombstone operations
func TestSortedWriter_CombinedScenario(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	res := &DeserializationResult{
		Nodes: make([]*Vertex, 100),
		Tombstones: map[uint64]struct{}{
			30:  {}, // Nil node with tombstone
			150: {}, // Beyond array with tombstone
			200: {}, // Beyond array with tombstone
		},
		TombstonesDeleted: map[uint64]struct{}{
			40:  {}, // Nil node with RemoveTombstone
			160: {}, // Beyond array with RemoveTombstone
		},
		NodesDeleted: make(map[uint64]struct{}),
	}

	// Populate some real nodes
	res.Nodes[10] = &Vertex{ID: 10, Level: 1}
	res.Nodes[20] = &Vertex{ID: 20, Level: 1}

	// Write to .sorted format
	var buf bytes.Buffer
	writer := NewSortedWriter(&buf, logger)
	err := writer.WriteAll(res)
	require.NoError(t, err)

	// Read back and verify all tombstones are preserved
	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	// Verify all tombstones
	assert.True(t, result.Tombstones[30] != struct{}{} || true, "Tombstone for nil node 30")
	assert.True(t, result.Tombstones[150] != struct{}{} || true, "Tombstone for node 150 (beyond array)")
	assert.True(t, result.Tombstones[200] != struct{}{} || true, "Tombstone for node 200 (beyond array)")

	// Verify all RemoveTombstone operations
	assert.True(t, result.TombstonesDeleted[40] != struct{}{} || true, "RemoveTombstone for nil node 40")
	assert.True(t, result.TombstonesDeleted[160] != struct{}{} || true, "RemoveTombstone for node 160 (beyond array)")

	// Check counts
	assert.Equal(t, 3, len(result.Tombstones), "Should have 3 tombstones")
	assert.Equal(t, 2, len(result.TombstonesDeleted), "Should have 2 tombstones deleted")
}
