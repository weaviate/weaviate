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

package compact

import (
	"bytes"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
	res := ent.NewDeserializationResult(100)
	res.Graph.Tombstones[50] = struct{}{}

	// Populate a few other nodes to make it realistic
	res.Graph.Nodes[10] = &ent.Vertex{ID: 10, Level: 1}
	res.Graph.Nodes[20] = &ent.Vertex{ID: 20, Level: 1}
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
	_, hasTombstone := result.Graph.Tombstones[50]
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
	res := ent.NewDeserializationResult(100)
	res.Graph.Tombstones[150] = struct{}{} // Beyond nodes array!

	// Populate a few nodes
	res.Graph.Nodes[10] = &ent.Vertex{ID: 10, Level: 1}
	res.Graph.Nodes[20] = &ent.Vertex{ID: 20, Level: 1}

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
	_, hasTombstone := result.Graph.Tombstones[150]
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
	res := ent.NewDeserializationResult(100)
	res.Graph.TombstonesDeleted[50] = struct{}{} // RemoveTombstone for nil node

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
	_, hasDeleted := result.Graph.TombstonesDeleted[50]
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
	res := ent.NewDeserializationResult(100)
	res.Graph.TombstonesDeleted[150] = struct{}{} // RemoveTombstone beyond array

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
	_, hasDeleted := result.Graph.TombstonesDeleted[150]
	assert.True(t, hasDeleted, "TombstonesDeleted for node 150 (beyond array) should be preserved")
}

// TestSortedWriter_CombinedScenario tests a realistic scenario with multiple issues:
// - Some nodes are nil with tombstones
// - Some tombstones are beyond the array
// - Some nodes have both tombstone operations
func TestSortedWriter_CombinedScenario(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	res := ent.NewDeserializationResult(100)
	// Set tombstones
	res.Graph.Tombstones[30] = struct{}{}  // Nil node with tombstone
	res.Graph.Tombstones[150] = struct{}{} // Beyond array with tombstone
	res.Graph.Tombstones[200] = struct{}{} // Beyond array with tombstone
	// Set tombstones deleted
	res.Graph.TombstonesDeleted[40] = struct{}{}  // Nil node with RemoveTombstone
	res.Graph.TombstonesDeleted[160] = struct{}{} // Beyond array with RemoveTombstone

	// Populate some real nodes
	res.Graph.Nodes[10] = &ent.Vertex{ID: 10, Level: 1}
	res.Graph.Nodes[20] = &ent.Vertex{ID: 20, Level: 1}

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
	_, hasTomb30 := result.Graph.Tombstones[30]
	assert.True(t, hasTomb30, "Tombstone for nil node 30")
	_, hasTomb150 := result.Graph.Tombstones[150]
	assert.True(t, hasTomb150, "Tombstone for node 150 (beyond array)")
	_, hasTomb200 := result.Graph.Tombstones[200]
	assert.True(t, hasTomb200, "Tombstone for node 200 (beyond array)")

	// Verify all RemoveTombstone operations
	_, hasDel40 := result.Graph.TombstonesDeleted[40]
	assert.True(t, hasDel40, "RemoveTombstone for nil node 40")
	_, hasDel160 := result.Graph.TombstonesDeleted[160]
	assert.True(t, hasDel160, "RemoveTombstone for node 160 (beyond array)")

	// Check counts
	assert.Equal(t, 3, len(result.Graph.Tombstones), "Should have 3 tombstones")
	assert.Equal(t, 2, len(result.Graph.TombstonesDeleted), "Should have 2 tombstones deleted")
}

// decodeAllCommits reads every commit from raw WAL bytes.
func decodeAllCommits(t *testing.T, raw []byte, logger logrus.FieldLogger) []Commit {
	t.Helper()

	r := NewWALCommitReader(bytes.NewReader(raw), logger)
	var out []Commit
	for {
		c, err := r.ReadNextCommit()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, c)
	}
}

// TestSortedWriter_DocIDReuse verifies the interaction between the reader's
// re-add reconciliation and the sorted writer's isDeleted-first classification.
// The DeserializationResult is intentionally produced by the reader (not
// hand-built): the writer must see the exact state the reader leaves behind.
func TestSortedWriter_DocIDReuse(t *testing.T) {
	const id = uint64(7)

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	t.Run("re-added node survives with its new life", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWALWriter(&buf)
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddNode(2, 0))
		// old life
		require.NoError(t, w.WriteAddNode(id, 2))
		require.NoError(t, w.WriteReplaceLinksAtLevel(id, 0, []uint64{1}))
		// delete lifecycle
		require.NoError(t, w.WriteAddTombstone(id))
		require.NoError(t, w.WriteDeleteNode(id))
		require.NoError(t, w.WriteRemoveTombstone(id))
		// new life (docID reuse)
		require.NoError(t, w.WriteAddNode(id, 1))
		require.NoError(t, w.WriteReplaceLinksAtLevel(id, 0, []uint64{2}))
		require.NoError(t, w.WriteReplaceLinksAtLevel(id, 1, []uint64{2}))

		reader := NewWALCommitReader(&buf, logger)
		memReader := NewInMemoryReader(reader, logger)
		res, err := memReader.Do(nil, true)
		require.NoError(t, err)

		var sortedBuf bytes.Buffer
		require.NoError(t, NewSortedWriter(&sortedBuf, logger).WriteAll(res))

		// The condensed output must contain the live re-added node and must
		// not delete or tombstone it.
		var sawAddNode bool
		for _, c := range decodeAllCommits(t, sortedBuf.Bytes(), logger) {
			switch ct := c.(type) {
			case *AddNodeCommit:
				if ct.ID == id {
					sawAddNode = true
					assert.Equal(t, uint16(1), ct.Level, "AddNode must carry the new life's level")
				}
			case *DeleteNodeCommit:
				assert.NotEqual(t, id, ct.ID, "re-added node must not be deleted in sorted output")
			case *AddTombstoneCommit:
				assert.NotEqual(t, id, ct.ID, "re-added node must not be tombstoned in sorted output")
			case *RemoveTombstoneCommit:
				assert.NotEqual(t, id, ct.ID, "old life's RemoveTombstone must not leak into sorted output")
			}
		}
		assert.True(t, sawAddNode, "sorted output must contain AddNode for the re-added node")

		// Replaying the sorted output must yield the live new life.
		replayed, err := NewInMemoryReader(NewWALCommitReader(&sortedBuf, logger), logger).Do(nil, true)
		require.NoError(t, err)
		require.NotNil(t, replayed.Graph.Nodes[id], "re-added node must survive the sorted round trip")
		assert.Equal(t, 1, replayed.Graph.Nodes[id].Level)
		assert.Equal(t, []uint64{2}, replayed.Graph.Nodes[id].Connections.GetLayer(0))
		assert.Equal(t, []uint64{2}, replayed.Graph.Nodes[id].Connections.GetLayer(1))
		assert.NotContains(t, replayed.Graph.NodesDeleted, id)
		assert.NotContains(t, replayed.Graph.Tombstones, id)
		assert.NotContains(t, replayed.Graph.TombstonesDeleted, id)
	})

	t.Run("deleted node without re-add still emits DeleteNode", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWALWriter(&buf)
		require.NoError(t, w.WriteAddNode(id, 1))
		require.NoError(t, w.WriteDeleteNode(id))

		reader := NewWALCommitReader(&buf, logger)
		memReader := NewInMemoryReader(reader, logger)
		res, err := memReader.Do(nil, true)
		require.NoError(t, err)

		var sortedBuf bytes.Buffer
		require.NoError(t, NewSortedWriter(&sortedBuf, logger).WriteAll(res))

		var sawDelete bool
		for _, c := range decodeAllCommits(t, sortedBuf.Bytes(), logger) {
			switch ct := c.(type) {
			case *DeleteNodeCommit:
				if ct.ID == id {
					sawDelete = true
				}
			case *AddNodeCommit:
				assert.NotEqual(t, id, ct.ID, "deleted node must not be re-added in sorted output")
			}
		}
		assert.True(t, sawDelete, "DeleteNode must still be emitted when the node is not re-added")

		replayed, err := NewInMemoryReader(NewWALCommitReader(&sortedBuf, logger), logger).Do(nil, true)
		require.NoError(t, err)
		assert.Nil(t, replayed.Graph.Nodes[id])
		assert.Contains(t, replayed.Graph.NodesDeleted, id)
	})
}

// TestSortedWriter_LiveTombstonedNode_SurvivesSortedRoundTrip guards the
// sorted format's canonical per-node ordering: AddTombstone is written BEFORE
// AddNode for a live tombstoned node. The reader's docID-reuse reconciliation
// must not treat that AddNode as a re-add, or the live tombstone would be
// destroyed on every re-read of a .sorted/.condensed file.
func TestSortedWriter_LiveTombstonedNode_SurvivesSortedRoundTrip(t *testing.T) {
	const id = uint64(7)

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	var buf bytes.Buffer
	w := NewWALWriter(&buf)
	require.NoError(t, w.WriteAddNode(1, 0))
	require.NoError(t, w.WriteAddNode(id, 2))
	require.NoError(t, w.WriteReplaceLinksAtLevel(id, 0, []uint64{1}))
	require.NoError(t, w.WriteAddTombstone(id))

	res, err := NewInMemoryReader(NewWALCommitReader(&buf, logger), logger).Do(nil, true)
	require.NoError(t, err)

	// Round trip through the sorted format twice: the second pass reads a file
	// in which AddTombstone(id) precedes AddNode(id).
	for i := 0; i < 2; i++ {
		var sortedBuf bytes.Buffer
		require.NoError(t, NewSortedWriter(&sortedBuf, logger).WriteAll(res))
		res, err = NewInMemoryReader(NewWALCommitReader(&sortedBuf, logger), logger).Do(nil, true)
		require.NoError(t, err)

		require.NotNil(t, res.Graph.Nodes[id], "round trip %d: node must stay live", i)
		assert.Equal(t, 2, res.Graph.Nodes[id].Level, "round trip %d", i)
		assert.Contains(t, res.Graph.Tombstones, id,
			"round trip %d: live tombstone must survive the sorted round trip", i)
	}
}
