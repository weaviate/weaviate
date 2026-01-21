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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

func TestSnapshotIterator_EmptySnapshot(t *testing.T) {
	result := ent.NewDeserializationResult(0)

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	assert.Equal(t, 0, it.ID())
	assert.True(t, it.Exhausted())
	assert.Nil(t, it.Current())
	assert.Empty(t, it.GlobalCommits())
}

func TestSnapshotIterator_GlobalCommits(t *testing.T) {
	result := ent.NewDeserializationResult(0)
	result.Graph.EntrypointChanged = true
	result.Graph.Entrypoint = 42
	result.Graph.Level = 3
	result.SetCompressed(true)
	result.SetCompressionSQData(&compression.SQData{
		A:          0.5,
		B:          1.5,
		Dimensions: 128,
	})

	it, err := NewSnapshotIteratorFromResult(result, 5, logrus.New())
	require.NoError(t, err)

	assert.Equal(t, 5, it.ID())

	globals := it.GlobalCommits()
	require.Len(t, globals, 2)

	// SQ commit should come first
	sqCommit, ok := globals[0].(*AddSQCommit)
	require.True(t, ok)
	assert.Equal(t, uint16(128), sqCommit.Data.Dimensions)

	// Entrypoint commit
	epCommit, ok := globals[1].(*SetEntryPointMaxLevelCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(42), epCommit.Entrypoint)
	assert.Equal(t, uint16(3), epCommit.Level)
}

func TestSnapshotIterator_SingleNode(t *testing.T) {
	// Create a node with connections
	conns, err := packedconn.NewWithMaxLayer(1)
	require.NoError(t, err)
	conns.BulkInsertAtLayer([]uint64{10, 20, 30}, 0)
	conns.BulkInsertAtLayer([]uint64{100, 200}, 1)

	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				{ID: 0, Level: 1, Connections: conns},
			},
			Tombstones:        make(map[uint64]struct{}),
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	// Should not be exhausted
	assert.False(t, it.Exhausted())

	// Get current node
	nc := it.Current()
	require.NotNil(t, nc)
	assert.Equal(t, uint64(0), nc.NodeID)

	// Check commits for this node
	require.GreaterOrEqual(t, len(nc.Commits), 1)

	// First should be AddNode
	addNode, ok := nc.Commits[0].(*AddNodeCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(0), addNode.ID)
	assert.Equal(t, uint16(1), addNode.Level)

	// Should have ReplaceLinksAtLevel commits for level 0 and 1
	var foundLevel0, foundLevel1 bool
	for _, c := range nc.Commits {
		if rl, ok := c.(*ReplaceLinksAtLevelCommit); ok {
			if rl.Level == 0 {
				foundLevel0 = true
				assert.Equal(t, []uint64{10, 20, 30}, rl.Targets)
			}
			if rl.Level == 1 {
				foundLevel1 = true
				assert.Equal(t, []uint64{100, 200}, rl.Targets)
			}
		}
	}
	assert.True(t, foundLevel0, "should have level 0 links")
	assert.True(t, foundLevel1, "should have level 1 links")

	// Next should exhaust the iterator
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
	assert.True(t, it.Exhausted())
}

func TestSnapshotIterator_MultipleNodes(t *testing.T) {
	// Create multiple nodes
	conns0, err := packedconn.NewWithMaxLayer(0)
	require.NoError(t, err)
	conns0.BulkInsertAtLayer([]uint64{1, 2}, 0)

	conns1, err := packedconn.NewWithMaxLayer(0)
	require.NoError(t, err)
	conns1.BulkInsertAtLayer([]uint64{0, 2}, 0)

	conns2, err := packedconn.NewWithMaxLayer(1)
	require.NoError(t, err)
	conns2.BulkInsertAtLayer([]uint64{0, 1}, 0)
	conns2.BulkInsertAtLayer([]uint64{3}, 1)

	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				{ID: 0, Level: 0, Connections: conns0},
				{ID: 1, Level: 0, Connections: conns1},
				{ID: 2, Level: 1, Connections: conns2},
			},
			Tombstones:        make(map[uint64]struct{}),
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	// Collect all nodes
	var nodeIDs []uint64
	for !it.Exhausted() {
		nc := it.Current()
		require.NotNil(t, nc)
		nodeIDs = append(nodeIDs, nc.NodeID)

		_, err = it.Next()
		require.NoError(t, err)
	}

	// Should have visited all three nodes in order
	assert.Equal(t, []uint64{0, 1, 2}, nodeIDs)
}

func TestSnapshotIterator_NodesWithTombstones(t *testing.T) {
	// Create a node with a tombstone
	conns, err := packedconn.NewWithMaxLayer(0)
	require.NoError(t, err)
	conns.BulkInsertAtLayer([]uint64{1}, 0)

	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				{ID: 0, Level: 0, Connections: conns},
			},
			Tombstones: map[uint64]struct{}{
				0: {}, // Node 0 has a tombstone
			},
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	nc := it.Current()
	require.NotNil(t, nc)

	// Should have AddTombstone commit
	var foundTombstone bool
	for _, c := range nc.Commits {
		if _, ok := c.(*AddTombstoneCommit); ok {
			foundTombstone = true
			break
		}
	}
	assert.True(t, foundTombstone, "should have tombstone commit")
}

func TestSnapshotIterator_NilNodesWithTombstones(t *testing.T) {
	// Create result with nil nodes but tombstones
	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				nil, // Node 0 is nil
				nil, // Node 1 is nil
				nil, // Node 2 is nil
			},
			Tombstones: map[uint64]struct{}{
				1: {}, // Node 1 has a tombstone
			},
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	// Should find node 1 with tombstone
	assert.False(t, it.Exhausted())
	nc := it.Current()
	require.NotNil(t, nc)
	assert.Equal(t, uint64(1), nc.NodeID)

	// Should have exactly one commit: AddTombstone
	require.Len(t, nc.Commits, 1)
	_, ok := nc.Commits[0].(*AddTombstoneCommit)
	assert.True(t, ok)

	// Next should exhaust
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestSnapshotIterator_SparseNodes(t *testing.T) {
	// Create sparse nodes array with gaps
	conns, err := packedconn.NewWithMaxLayer(0)
	require.NoError(t, err)
	conns.BulkInsertAtLayer([]uint64{2}, 0)

	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				nil,                                   // Node 0 is nil
				nil,                                   // Node 1 is nil
				{ID: 2, Level: 0, Connections: conns}, // Node 2 exists
				nil,                                   // Node 3 is nil
			},
			Tombstones:        make(map[uint64]struct{}),
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	// Should skip to node 2
	assert.False(t, it.Exhausted())
	nc := it.Current()
	require.NotNil(t, nc)
	assert.Equal(t, uint64(2), nc.NodeID)

	// Next should exhaust
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestSnapshotIterator_TombstonesBeyondNodesArray(t *testing.T) {
	// Create result with tombstones beyond the nodes array
	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				nil, // Only one position in array
			},
			Tombstones: map[uint64]struct{}{
				5:  {}, // Beyond nodes array
				10: {}, // Beyond nodes array
			},
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	// Should find tombstones beyond the array
	var nodeIDs []uint64
	for !it.Exhausted() {
		nc := it.Current()
		require.NotNil(t, nc)
		nodeIDs = append(nodeIDs, nc.NodeID)

		_, err = it.Next()
		require.NoError(t, err)
	}

	// Should have visited both tombstone nodes (in sorted order)
	assert.Equal(t, []uint64{5, 10}, nodeIDs)
}

func TestSnapshotIterator_ImplementsIteratorLike(t *testing.T) {
	// This test verifies that SnapshotIterator can be used where Iterator is expected
	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes:             make([]*ent.Vertex, 0),
			Tombstones:        make(map[uint64]struct{}),
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	// Verify it can be assigned to IteratorLike
	var iterLike IteratorLike = it
	assert.NotNil(t, iterLike)
}

func TestSnapshotIterator_NodeWithEmptyConnections(t *testing.T) {
	// Create a node with no connections (empty packedconn)
	conns, err := packedconn.NewWithMaxLayer(0)
	require.NoError(t, err)
	// Don't add any connections

	result := &ent.DeserializationResult{
		Graph: &ent.GraphState{
			Nodes: []*ent.Vertex{
				{ID: 0, Level: 0, Connections: conns},
			},
			Tombstones:        make(map[uint64]struct{}),
			NodesDeleted:      make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		},
	}

	it, err := NewSnapshotIteratorFromResult(result, 0, logrus.New())
	require.NoError(t, err)

	nc := it.Current()
	require.NotNil(t, nc)

	// Should have at least AddNode commit
	require.GreaterOrEqual(t, len(nc.Commits), 1)
	_, ok := nc.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)

	// Should not have any ReplaceLinksAtLevel commits with empty targets
	for _, c := range nc.Commits {
		if rl, ok := c.(*ReplaceLinksAtLevelCommit); ok {
			assert.NotEmpty(t, rl.Targets, "should not emit empty link commits")
		}
	}
}
