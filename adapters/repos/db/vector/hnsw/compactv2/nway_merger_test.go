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
)

func TestNWayMerger_SingleIterator(t *testing.T) {
	commits := []Commit{
		&SetEntryPointMaxLevelCommit{Entrypoint: 100, Level: 3},
		&AddNodeCommit{ID: 1, Level: 0},
		&AddLinksAtLevelCommit{Source: 1, Level: 0, Targets: []uint64{2, 3}},
		&AddNodeCommit{ID: 5, Level: 1},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it}, logrus.New())
	require.NoError(t, err)

	// Check global commits
	globalCommits := merger.GlobalCommits()
	assert.Equal(t, 1, len(globalCommits))

	// Check node 1
	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)
	assert.Equal(t, uint64(1), nodeCommits.NodeID)
	assert.Equal(t, 2, len(nodeCommits.Commits))

	// Check node 5
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)
	assert.Equal(t, uint64(5), nodeCommits.NodeID)
	assert.Equal(t, 1, len(nodeCommits.Commits))

	// Should be done
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	assert.Nil(t, nodeCommits)
}

func TestNWayMerger_TwoIteratorsNoOverlap(t *testing.T) {
	// Iterator 0 (older) has nodes 1, 3
	commits0 := []Commit{
		&AddNodeCommit{ID: 1, Level: 0},
		&AddNodeCommit{ID: 3, Level: 1},
	}

	// Iterator 1 (newer) has nodes 2, 4
	commits1 := []Commit{
		&AddNodeCommit{ID: 2, Level: 0},
		&AddNodeCommit{ID: 4, Level: 1},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1}, logrus.New())
	require.NoError(t, err)

	// Should get nodes in sorted order: 1, 2, 3, 4
	expectedIDs := []uint64{1, 2, 3, 4}
	for _, expectedID := range expectedIDs {
		nodeCommits, err := merger.Next()
		require.NoError(t, err)
		require.NotNil(t, nodeCommits)
		assert.Equal(t, expectedID, nodeCommits.NodeID)
	}

	// Should be done
	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	assert.Nil(t, nodeCommits)
}

func TestNWayMerger_TwoIteratorsWithOverlap_AddLinks(t *testing.T) {
	// Iterator 0 (older) - node 5 with some links
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 1},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{3}},
	}

	// Iterator 1 (newer) - node 5 with additional links
	commits1 := []Commit{
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{4, 5}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{6}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1}, logrus.New())
	require.NoError(t, err)

	// Should merge node 5
	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)
	assert.Equal(t, uint64(5), nodeCommits.NodeID)

	// Should have: AddNode + 2 AddLinksAtLevel (level 0 and 1, merged)
	assert.Equal(t, 3, len(nodeCommits.Commits))

	addNode, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)
	assert.Equal(t, uint64(5), addNode.ID)

	// Check level 0 links (should be merged: 4, 5, 1, 2)
	addLinks0, ok := nodeCommits.Commits[1].(*AddLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint16(0), addLinks0.Level)
	assert.Equal(t, []uint64{4, 5, 1, 2}, addLinks0.Targets)

	// Check level 1 links (should be merged: 6, 3)
	addLinks1, ok := nodeCommits.Commits[2].(*AddLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint16(1), addLinks1.Level)
	assert.Equal(t, []uint64{6, 3}, addLinks1.Targets)
}

func TestNWayMerger_ReplaceLinksStopsAccumulation(t *testing.T) {
	// Iterator 0 (oldest) - add links
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 1},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
	}

	// Iterator 1 (middle) - replace links at level 0
	commits1 := []Commit{
		&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{10, 11, 12}},
	}

	// Iterator 2 (newest) - add more links at level 0
	commits2 := []Commit{
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{20, 21}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)
	it2, err := NewIterator(newFakeCommitReader(commits2), 2, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1, it2}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Should have: AddNode + ReplaceLinks
	// The replace from it1 should include links from it2 (added after replace)
	// but NOT links from it0 (added before replace)
	assert.Equal(t, 2, len(nodeCommits.Commits))

	_, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)

	replaceLinks, ok := nodeCommits.Commits[1].(*ReplaceLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint16(0), replaceLinks.Level)
	// Should be: it2's adds (20, 21) + it1's replace (10, 11, 12)
	// NOT it0's adds (1, 2) because they were before the replace
	assert.Equal(t, []uint64{20, 21, 10, 11, 12}, replaceLinks.Targets)
}

func TestNWayMerger_DeleteNodeDropsAllData(t *testing.T) {
	// Iterator 0 (older) - node with data
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 2},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{3, 4}},
	}

	// Iterator 1 (newer) - delete the node
	commits1 := []Commit{
		&DeleteNodeCommit{ID: 5},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Should only have DeleteNode
	assert.Equal(t, 1, len(nodeCommits.Commits))
	_, ok := nodeCommits.Commits[0].(*DeleteNodeCommit)
	assert.True(t, ok)
}

func TestNWayMerger_TombstoneAddRemoveCancellation(t *testing.T) {
	// Iterator 0 - add tombstone
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 0},
		&AddTombstoneCommit{ID: 5},
	}

	// Iterator 1 - remove tombstone
	commits1 := []Commit{
		&RemoveTombstoneCommit{ID: 5},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Should only have AddNode (tombstone add + remove = no-op)
	assert.Equal(t, 1, len(nodeCommits.Commits))
	_, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)
}

func TestNWayMerger_TombstoneWithDeleteNode(t *testing.T) {
	// Iterator 0 - node with tombstone
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 0},
		&AddTombstoneCommit{ID: 5},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
	}

	// Iterator 1 - delete the node but keep tombstone
	commits1 := []Commit{
		&DeleteNodeCommit{ID: 5},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Should have: DeleteNode + AddTombstone (tombstone survives deletion)
	assert.Equal(t, 2, len(nodeCommits.Commits))
	_, ok := nodeCommits.Commits[0].(*DeleteNodeCommit)
	assert.True(t, ok)
	_, ok = nodeCommits.Commits[1].(*AddTombstoneCommit)
	assert.True(t, ok)
}

func TestNWayMerger_GlobalCommitsMerged(t *testing.T) {
	// Iterator 0 (oldest) - has PQ
	commits0 := []Commit{
		&AddPQCommit{Data: nil},
		&AddNodeCommit{ID: 1, Level: 0},
	}

	// Iterator 1 (middle) - has entrypoint
	commits1 := []Commit{
		&SetEntryPointMaxLevelCommit{Entrypoint: 100, Level: 3},
		&AddNodeCommit{ID: 2, Level: 0},
	}

	// Iterator 2 (newest) - has Muvera
	commits2 := []Commit{
		&AddMuveraCommit{Data: nil},
		&AddNodeCommit{ID: 3, Level: 0},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)
	it2, err := NewIterator(newFakeCommitReader(commits2), 2, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1, it2}, logrus.New())
	require.NoError(t, err)

	// Should have all global commits merged
	globalCommits := merger.GlobalCommits()
	assert.Equal(t, 3, len(globalCommits))

	_, ok := globalCommits[0].(*AddPQCommit)
	assert.True(t, ok)
	_, ok = globalCommits[1].(*AddMuveraCommit)
	assert.True(t, ok)
	entrypoint, ok := globalCommits[2].(*SetEntryPointMaxLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint64(100), entrypoint.Entrypoint)
}

func TestNWayMerger_GlobalCommitsPrecedence(t *testing.T) {
	// Iterator 0 (older) - entrypoint at 50
	commits0 := []Commit{
		&SetEntryPointMaxLevelCommit{Entrypoint: 50, Level: 2},
		&AddNodeCommit{ID: 1, Level: 0},
	}

	// Iterator 1 (newer) - entrypoint at 100 (should win)
	commits1 := []Commit{
		&SetEntryPointMaxLevelCommit{Entrypoint: 100, Level: 3},
		&AddNodeCommit{ID: 2, Level: 0},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1}, logrus.New())
	require.NoError(t, err)

	globalCommits := merger.GlobalCommits()
	assert.Equal(t, 1, len(globalCommits))

	entrypoint, ok := globalCommits[0].(*SetEntryPointMaxLevelCommit)
	assert.True(t, ok)
	// Should be from it1 (newer)
	assert.Equal(t, uint64(100), entrypoint.Entrypoint)
	assert.Equal(t, uint16(3), entrypoint.Level)
}

func TestNWayMerger_ThreeIteratorsComplexMerge(t *testing.T) {
	// Iterator 0 - nodes 1, 3, 5
	commits0 := []Commit{
		&AddNodeCommit{ID: 1, Level: 0},
		&AddLinksAtLevelCommit{Source: 1, Level: 0, Targets: []uint64{10}},
		&AddNodeCommit{ID: 3, Level: 0},
		&AddNodeCommit{ID: 5, Level: 0},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
	}

	// Iterator 1 - nodes 2, 3 (overlaps with it0 on node 3)
	commits1 := []Commit{
		&AddNodeCommit{ID: 2, Level: 0},
		&AddNodeCommit{ID: 3, Level: 1}, // Higher level than it0
		&AddLinksAtLevelCommit{Source: 3, Level: 0, Targets: []uint64{20}},
		&AddLinksAtLevelCommit{Source: 3, Level: 1, Targets: []uint64{21}},
	}

	// Iterator 2 - node 4, 5 (overlaps with it0 on node 5)
	commits2 := []Commit{
		&AddNodeCommit{ID: 4, Level: 0},
		&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{100, 101}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)
	it2, err := NewIterator(newFakeCommitReader(commits2), 2, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1, it2}, logrus.New())
	require.NoError(t, err)

	// Node 1 (only in it0)
	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nodeCommits.NodeID)

	// Node 2 (only in it1)
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), nodeCommits.NodeID)

	// Node 3 (in it0 and it1, should merge)
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), nodeCommits.NodeID)
	// Should have level 1 from it1 (higher precedence)
	addNode, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)
	assert.Equal(t, uint16(1), addNode.Level)

	// Node 4 (only in it2)
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), nodeCommits.NodeID)

	// Node 5 (in it0 and it2, should merge with replace taking precedence)
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), nodeCommits.NodeID)
	// Should have replace from it2 (which stops accumulation from it0)
	assert.Equal(t, 2, len(nodeCommits.Commits))
	replaceLinks, ok := nodeCommits.Commits[1].(*ReplaceLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, []uint64{100, 101}, replaceLinks.Targets)

	// Should be done
	nodeCommits, err = merger.Next()
	require.NoError(t, err)
	assert.Nil(t, nodeCommits)
}

func TestNWayMerger_ClearLinksAtLevel(t *testing.T) {
	// Iterator 0 - add links
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 1},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2, 3}},
	}

	// Iterator 1 - clear links at level 0
	commits1 := []Commit{
		&ClearLinksAtLevelCommit{ID: 5, Level: 0},
	}

	// Iterator 2 - add new links at level 0
	commits2 := []Commit{
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{10, 11}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)
	it2, err := NewIterator(newFakeCommitReader(commits2), 2, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]*Iterator{it0, it1, it2}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Should have: AddNode + ReplaceLinks (clear acts as replace)
	assert.Equal(t, 2, len(nodeCommits.Commits))

	replaceLinks, ok := nodeCommits.Commits[1].(*ReplaceLinksAtLevelCommit)
	assert.True(t, ok)
	// Should only have links from it2 (after clear), not from it0 (before clear)
	assert.Equal(t, []uint64{10, 11}, replaceLinks.Targets)
}
