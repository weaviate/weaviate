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

	merger, err := NewNWayMerger([]IteratorLike{it}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
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

	// Check level 0 links (should be merged: 1, 2, 4, 5 - older first, then newer)
	addLinks0, ok := nodeCommits.Commits[1].(*AddLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint16(0), addLinks0.Level)
	assert.Equal(t, []uint64{1, 2, 4, 5}, addLinks0.Targets)

	// Check level 1 links (should be merged: 3, 6 - older first, then newer)
	addLinks1, ok := nodeCommits.Commits[2].(*AddLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint16(1), addLinks1.Level)
	assert.Equal(t, []uint64{3, 6}, addLinks1.Targets)
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1, it2}, logrus.New())
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
	// Should be: it1's replace (10, 11, 12) + it2's adds (20, 21)
	// NOT it0's adds (1, 2) because they were before the replace
	assert.Equal(t, []uint64{10, 11, 12, 20, 21}, replaceLinks.Targets)
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1, it2}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1, it2}, logrus.New())
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

// ClearLinksCommit precedence in commitMerger.
//
// commitMerger processes iterators from highest to lowest precedence (newest
// first). ClearLinksAtLevelCommit is handled correctly — it guards with
// !m.seenReplaceAtLevel[level] so it never erases data already accumulated
// from newer iterators, and it sets the flag to stop older accumulation.
//
// ClearLinksCommit (all-level variant) has the same two responsibilities but
// implements neither guard:
//
//  1. It unconditionally resets linksPerLevel/seenReplaceAtLevel, wiping links
//     that were already set by newer (higher-precedence) iterators.
//
//  2. Because it resets seenReplaceAtLevel, older AddLinks commits processed
//     afterwards can accumulate links that should have been cleared.
//
// These tests pin the correct expected behaviour. They currently FAIL because
// the bug is present; once the bug is fixed they should pass without
// modification.

// TestCommitMerger_ClearLinks_OlderIterator_ShouldNotWipeNewerReplace checks
// that a ClearLinks from an older file does not erase a ReplaceLinksAtLevel
// that was written by a newer file.
//
// Timeline (oldest → newest):
//
//	it0: AddNode(5)  ClearLinks(5)
//	it1:                             ReplaceLinks(5, level=0, [10,11,12])
//
// The replace is more recent and must survive.
func TestCommitMerger_ClearLinks_OlderIterator_ShouldNotWipeNewerReplace(t *testing.T) {
	// it0 is older (ID=0); it1 is newer (ID=1).
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 0},
		&ClearLinksCommit{ID: 5},
	}
	commits1 := []Commit{
		&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{10, 11, 12}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)
	assert.Equal(t, uint64(5), nodeCommits.NodeID)

	// Expected: AddNode + ReplaceLinksAtLevel(0, [10,11,12])
	// Bug:      AddNode only — ClearLinks from it0 erases the newer replace.
	require.Len(t, nodeCommits.Commits, 2,
		"ClearLinks from an older iterator must not erase links set by a newer iterator")

	_, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok, "first commit should be AddNode")

	replace, ok := nodeCommits.Commits[1].(*ReplaceLinksAtLevelCommit)
	require.True(t, ok, "second commit should be ReplaceLinksAtLevel")
	assert.Equal(t, uint16(0), replace.Level)
	assert.Equal(t, []uint64{10, 11, 12}, replace.Targets,
		"links from the newer iterator must survive the older ClearLinks")
}

// TestCommitMerger_ClearLinks_OlderIterator_ShouldNotWipeAnyNewerLevel checks
// that ClearLinks from an older file does not erase links at any level set
// by a newer file, even when the older file also had per-level adds before
// the clear.
//
// Timeline (oldest → newest):
//
//	it0: AddNode(5)  AddLinks(5,0,[1,2])  AddLinks(5,1,[3,4])  ClearLinks(5)
//	it1:                                                         ReplaceLinks(5,0,[10,11])  AddLinks(5,1,[20])
func TestCommitMerger_ClearLinks_OlderIterator_ShouldNotWipeAnyNewerLevel(t *testing.T) {
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 1},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{3, 4}},
		&ClearLinksCommit{ID: 5},
	}
	commits1 := []Commit{
		&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{10, 11}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{20}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Expected: AddNode + ReplaceLinks(0,[10,11]) + AddLinks(1,[20])
	// The older AddLinks(0,[1,2]) and AddLinks(1,[3,4]) are superseded by the
	// ClearLinks within it0 itself, so they must NOT appear.
	// The ClearLinks must NOT erase the newer it1 contributions.
	// Bug: ClearLinks from it0 wipes everything → AddNode only.
	require.Len(t, nodeCommits.Commits, 3,
		"ClearLinks from an older iterator must not erase links at any level set by a newer iterator")

	_, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok, "first commit should be AddNode")

	replace, ok := nodeCommits.Commits[1].(*ReplaceLinksAtLevelCommit)
	require.True(t, ok, "second commit should be ReplaceLinksAtLevel for level 0")
	assert.Equal(t, uint16(0), replace.Level)
	assert.Equal(t, []uint64{10, 11}, replace.Targets)

	addLinks, ok := nodeCommits.Commits[2].(*AddLinksAtLevelCommit)
	require.True(t, ok, "third commit should be AddLinksAtLevel for level 1")
	assert.Equal(t, uint16(1), addLinks.Level)
	assert.Equal(t, []uint64{20}, addLinks.Targets,
		"older AddLinks(1,[3,4]) must be blocked by the ClearLinks within the same file")
}

// TestCommitMerger_ClearLinks_NewerIterator_ShouldBlockOlderAccumulation checks
// that a ClearLinks from a newer file prevents older AddLinks from
// accumulating into the merged result.
//
// Timeline (oldest → newest):
//
//	it0: AddNode(5)  AddLinks(5,0,[1,2])
//	it1:                                  ClearLinks(5)
//
// The clear is more recent and must erase the older adds.
func TestCommitMerger_ClearLinks_NewerIterator_ShouldBlockOlderAccumulation(t *testing.T) {
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 0},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
	}
	commits1 := []Commit{
		&ClearLinksCommit{ID: 5},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// Expected: AddNode only — the newer ClearLinks must erase the older AddLinks.
	// Bug: ClearLinks resets seenReplaceAtLevel, so the older AddLinks accumulates
	//      afterwards → node ends up with AddNode + AddLinks(0,[1,2]).
	require.Len(t, nodeCommits.Commits, 1,
		"newer ClearLinks must block older AddLinks from accumulating into the result")
	_, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok, "only commit should be AddNode; older links must be cleared")
}

// TestCommitMerger_ClearLinksAtLevel_Correct shows that the per-level variant
// already handles both responsibilities correctly and serves as a reference for
// how ClearLinksCommit should behave after the fix.
//
// This test is expected to PASS even before the bug fix.
func TestCommitMerger_ClearLinksAtLevel_Correct(t *testing.T) {
	// Same shape as the ClearLinks tests above, but per-level.
	commits0 := []Commit{
		&AddNodeCommit{ID: 5, Level: 0},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
		&ClearLinksAtLevelCommit{ID: 5, Level: 0},
	}
	commits1 := []Commit{
		&ReplaceLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{10, 11, 12}},
	}

	it0, err := NewIterator(newFakeCommitReader(commits0), 0, logrus.New())
	require.NoError(t, err)
	it1, err := NewIterator(newFakeCommitReader(commits1), 1, logrus.New())
	require.NoError(t, err)

	merger, err := NewNWayMerger([]IteratorLike{it0, it1}, logrus.New())
	require.NoError(t, err)

	nodeCommits, err := merger.Next()
	require.NoError(t, err)
	require.NotNil(t, nodeCommits)

	// ClearLinksAtLevel correctly guards with !seenReplaceAtLevel, so it does
	// NOT erase the newer replace. This is the behaviour ClearLinks should mirror.
	require.Len(t, nodeCommits.Commits, 2)
	_, ok := nodeCommits.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)
	replace, ok := nodeCommits.Commits[1].(*ReplaceLinksAtLevelCommit)
	require.True(t, ok)
	assert.Equal(t, []uint64{10, 11, 12}, replace.Targets)
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

	merger, err := NewNWayMerger([]IteratorLike{it0, it1, it2}, logrus.New())
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
