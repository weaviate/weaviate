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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCommitReader is a fake implementation that returns commits from a slice.
type fakeCommitReader struct {
	commits []Commit
	index   int
}

func newFakeCommitReader(commits []Commit) *fakeCommitReader {
	return &fakeCommitReader{commits: commits, index: 0}
}

func (f *fakeCommitReader) ReadNextCommit() (Commit, error) {
	if f.index >= len(f.commits) {
		return nil, io.EOF
	}
	commit := f.commits[f.index]
	f.index++
	return commit, nil
}

func TestIterator_EmptyReader(t *testing.T) {
	reader := newFakeCommitReader([]Commit{})
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	assert.True(t, it.Exhausted())
	assert.Nil(t, it.Current())
	assert.Equal(t, 0, len(it.GlobalCommits()))
}

func TestIterator_OnlyGlobalCommits(t *testing.T) {
	commits := []Commit{
		&SetEntryPointMaxLevelCommit{Entrypoint: 100, Level: 3},
		&AddPQCommit{Data: nil}, // Data is not relevant for this test
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	// Should be exhausted (no node commits)
	assert.True(t, it.Exhausted())
	assert.Nil(t, it.Current())

	// Should have captured global commits
	globalCommits := it.GlobalCommits()
	assert.Equal(t, 2, len(globalCommits))

	entrypoint, ok := globalCommits[0].(*SetEntryPointMaxLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint64(100), entrypoint.Entrypoint)
	assert.Equal(t, uint16(3), entrypoint.Level)

	_, ok = globalCommits[1].(*AddPQCommit)
	assert.True(t, ok)
}

func TestIterator_SingleNodeWithOneCommit(t *testing.T) {
	commits := []Commit{
		&AddNodeCommit{ID: 5, Level: 2},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	// Should not be exhausted initially
	assert.False(t, it.Exhausted())

	// Current should return node 5
	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(5), current.NodeID)
	assert.Equal(t, 1, len(current.Commits))

	addNode, ok := current.Commits[0].(*AddNodeCommit)
	assert.True(t, ok)
	assert.Equal(t, uint64(5), addNode.ID)
	assert.Equal(t, uint16(2), addNode.Level)

	// Advance should exhaust
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
	assert.True(t, it.Exhausted())
	assert.Nil(t, it.Current())
}

func TestIterator_SingleNodeWithMultipleCommits(t *testing.T) {
	commits := []Commit{
		&AddNodeCommit{ID: 5, Level: 2},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2, 3}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{4, 5}},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	assert.False(t, it.Exhausted())

	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(5), current.NodeID)
	assert.Equal(t, 3, len(current.Commits))

	// Verify all commits are for the same node
	for i, commit := range current.Commits {
		switch c := commit.(type) {
		case *AddNodeCommit:
			assert.Equal(t, uint64(5), c.ID, "commit %d", i)
		case *AddLinksAtLevelCommit:
			assert.Equal(t, uint64(5), c.Source, "commit %d", i)
		default:
			t.Fatalf("unexpected commit type: %T", c)
		}
	}
}

func TestIterator_MultipleNodesGroupedCorrectly(t *testing.T) {
	commits := []Commit{
		// Node 1
		&AddNodeCommit{ID: 1, Level: 0},
		&AddLinksAtLevelCommit{Source: 1, Level: 0, Targets: []uint64{10}},
		// Node 5
		&AddNodeCommit{ID: 5, Level: 2},
		&AddLinksAtLevelCommit{Source: 5, Level: 0, Targets: []uint64{1, 2}},
		&AddLinksAtLevelCommit{Source: 5, Level: 1, Targets: []uint64{3}},
		// Node 10
		&AddTombstoneCommit{ID: 10},
		&DeleteNodeCommit{ID: 10},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	// Node 1
	assert.False(t, it.Exhausted())
	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(1), current.NodeID)
	assert.Equal(t, 2, len(current.Commits))

	// Advance to node 5
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	current = it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(5), current.NodeID)
	assert.Equal(t, 3, len(current.Commits))

	// Advance to node 10
	hasNext, err = it.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	current = it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(10), current.NodeID)
	assert.Equal(t, 2, len(current.Commits))

	// Advance past end
	hasNext, err = it.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
	assert.True(t, it.Exhausted())
}

func TestIterator_GlobalCommitsFollowedByNodes(t *testing.T) {
	commits := []Commit{
		// Global commits
		&SetEntryPointMaxLevelCommit{Entrypoint: 42, Level: 5},
		&AddSQCommit{Data: nil},
		// Node commits
		&AddNodeCommit{ID: 10, Level: 3},
		&AddLinksAtLevelCommit{Source: 10, Level: 0, Targets: []uint64{5, 6}},
		&AddNodeCommit{ID: 20, Level: 1},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	// Check global commits
	globalCommits := it.GlobalCommits()
	assert.Equal(t, 2, len(globalCommits))

	// Check node 10
	assert.False(t, it.Exhausted())
	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(10), current.NodeID)
	assert.Equal(t, 2, len(current.Commits))

	// Check node 20
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	current = it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(20), current.NodeID)
	assert.Equal(t, 1, len(current.Commits))
}

func TestIterator_IteratorID(t *testing.T) {
	commits := []Commit{
		&AddNodeCommit{ID: 1, Level: 0},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 42, logrus.New())
	require.NoError(t, err)

	assert.Equal(t, 42, it.ID())
}

func TestIterator_AllCommitTypesForSingleNode(t *testing.T) {
	commits := []Commit{
		&AddTombstoneCommit{ID: 7},
		&AddNodeCommit{ID: 7, Level: 2},
		&AddLinksAtLevelCommit{Source: 7, Level: 0, Targets: []uint64{1, 2}},
		&ReplaceLinksAtLevelCommit{Source: 7, Level: 1, Targets: []uint64{3, 4, 5}},
		&AddLinkAtLevelCommit{Source: 7, Level: 2, Target: 6},
		&ClearLinksAtLevelCommit{ID: 7, Level: 3},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(7), current.NodeID)
	assert.Equal(t, 6, len(current.Commits))

	// Verify commit types in order
	_, ok := current.Commits[0].(*AddTombstoneCommit)
	assert.True(t, ok)

	_, ok = current.Commits[1].(*AddNodeCommit)
	assert.True(t, ok)

	_, ok = current.Commits[2].(*AddLinksAtLevelCommit)
	assert.True(t, ok)

	replace, ok := current.Commits[3].(*ReplaceLinksAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, []uint64{3, 4, 5}, replace.Targets)

	addLink, ok := current.Commits[4].(*AddLinkAtLevelCommit)
	assert.True(t, ok)
	assert.Equal(t, uint64(6), addLink.Target)

	_, ok = current.Commits[5].(*ClearLinksAtLevelCommit)
	assert.True(t, ok)
}

func TestIterator_TransitionBetweenNodesCorrectly(t *testing.T) {
	// Test various node ID transitions to ensure proper grouping
	commits := []Commit{
		// Node 1 - single commit
		&AddNodeCommit{ID: 1, Level: 0},
		// Node 2 - three commits
		&AddNodeCommit{ID: 2, Level: 1},
		&AddLinksAtLevelCommit{Source: 2, Level: 0, Targets: []uint64{1}},
		&AddLinksAtLevelCommit{Source: 2, Level: 1, Targets: []uint64{3}},
		// Node 3 - single commit
		&AddTombstoneCommit{ID: 3},
		// Node 4 - two commits
		&AddNodeCommit{ID: 4, Level: 0},
		&DeleteNodeCommit{ID: 4},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	expectedNodes := []struct {
		id          uint64
		commitCount int
	}{
		{1, 1},
		{2, 3},
		{3, 1},
		{4, 2},
	}

	for i, expected := range expectedNodes {
		assert.False(t, it.Exhausted(), "should not be exhausted at node %d", i)
		current := it.Current()
		require.NotNil(t, current, "current should not be nil at position %d", i)
		assert.Equal(t, expected.id, current.NodeID, "node ID mismatch at position %d", i)
		assert.Equal(t, expected.commitCount, len(current.Commits), "commit count mismatch for node %d", expected.id)

		if i < len(expectedNodes)-1 {
			hasNext, err := it.Next()
			require.NoError(t, err)
			assert.True(t, hasNext, "should have next at position %d", i)
		}
	}

	// Verify exhausted at end
	hasNext, err := it.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
	assert.True(t, it.Exhausted())
}

func TestIterator_RemoveTombstoneCommit(t *testing.T) {
	commits := []Commit{
		&AddTombstoneCommit{ID: 5},
		&RemoveTombstoneCommit{ID: 5},
		&AddNodeCommit{ID: 5, Level: 1},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(5), current.NodeID)
	assert.Equal(t, 3, len(current.Commits))

	_, ok := current.Commits[0].(*AddTombstoneCommit)
	assert.True(t, ok)

	_, ok = current.Commits[1].(*RemoveTombstoneCommit)
	assert.True(t, ok)

	_, ok = current.Commits[2].(*AddNodeCommit)
	assert.True(t, ok)
}

func TestIterator_ClearLinksCommit(t *testing.T) {
	commits := []Commit{
		&AddLinksAtLevelCommit{Source: 10, Level: 0, Targets: []uint64{1, 2, 3}},
		&ClearLinksCommit{ID: 10},
		&AddLinksAtLevelCommit{Source: 10, Level: 0, Targets: []uint64{4, 5}},
	}

	reader := newFakeCommitReader(commits)
	it, err := NewIterator(reader, 0, logrus.New())
	require.NoError(t, err)

	current := it.Current()
	require.NotNil(t, current)
	assert.Equal(t, uint64(10), current.NodeID)
	assert.Equal(t, 3, len(current.Commits))
}
