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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// The n-way merger collapses AddTombstone + RemoveTombstone for the same node to a
// no-op (emits neither), regardless of precedence or in-file order. These tests
// prove that is correct for every reachable input:
//
//	RemoveTombstone is terminal. It is emitted only while cleaning up a tombstoned
//	node, always immediately after DeleteNode (delete.go), and docIDs are immutable
//	(a deleted id is never reissued). So once RemoveTombstone appears for an id,
//	nothing newer can exist for it and the node is gone. Whenever both tombstone
//	flags are set, DeleteNode is present too, so result() emits [DeleteNode] — the
//	correct final state. No live tombstone is ever dropped.

const tombNode = uint64(5)

// mergedStateForNode runs an n-way merge over the given per-iterator commit
// streams (index 0 = oldest / lowest precedence, last = newest / highest
// precedence) and returns the classified merged output for tombNode.
func mergedStateForNode(t *testing.T, streams ...[]Commit) mergedTombState {
	t.Helper()
	iterators := make([]IteratorLike, 0, len(streams))
	for i, s := range streams {
		it, err := NewIterator(newFakeCommitReader(s), i, logrus.New())
		require.NoError(t, err)
		iterators = append(iterators, it)
	}
	merger, err := NewNWayMerger(iterators, logrus.New())
	require.NoError(t, err)

	var out mergedTombState
	for {
		nc, err := merger.Next()
		require.NoError(t, err)
		if nc == nil {
			break
		}
		if nc.NodeID != tombNode {
			continue
		}
		out = classifyTombState(nc.Commits)
	}
	return out
}

type mergedTombState struct {
	hasAddNode    bool
	hasDeleteNode bool
	emitsAddTomb  bool // merged output contains an AddTombstoneCommit
	emitsRmTomb   bool // merged output contains a RemoveTombstoneCommit
}

func classifyTombState(commits []Commit) mergedTombState {
	var s mergedTombState
	for _, c := range commits {
		switch c.(type) {
		case *AddNodeCommit:
			s.hasAddNode = true
		case *DeleteNodeCommit:
			s.hasDeleteNode = true
		case *AddTombstoneCommit:
			s.emitsAddTomb = true
		case *RemoveTombstoneCommit:
			s.emitsRmTomb = true
		}
	}
	return s
}

// TestNWayMerger_TombstoneCollapse_Mechanism pins the literal collapse: Add+Remove
// for the same node always yield neither, independent of order and precedence.
func TestNWayMerger_TombstoneCollapse_Mechanism(t *testing.T) {
	cases := []struct {
		name    string
		streams [][]Commit
	}{
		{
			name: "older_Add_newer_Remove",
			streams: [][]Commit{
				{&AddNodeCommit{ID: tombNode, Level: 0}, &AddTombstoneCommit{ID: tombNode}},
				{&RemoveTombstoneCommit{ID: tombNode}},
			},
		},
		{
			name: "older_Remove_newer_Add",
			streams: [][]Commit{
				{&RemoveTombstoneCommit{ID: tombNode}},
				{&AddNodeCommit{ID: tombNode, Level: 0}, &AddTombstoneCommit{ID: tombNode}},
			},
		},
		{
			name: "same_file_Remove_then_Add",
			streams: [][]Commit{
				{&AddNodeCommit{ID: tombNode, Level: 0}, &RemoveTombstoneCommit{ID: tombNode}, &AddTombstoneCommit{ID: tombNode}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mergedStateForNode(t, tc.streams...)
			require.False(t, s.emitsAddTomb, "merger unexpectedly emitted AddTombstone")
			require.False(t, s.emitsRmTomb, "merger unexpectedly emitted RemoveTombstone")
		})
	}
}

// TestNWayMerger_TombstoneLifecycle_NoDataLoss replays the full tombstone lifecycle
// (AddNode -> AddTombstone -> DeleteNode -> RemoveTombstone) split across files in
// every realizable way and asserts the merged output is always correct.
func TestNWayMerger_TombstoneLifecycle_NoDataLoss(t *testing.T) {
	add := func() Commit { return &AddNodeCommit{ID: tombNode, Level: 0} }
	addT := func() Commit { return &AddTombstoneCommit{ID: tombNode} }
	del := func() Commit { return &DeleteNodeCommit{ID: tombNode} }
	rmT := func() Commit { return &RemoveTombstoneCommit{ID: tombNode} }

	cases := []struct {
		name          string
		streams       [][]Commit
		wantDeleted   bool // node should be deleted (DeleteNode present)
		wantTombstone bool // node should remain tombstoned (AddTombstone present, not deleted)
	}{
		{
			name:          "added_then_tombstoned_not_yet_cleaned",
			streams:       [][]Commit{{add(), addT()}},
			wantTombstone: true,
		},
		{
			name:        "full_lifecycle_single_file",
			streams:     [][]Commit{{add(), addT(), del(), rmT()}},
			wantDeleted: true,
		},
		{
			name:        "split_add+tomb__then__del+rm",
			streams:     [][]Commit{{add(), addT()}, {del(), rmT()}},
			wantDeleted: true,
		},
		{
			name:        "split_add+tomb+del__then__rm_rotation",
			streams:     [][]Commit{{add(), addT(), del()}, {rmT()}},
			wantDeleted: true,
		},
		{
			name:        "split_add__then__tomb+del+rm",
			streams:     [][]Commit{{add()}, {addT(), del(), rmT()}},
			wantDeleted: true,
		},
		{
			// DeleteNode/AddTombstone already absorbed elsewhere; only a stray
			// RemoveTombstone reaches this merge. Harmless: no live tombstone to lose.
			name:    "stray_remove_only",
			streams: [][]Commit{{rmT()}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mergedStateForNode(t, tc.streams...)
			if tc.wantDeleted {
				require.True(t, s.hasDeleteNode, "expected DeleteNode in merged output")
				require.False(t, s.emitsAddTomb, "deleted node must not be re-tombstoned")
			}
			if tc.wantTombstone {
				require.True(t, s.emitsAddTomb,
					"node should remain tombstoned but merged output dropped the tombstone")
				require.False(t, s.hasDeleteNode)
			}
		})
	}
}

// TestNWayMerger_TombstoneCollapse_SafeOnlyWithImmutableDocIDs documents the
// load-bearing invariant. The collapse would drop a live tombstone only if a
// RemoveTombstone'd id could be re-added with a newer AddTombstone (docID reuse).
// Immutable docIDs make that input unreachable; this pins the boundary so a future
// change that breaks the invariant is caught here.
func TestNWayMerger_TombstoneCollapse_SafeOnlyWithImmutableDocIDs(t *testing.T) {
	// Non-realizable input: same id re-added/re-deleted after a RemoveTombstone.
	s := mergedStateForNode(t,
		[]Commit{&RemoveTombstoneCommit{ID: tombNode}},
		[]Commit{&AddNodeCommit{ID: tombNode, Level: 0}, &AddTombstoneCommit{ID: tombNode}},
	)
	require.True(t, s.hasAddNode)
	require.False(t, s.emitsAddTomb)
}
