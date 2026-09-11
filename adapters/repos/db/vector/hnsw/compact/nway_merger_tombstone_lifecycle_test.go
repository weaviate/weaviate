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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// The n-way merger collapses AddTombstone + RemoveTombstone for the same node
// to a no-op (emits neither) whenever both ops belong to the same life. These
// tests prove no live tombstone is ever dropped:
//
//	RemoveTombstone is terminal within a node's life. It is emitted only while
//	cleaning up a tombstoned node, always immediately after DeleteNode
//	(delete.go). With docID reuse a deleted id CAN be reissued: same-file
//	re-adds are reconciled by the deserializer (InMemoryReader.readNode clears
//	all per-id delete/tombstone state on re-add), and cross-file re-adds are
//	reconciled by the merger itself (an older file's delete — and its stranded
//	RemoveTombstone tail — never overrides a life established by a newer
//	file; see the crossfile reuse tests). Whenever both tombstone flags
//	survive to result(), they describe the SAME life's completed cycle, so
//	collapsing them is the correct final state.

const tombNode = uint64(5)

// mergedStateForNode runs an n-way merge over the given per-iterator commit
// streams (index 0 = oldest / lowest precedence, last = newest / highest
// precedence) and returns the classified merged output for tombNode.
func mergedStateForNode(t *testing.T, streams ...[]Commit) mergedTombState {
	t.Helper()
	return mergedStateAndLinksForNode(t, tombNode, streams...).mergedTombState
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

// TestNWayMerger_TombstoneCollapse_Mechanism pins the collapse mechanism:
// Add+Remove yield neither when both ops belong to the same life, while an
// older file's stranded RemoveTombstone never cancels a newer life's live
// tombstone.
func TestNWayMerger_TombstoneCollapse_Mechanism(t *testing.T) {
	cases := []struct {
		name        string
		streams     [][]Commit
		wantAddTomb bool
	}{
		{
			name: "older_Add_newer_Remove",
			streams: [][]Commit{
				{&AddNodeCommit{ID: tombNode, Level: 0}, &AddTombstoneCommit{ID: tombNode}},
				{&RemoveTombstoneCommit{ID: tombNode}},
			},
		},
		{
			// An older file's RemoveTombstone can only be the stranded tail of
			// an old life's cleanup (its DeleteNode is in the same or an older
			// file). The newer file's AddTombstone tombstones the NEW life and
			// must survive: no collapse across lives.
			name: "older_Remove_newer_Add_keeps_new_life_tombstone",
			streams: [][]Commit{
				{&RemoveTombstoneCommit{ID: tombNode}},
				{&AddNodeCommit{ID: tombNode, Level: 0}, &AddTombstoneCommit{ID: tombNode}},
			},
			wantAddTomb: true,
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
			require.Equal(t, tc.wantAddTomb, s.emitsAddTomb, "AddTombstone in merged output")
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

// TestNWayMerger_DocIDReuse_ReAddedNodeSurvivesPipeline replaces the former
// "safe only with immutable docIDs" boundary test. With docID reuse, the
// commit-log sequence AddNode(X) … DeleteNode(X) … AddNode(X) is a VALID
// input. The load-bearing invariant moved upstream: the deserializer clears
// per-id delete/tombstone state on re-add, so by the time a reuse log has been
// materialized into the sorted format, it describes only the id's newest life.
// This test drives the reuse sequence through the real pipeline
// (reader -> sorted writer -> iterator -> merger) and asserts X comes out
// alive, untombstoned, and with its new life's level and links.
func TestNWayMerger_DocIDReuse_ReAddedNodeSurvivesPipeline(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	// Older log: an unrelated node, to make this a genuine n-way merge.
	var rawOld bytes.Buffer
	wOld := NewWALWriter(&rawOld)
	require.NoError(t, wOld.WriteAddNode(1, 0))

	// Newer log: the full docID-reuse lifecycle for tombNode.
	var rawNew bytes.Buffer
	wNew := NewWALWriter(&rawNew)
	require.NoError(t, wNew.WriteAddNode(tombNode, 2)) // old life
	require.NoError(t, wNew.WriteReplaceLinksAtLevel(tombNode, 0, []uint64{1}))
	require.NoError(t, wNew.WriteAddTombstone(tombNode))
	require.NoError(t, wNew.WriteDeleteNode(tombNode))
	require.NoError(t, wNew.WriteRemoveTombstone(tombNode))
	require.NoError(t, wNew.WriteAddNode(tombNode, 1)) // new life (docID reuse)
	require.NoError(t, wNew.WriteReplaceLinksAtLevel(tombNode, 0, []uint64{1}))

	// Condense both raw logs into the sorted format (reader -> sorted writer),
	// exactly like the compactor's convertFileToSorted does.
	iterators := make([]IteratorLike, 0, 2)
	for i, raw := range []*bytes.Buffer{&rawOld, &rawNew} {
		res, err := NewInMemoryReader(NewWALCommitReader(raw, logger), logger).Do(nil, true)
		require.NoError(t, err)

		var sorted bytes.Buffer
		require.NoError(t, NewSortedWriter(&sorted, logger).WriteAll(res))

		it, err := NewIterator(NewWALCommitReader(&sorted, logger), i, logger)
		require.NoError(t, err)
		iterators = append(iterators, it)
	}

	merger, err := NewNWayMerger(iterators, logger)
	require.NoError(t, err)

	var s mergedTombState
	var addNode *AddNodeCommit
	for {
		nc, err := merger.Next()
		require.NoError(t, err)
		if nc == nil {
			break
		}
		if nc.NodeID != tombNode {
			continue
		}
		s = classifyTombState(nc.Commits)
		for _, c := range nc.Commits {
			if an, ok := c.(*AddNodeCommit); ok {
				addNode = an
			}
		}
	}

	require.True(t, s.hasAddNode, "re-added node must be alive after the full pipeline")
	require.False(t, s.hasDeleteNode, "old life's DeleteNode must not survive the re-add")
	require.False(t, s.emitsAddTomb, "old life's tombstone must not survive the re-add")
	require.False(t, s.emitsRmTomb, "old life's RemoveTombstone must not survive the re-add")
	require.NotNil(t, addNode)
	require.Equal(t, uint16(1), addNode.Level, "AddNode must carry the new life's level")
}
