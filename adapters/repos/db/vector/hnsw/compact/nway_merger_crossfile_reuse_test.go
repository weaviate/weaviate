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

// Cross-file docID reuse: a node's delete and its re-add can land in
// DIFFERENT sorted files (with the queue's drain gate, delete-in-an-earlier-
// file is the COMMON case, not the corner). InMemoryReader.readNode only
// reconciles a re-add against a delete recorded in the same materialization
// pass, so for the compaction path the merger itself must be file-order
// aware: a DeleteNodeCommit from an older file ends an OLDER life and must
// not override the newer life already merged from newer files.
//
// These tests drive the merger with per-file canonical commit sets exactly
// as the sorted conversion produces them (delete files carry
// [DeleteNode, RemoveTombstone]; live files carry
// [AddTombstone?, AddNode, ReplaceLinks...]).

// mergedNodeState captures everything the merged output says about one node.
type mergedNodeState struct {
	mergedTombState
	addNode *AddNodeCommit
	links   map[uint16][]uint64
}

// mergedStateAndLinksForNode is mergedStateForNode plus AddNode/link capture.
// streams: index 0 = oldest file, last = newest file.
func mergedStateAndLinksForNode(t *testing.T, nodeID uint64, streams ...[]Commit) mergedNodeState {
	t.Helper()
	iterators := make([]IteratorLike, 0, len(streams))
	for i, s := range streams {
		it, err := NewIterator(newFakeCommitReader(s), i, logrus.New())
		require.NoError(t, err)
		iterators = append(iterators, it)
	}
	merger, err := NewNWayMerger(iterators, logrus.New())
	require.NoError(t, err)

	out := mergedNodeState{links: map[uint16][]uint64{}}
	for {
		nc, err := merger.Next()
		require.NoError(t, err)
		if nc == nil {
			break
		}
		if nc.NodeID != nodeID {
			continue
		}
		out.mergedTombState = classifyTombState(nc.Commits)
		for _, c := range nc.Commits {
			switch ct := c.(type) {
			case *AddNodeCommit:
				out.addNode = ct
			case *ReplaceLinksAtLevelCommit:
				out.links[ct.Level] = ct.Targets
			case *AddLinksAtLevelCommit:
				out.links[ct.Level] = append(out.links[ct.Level], ct.Targets...)
			}
		}
	}
	return out
}

// TestNWayMerger_CrossFileReuse_DeleteInOlderFile is the gate-drain common
// case: the old life's cleanup ([DeleteNode, RemoveTombstone]) was rotated
// into sorted file N, the reused id's new life lives in sorted file N+1.
// Sequential replay of N then N+1 ends with the new life alive; the merged
// output must agree.
func TestNWayMerger_CrossFileReuse_DeleteInOlderFile(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		// file N (older): old life's cleanup
		[]Commit{&DeleteNodeCommit{ID: id}, &RemoveTombstoneCommit{ID: id}},
		// file N+1 (newer): new life
		[]Commit{
			&AddNodeCommit{ID: id, Level: 1},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{1, 2}},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 1, Targets: []uint64{1}},
		},
	)

	require.False(t, s.hasDeleteNode, "old life's DeleteNode must not kill the newer life")
	require.NotNil(t, s.addNode, "re-added node must be alive in the merged output")
	require.Equal(t, uint16(1), s.addNode.Level)
	require.False(t, s.emitsRmTomb, "old life's RemoveTombstone must not attach to the new life")
	require.False(t, s.emitsAddTomb)
	require.Equal(t, []uint64{1, 2}, s.links[0])
	require.Equal(t, []uint64{1}, s.links[1])
}

// TestNWayMerger_CrossFileReuse_ThreeFileSplit splits the full lifecycle over
// three sorted files: the old (higher-level) life in the oldest, its cleanup
// in the middle, the new life in the newest. Besides survival, the old life's
// links at levels the new life does not have must NOT leak into the merge.
func TestNWayMerger_CrossFileReuse_ThreeFileSplit(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		// oldest: old life, tombstoned, level 2 with links on all levels
		[]Commit{
			&AddTombstoneCommit{ID: id},
			&AddNodeCommit{ID: id, Level: 2},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{3}},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 1, Targets: []uint64{3}},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 2, Targets: []uint64{3}},
		},
		// middle: cleanup of the old life
		[]Commit{&DeleteNodeCommit{ID: id}, &RemoveTombstoneCommit{ID: id}},
		// newest: new life at level 0 only
		[]Commit{
			&AddNodeCommit{ID: id, Level: 0},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{1}},
		},
	)

	require.False(t, s.hasDeleteNode)
	require.NotNil(t, s.addNode)
	require.Equal(t, uint16(0), s.addNode.Level, "AddNode must carry the NEW life's level")
	require.False(t, s.emitsAddTomb, "old life's tombstone must not survive onto the new life")
	require.False(t, s.emitsRmTomb)
	require.Equal(t, []uint64{1}, s.links[0], "level 0 links must be the new life's")
	require.Empty(t, s.links[1], "old life's level 1 links must not leak into the new life")
	require.Empty(t, s.links[2], "old life's level 2 links must not leak into the new life")
}

// TestNWayMerger_CrossFileReuse_OldTombstonePendingAcrossFiles covers the
// re-add arriving while the OLD life's tombstone had been pending across a
// file boundary before its cleanup: [AddNode, AddTombstone] | [DeleteNode,
// RemoveTombstone] | [AddNode new]. The new life must come out alive and
// untombstoned.
func TestNWayMerger_CrossFileReuse_OldTombstonePendingAcrossFiles(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		[]Commit{
			&AddTombstoneCommit{ID: id},
			&AddNodeCommit{ID: id, Level: 1},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{9}},
		},
		[]Commit{&DeleteNodeCommit{ID: id}, &RemoveTombstoneCommit{ID: id}},
		[]Commit{
			&AddNodeCommit{ID: id, Level: 0},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{1}},
		},
	)

	require.False(t, s.hasDeleteNode)
	require.NotNil(t, s.addNode)
	require.Equal(t, uint16(0), s.addNode.Level)
	require.False(t, s.emitsAddTomb, "the OLD life's pending tombstone must not tombstone the new life")
	require.False(t, s.emitsRmTomb)
	require.Equal(t, []uint64{1}, s.links[0])
}

// TestNWayMerger_CrossFileReuse_NewLifeTombstoneSurvivesStrayRemove pins the
// rotation-split hazard: cleanup writes DeleteNode and RemoveTombstone as two
// appends, so a rotation can strand the RemoveTombstone in the NEXT file.
// When the reused id's new life is later tombstoned again, the stray old-life
// RemoveTombstone from the middle file must NOT cancel the new life's live
// tombstone — a cancelled live tombstone is never cleaned up and the node
// resurrects on restart.
func TestNWayMerger_CrossFileReuse_NewLifeTombstoneSurvivesStrayRemove(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		// oldest: old life + its DeleteNode (RemoveTombstone rotated away)
		[]Commit{&AddTombstoneCommit{ID: id}, &DeleteNodeCommit{ID: id}},
		// middle: the stranded RemoveTombstone of the OLD life
		[]Commit{&RemoveTombstoneCommit{ID: id}},
		// newest: new life, tombstoned again and awaiting cleanup
		[]Commit{&AddTombstoneCommit{ID: id}, &AddNodeCommit{ID: id, Level: 0}},
	)

	require.False(t, s.hasDeleteNode)
	require.NotNil(t, s.addNode, "re-added node must be alive")
	require.True(t, s.emitsAddTomb,
		"the new life's live tombstone must survive the old life's stray RemoveTombstone")
	require.False(t, s.emitsRmTomb)
}

// TestNWayMerger_CrossFileReuse_DeadIdStaysDead guards the other direction:
// when the newest state for the id IS the delete (no re-add yet), the merged
// output must still collapse to the delete exactly as before.
func TestNWayMerger_CrossFileReuse_DeadIdStaysDead(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		// oldest: the life
		[]Commit{
			&AddNodeCommit{ID: id, Level: 1},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{3}},
		},
		// middle: tombstoned
		[]Commit{&AddTombstoneCommit{ID: id}},
		// newest: cleaned up
		[]Commit{&DeleteNodeCommit{ID: id}, &RemoveTombstoneCommit{ID: id}},
	)

	require.True(t, s.hasDeleteNode, "dead id must stay dead")
	require.Nil(t, s.addNode)
	require.False(t, s.emitsAddTomb)
	require.Empty(t, s.links[0])
}

// TestNWayMerger_CrossFileReuse_SecondLifeAlsoDeleted covers two full
// lifecycles across files with the SECOND delete being the newest state:
// life1 | cleanup1 | life2 | cleanup2 — the id is dead at the end.
func TestNWayMerger_CrossFileReuse_SecondLifeAlsoDeleted(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		[]Commit{&AddNodeCommit{ID: id, Level: 0}},
		[]Commit{&DeleteNodeCommit{ID: id}, &RemoveTombstoneCommit{ID: id}},
		[]Commit{&AddNodeCommit{ID: id, Level: 1}},
		[]Commit{&DeleteNodeCommit{ID: id}, &RemoveTombstoneCommit{ID: id}},
	)

	require.True(t, s.hasDeleteNode, "second delete is the newest state; the id is dead")
	require.Nil(t, s.addNode)
}

// TestNWayMerger_CrossFileReuse_TombstoneOnOldNodeFromNeighborCleanup guards a
// same-life cross-file interleaving that must keep working: a node created in
// an old file, tombstoned in a middle file, whose links were rewritten in a
// NEWER file by a neighbor's tombstone cleanup. The tombstone is still live
// (no delete anywhere) and must survive the merge.
func TestNWayMerger_CrossFileReuse_TombstoneOnOldNodeFromNeighborCleanup(t *testing.T) {
	const id = uint64(7)

	s := mergedStateAndLinksForNode(t, id,
		[]Commit{
			&AddNodeCommit{ID: id, Level: 0},
			&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{3, 4}},
		},
		[]Commit{&AddTombstoneCommit{ID: id}},
		// newest: neighbor 4 was cleaned up; id's links got rewritten
		[]Commit{&ReplaceLinksAtLevelCommit{Source: id, Level: 0, Targets: []uint64{3, 8}}},
	)

	require.False(t, s.hasDeleteNode)
	require.NotNil(t, s.addNode)
	require.True(t, s.emitsAddTomb, "pending tombstone on a live node must survive")
	require.Equal(t, []uint64{3, 8}, s.links[0], "newest link rewrite wins")
}
