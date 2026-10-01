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

// End-to-end correctness tests for tombstone cleanup through Loader.Load.
//
// Coverage is intentionally complementary to:
//   - nway_merger_tombstone_lifecycle_test.go (merger-level collapse semantics
//     during compaction).
//   - compact_integration_test.go (compactor cycles end-to-end).
//   - loader_test.go (single-WAL "tombstone present" smoke test only).
//
// These tests pin the Loader / InMemoryReader contract: the post-startup
// state's Tombstones, TombstonesDeleted, NodesDeleted and Nodes maps reflect
// the correct lifecycle position of every node across snapshot + WAL files.
// The matrix below covers every realizable transition since docIDs are
// immutable (a Removed id is never re-added).

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// loadDir runs Loader.Load on a temp dir and returns the resulting graph
// state. Fails the test on any error.
func loadDir(t *testing.T, dir string) *ent.DeserializationResult {
	t.Helper()
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: loaderTestLogger()})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result, "loader returned nil result")
	require.NotNil(t, result.State, "loader returned nil state")
	return result.State
}

func hasTombstone(state *ent.DeserializationResult, id uint64) bool {
	_, ok := state.Graph.Tombstones[id]
	return ok
}

func hasTombstoneDeleted(state *ent.DeserializationResult, id uint64) bool {
	_, ok := state.Graph.TombstonesDeleted[id]
	return ok
}

func hasNodeDeleted(state *ent.DeserializationResult, id uint64) bool {
	_, ok := state.Graph.NodesDeleted[id]
	return ok
}

func nodePresent(state *ent.DeserializationResult, id uint64) bool {
	if uint64(len(state.Graph.Nodes)) <= id {
		return false
	}
	return state.Graph.Nodes[id] != nil
}

// =============================================================================
// AddTombstone / RemoveTombstone correctness across WAL ordering.
// =============================================================================

// TestLoader_TombstoneAddedAndRemoved_SameWAL: AddTombstone followed by
// RemoveTombstone inside one WAL file must leave Tombstones empty.
func TestLoader_TombstoneAddedAndRemoved_SameWAL(t *testing.T) {
	dir := t.TempDir()

	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(7, 0))
		require.NoError(t, w.WriteAddTombstone(7))
		require.NoError(t, w.WriteRemoveTombstone(7))
	})

	st := loadDir(t, dir)
	assert.False(t, hasTombstone(st, 7), "tombstone for 7 should be cleared")
	assert.True(t, nodePresent(st, 7), "node 7 should still be present (RemoveTombstone alone is not DeleteNode)")
}

// TestLoader_TombstoneAddedThenRemoved_AcrossWALs: a tombstone added in WAL
// 1000 and removed in WAL 1001 must result in no tombstone — loader honors
// timestamp order.
func TestLoader_TombstoneAddedThenRemoved_AcrossWALs(t *testing.T) {
	dir := t.TempDir()

	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(11, 0))
		require.NoError(t, w.WriteAddTombstone(11))
	})
	createTestWALFile(t, filepath.Join(dir, "1001"), func(w *WALWriter) {
		require.NoError(t, w.WriteRemoveTombstone(11))
	})

	st := loadDir(t, dir)
	assert.False(t, hasTombstone(st, 11), "tombstone for 11 should be cleared across WAL boundary")
}

// TestLoader_RemoveTombstoneWithoutPriorAdd_BecomesPendingDeletion: a stray
// RemoveTombstone (no in-scope AddTombstone) lands in TombstonesDeleted so a
// future merge can drop an older tombstone that lives in a yet-unread file.
// This is the documented "tombstone may exist in older commit log" path
// (in_memory_reader.go:114-116).
func TestLoader_RemoveTombstoneWithoutPriorAdd_BecomesPendingDeletion(t *testing.T) {
	dir := t.TempDir()

	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(13, 0))
		require.NoError(t, w.WriteRemoveTombstone(13))
	})

	st := loadDir(t, dir)
	assert.False(t, hasTombstone(st, 13))
	assert.True(t, hasTombstoneDeleted(st, 13),
		"RemoveTombstone with no in-scope Add should be recorded as TombstonesDeleted")
}

// =============================================================================
// Snapshot interaction.
// =============================================================================

// TestLoader_TombstoneInSnapshot_RemovedByLaterWAL: the snapshot encodes a
// tombstone; a later WAL removes it. The post-Load Tombstones map must NOT
// contain the id.
func TestLoader_TombstoneInSnapshot_RemovedByLaterWAL(t *testing.T) {
	dir := t.TempDir()

	createTestSnapshot(t, filepath.Join(dir, "1000.snapshot"), 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 5, level: 0, connections: [][]uint64{{}}, tombstone: true},
	})
	createTestWALFile(t, filepath.Join(dir, "1001"), func(w *WALWriter) {
		require.NoError(t, w.WriteRemoveTombstone(5))
	})

	st := loadDir(t, dir)
	assert.False(t, hasTombstone(st, 5), "snapshot tombstone should be cleared by later RemoveTombstone")
	assert.True(t, nodePresent(st, 5), "node 5 should remain (RemoveTombstone doesn't delete the node)")
}

// TestLoader_TombstoneInSnapshot_SurvivesNoOpWAL: a snapshot encodes a
// tombstone; the trailing WAL touches other nodes. The tombstone must
// survive into the post-Load state.
func TestLoader_TombstoneInSnapshot_SurvivesNoOpWAL(t *testing.T) {
	dir := t.TempDir()

	createTestSnapshot(t, filepath.Join(dir, "1000.snapshot"), 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 9, level: 0, connections: [][]uint64{{}}, tombstone: true},
	})
	createTestWALFile(t, filepath.Join(dir, "1001"), func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(20, 0))
	})

	st := loadDir(t, dir)
	assert.True(t, hasTombstone(st, 9), "snapshot tombstone should survive unrelated WAL")
}

// =============================================================================
// Full lifecycle (Add -> Tomb -> Delete -> RemoveTomb).
// =============================================================================

// TestLoader_FullLifecycle_DeleteAndRemoveTombstone: after the complete
// lifecycle the node is gone, the tombstone is gone, and NodesDeleted
// records the delete.
func TestLoader_FullLifecycle_DeleteAndRemoveTombstone(t *testing.T) {
	dir := t.TempDir()

	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(3, 0))
		require.NoError(t, w.WriteAddTombstone(3))
		require.NoError(t, w.WriteDeleteNode(3))
		require.NoError(t, w.WriteRemoveTombstone(3))
	})

	st := loadDir(t, dir)
	assert.False(t, hasTombstone(st, 3), "tombstone cleared after lifecycle")
	assert.True(t, hasNodeDeleted(st, 3), "NodesDeleted should record id 3")
	assert.False(t, nodePresent(st, 3), "Nodes[3] should be nil after DeleteNode")
}

// TestLoader_FullLifecycle_SplitAcrossWALs: same as above but each lifecycle
// step lives in its own WAL file. Tests that load-time ordering of WAL
// timestamps drives correct end state.
func TestLoader_FullLifecycle_SplitAcrossWALs(t *testing.T) {
	dir := t.TempDir()

	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(8, 0))
	})
	createTestWALFile(t, filepath.Join(dir, "1001"), func(w *WALWriter) {
		require.NoError(t, w.WriteAddTombstone(8))
	})
	createTestWALFile(t, filepath.Join(dir, "1002"), func(w *WALWriter) {
		require.NoError(t, w.WriteDeleteNode(8))
	})
	createTestWALFile(t, filepath.Join(dir, "1003"), func(w *WALWriter) {
		require.NoError(t, w.WriteRemoveTombstone(8))
	})

	st := loadDir(t, dir)
	assert.False(t, hasTombstone(st, 8))
	assert.True(t, hasNodeDeleted(st, 8))
	assert.False(t, nodePresent(st, 8))
}

// TestLoader_DeleteWithoutRemoveTombstone_LeavesNodeDeletedAndTombstoneAlive:
// a partially-completed cleanup (DeleteNode but no RemoveTombstone yet) must
// leave the node deleted AND the tombstone alive — so a future compaction
// cycle can finish the work. Losing the tombstone here would skip the final
// graph link cleanup.
func TestLoader_DeleteWithoutRemoveTombstone_LeavesNodeDeletedAndTombstoneAlive(t *testing.T) {
	dir := t.TempDir()

	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(4, 0))
		require.NoError(t, w.WriteAddTombstone(4))
		require.NoError(t, w.WriteDeleteNode(4))
	})

	st := loadDir(t, dir)
	assert.True(t, hasTombstone(st, 4),
		"tombstone must remain until RemoveTombstone — needed for future link cleanup")
	assert.True(t, hasNodeDeleted(st, 4))
	assert.False(t, nodePresent(st, 4))
}

// =============================================================================
// High-churn sanity: many tombstone events, end-state correctness.
// =============================================================================

// TestLoader_AlternatingTombstoneChurn_EndStateCorrect: heavy alternation of
// AddTombstone/RemoveTombstone across many WAL files. Whatever the final
// state should be per node, the Loader must compute it correctly.
func TestLoader_AlternatingTombstoneChurn_EndStateCorrect(t *testing.T) {
	dir := t.TempDir()

	// Set up: 10 nodes, ids 100..109. We will tombstone/untombstone in a
	// pattern such that we know the final state per id.
	const baseID = 100
	const numNodes = 10
	createTestWALFile(t, filepath.Join(dir, "1000"), func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(baseID, 0))
		for i := uint64(0); i < numNodes; i++ {
			require.NoError(t, w.WriteAddNode(baseID+i, 0))
		}
	})

	// WAL 1001: tombstone all 10 nodes.
	createTestWALFile(t, filepath.Join(dir, "1001"), func(w *WALWriter) {
		for i := uint64(0); i < numNodes; i++ {
			require.NoError(t, w.WriteAddTombstone(baseID+i))
		}
	})

	// WAL 1002: untombstone the even-indexed nodes only.
	createTestWALFile(t, filepath.Join(dir, "1002"), func(w *WALWriter) {
		for i := uint64(0); i < numNodes; i += 2 {
			require.NoError(t, w.WriteRemoveTombstone(baseID+i))
		}
	})

	st := loadDir(t, dir)
	for i := uint64(0); i < numNodes; i++ {
		id := uint64(baseID + i)
		want := (i%2 == 1) // odd-indexed nodes stay tombstoned
		assert.Equalf(t, want, hasTombstone(st, id),
			"node %d tombstone state wrong (want=%v)", id, want)
	}
}

// TestLoader_TombstoneCleanup_IdempotentReload: writing the same WAL twice
// to the same load (two timestamps, same content) must not double-count
// tombstones — Tombstones is a set, not a multiset.
func TestLoader_TombstoneCleanup_IdempotentReload(t *testing.T) {
	dir := t.TempDir()

	for _, ts := range []string{"1000", "1001"} {
		path := filepath.Join(dir, ts)
		createTestWALFile(t, path, func(w *WALWriter) {
			require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
			require.NoError(t, w.WriteAddNode(0, 0))
			require.NoError(t, w.WriteAddNode(2, 0))
			require.NoError(t, w.WriteAddTombstone(2))
		})
	}

	st := loadDir(t, dir)
	assert.True(t, hasTombstone(st, 2))
	assert.Lenf(t, st.Graph.Tombstones, 1, "Tombstones map should contain exactly one entry, got %v", st.Graph.Tombstones)
}

// =============================================================================
// Table-driven matrix of lifecycle inputs vs expected end state.
// =============================================================================

type tombstoneCase struct {
	name string
	// walEvents[w] is the sequence of events to write into WAL file with
	// timestamp 1000+w. Each event is a closure on the WALWriter.
	walEvents [][]func(w *WALWriter) error

	wantTombstone        bool
	wantTombstoneDeleted bool
	wantNodeDeleted      bool
	wantNodePresent      bool
}

func TestLoader_TombstoneCleanup_Matrix(t *testing.T) {
	const id = uint64(42)

	add := func(w *WALWriter) error { return w.WriteAddNode(id, 0) }
	tomb := func(w *WALWriter) error { return w.WriteAddTombstone(id) }
	rmtomb := func(w *WALWriter) error { return w.WriteRemoveTombstone(id) }
	del := func(w *WALWriter) error { return w.WriteDeleteNode(id) }
	setEP := func(w *WALWriter) error { return w.WriteSetEntryPointMaxLevel(0, 0) }
	addEP := func(w *WALWriter) error { return w.WriteAddNode(0, 0) }

	cases := []tombstoneCase{
		{
			name: "add_only",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add},
			},
			wantNodePresent: true,
		},
		{
			name: "add_then_tomb",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, tomb},
			},
			wantTombstone:   true,
			wantNodePresent: true,
		},
		{
			name: "add_tomb_rmtomb_single_wal",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, tomb, rmtomb},
			},
			wantNodePresent: true,
		},
		{
			name: "add_tomb_then_rmtomb_split",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, tomb},
				{rmtomb},
			},
			wantNodePresent: true,
		},
		{
			name: "add_tomb_del_no_rmtomb",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, tomb, del},
			},
			wantTombstone:   true,
			wantNodeDeleted: true,
		},
		{
			name: "add_tomb_del_rmtomb_full_lifecycle",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, tomb, del, rmtomb},
			},
			wantNodeDeleted: true,
		},
		{
			name: "stray_rmtomb_only",
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, rmtomb},
			},
			wantTombstoneDeleted: true,
			wantNodePresent:      true,
		},
		{
			name: "rmtomb_then_tomb_immutable_id_violation_documents_behavior",
			// Per nway_merger_tombstone_lifecycle_test.go this input is
			// non-realizable: docIDs are immutable, an id reissued after a
			// RemoveTombstone shouldn't happen. The Loader still has to
			// pick one answer though — record what it does so a regression
			// is caught.
			walEvents: [][]func(w *WALWriter) error{
				{setEP, addEP, add, rmtomb},
				{tomb},
			},
			wantTombstone:        true,
			wantTombstoneDeleted: true,
			wantNodePresent:      true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for w, events := range tc.walEvents {
				path := filepath.Join(dir, fmt.Sprintf("%d", 1000+w))
				createTestWALFile(t, path, func(ww *WALWriter) {
					for _, ev := range events {
						require.NoError(t, ev(ww))
					}
				})
			}

			st := loadDir(t, dir)
			assert.Equal(t, tc.wantTombstone, hasTombstone(st, id), "Tombstones")
			assert.Equal(t, tc.wantTombstoneDeleted, hasTombstoneDeleted(st, id), "TombstonesDeleted")
			assert.Equal(t, tc.wantNodeDeleted, hasNodeDeleted(st, id), "NodesDeleted")
			assert.Equal(t, tc.wantNodePresent, nodePresent(st, id), "Nodes[id] presence")
		})
	}
}
