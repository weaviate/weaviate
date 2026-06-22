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

package lsmkv

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEditOps(t *testing.T) *SegmentEditOps {
	t.Helper()
	s, err := OpenSegmentEditOps(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func set(ids ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func removeOp(targets ...string) OpDescriptor {
	return OpDescriptor{Type: "remove_target_vectors", Targets: targets, CreatedAt: 100}
}

func TestSegmentEditOps_RegisterOpIdempotent(t *testing.T) {
	s := newTestEditOps(t)

	require.NoError(t, s.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", Targets: []string{"foo"}, CreatedAt: 100}))
	// Re-register with a different descriptor: the original must be kept.
	require.NoError(t, s.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", Targets: []string{"bar"}, CreatedAt: 999}))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "op1", ops[0].ID)
	assert.Equal(t, []string{"foo"}, ops[0].Descriptor.Targets)
	assert.EqualValues(t, 100, ops[0].Descriptor.CreatedAt)
}

func TestSegmentEditOps_LoadOpsSortedByCreatedAt(t *testing.T) {
	s := newTestEditOps(t)

	require.NoError(t, s.RegisterOp("b", OpDescriptor{Type: "t", CreatedAt: 200}))
	require.NoError(t, s.RegisterOp("a", OpDescriptor{Type: "t", CreatedAt: 100}))
	require.NoError(t, s.RegisterOp("c", OpDescriptor{Type: "t", CreatedAt: 100}))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 3)
	// Sorted by CreatedAt, ties broken by ID: (a,100), (c,100), (b,200).
	assert.Equal(t, []string{"a", "c", "b"}, []string{ops[0].ID, ops[1].ID, ops[2].ID})
}

func TestSegmentEditOps_SnapshotSegmentsIdempotent(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))

	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("boom")))
	// Re-snapshotting must not duplicate or reset accrued retry state.
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2", "seg3"}))

	pending, err := s.AllPending()
	require.NoError(t, err)
	require.Len(t, pending, 3)
	byID := map[string]PendingSegment{}
	for _, p := range pending {
		byID[p.SegmentID] = p
	}
	assert.Equal(t, 1, byID["seg1"].Attempts)
	assert.Equal(t, "boom", byID["seg1"].LastError)
	assert.Equal(t, 0, byID["seg2"].Attempts)
	assert.Equal(t, 0, byID["seg3"].Attempts)
}

func TestSegmentEditOps_PendingScopedPerOp(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.RegisterOp("op2", removeOp("bar")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.SnapshotSegments("op2", []string{"seg3"}))

	p1, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg1", "seg2"}, p1)

	p2, err := s.Pending("op2")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg3"}, p2)

	// Unknown op returns nothing, not an error.
	pNone, err := s.Pending("missing")
	require.NoError(t, err)
	assert.Empty(t, pNone)
}

func TestSegmentEditOps_MarkSegmentDone(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))

	require.NoError(t, s.MarkSegmentDone("op1", "seg1"))

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg2"}, p)

	// Idempotent: marking an already-done (or unknown) segment is a no-op.
	require.NoError(t, s.MarkSegmentDone("op1", "seg1"))
	require.NoError(t, s.MarkSegmentDone("missing", "seg1"))
}

func TestSegmentEditOps_BumpAttemptThenQuarantine(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1"}))

	for range 3 {
		require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("disk full")))
	}

	pending, err := s.AllPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, 3, pending[0].Attempts)
	assert.Equal(t, "disk full", pending[0].LastError)

	require.NoError(t, s.Quarantine("op1", "seg1"))

	// Moved out of pending, into quarantined, retry metadata preserved.
	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.Empty(t, p)

	q, err := s.Quarantined()
	require.NoError(t, err)
	require.Len(t, q, 1)
	assert.Equal(t, "seg1", q[0].SegmentID)
	assert.Equal(t, 3, q[0].Attempts)
}

func TestSegmentEditOps_DeleteOp(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.RegisterOp("op2", removeOp("bar")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1"}))
	require.NoError(t, s.SnapshotSegments("op2", []string{"seg2"}))
	require.NoError(t, s.Quarantine("op1", "seg1")) // op1 has a quarantined row too... but it was the only pending
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1"}))

	require.NoError(t, s.DeleteOp("op1"))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "op2", ops[0].ID)

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.Empty(t, p)
	q, err := s.Quarantined()
	require.NoError(t, err)
	for _, qs := range q {
		assert.NotEqual(t, "op1", qs.OpID)
	}

	// op2 untouched.
	p2, err := s.Pending("op2")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg2"}, p2)
}

func TestSegmentEditOps_ReconcileDeletesStaleSegmentRows(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2", "seg3"}))
	require.NoError(t, s.Quarantine("op1", "seg3"))

	// seg2 (pending) and seg3 (quarantined) no longer exist on disk.
	require.NoError(t, s.Reconcile(set("seg1"), nil))

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg1"}, p)

	q, err := s.Quarantined()
	require.NoError(t, err)
	assert.Empty(t, q)

	// The operation itself survives (liveOpIDs nil = skip orphan sweep).
	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
}

func TestSegmentEditOps_ReconcileDeletesOrphanedOps(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("live", removeOp("foo")))
	require.NoError(t, s.RegisterOp("orphan", removeOp("bar")))
	require.NoError(t, s.SnapshotSegments("live", []string{"seg1"}))
	require.NoError(t, s.SnapshotSegments("orphan", []string{"seg1"}))

	require.NoError(t, s.Reconcile(set("seg1"), set("live")))

	ops, err := s.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "live", ops[0].ID)

	// orphan's rows are gone; live's remain.
	all, err := s.AllPending()
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, "live", all[0].OpID)
}

func TestSegmentEditOps_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenSegmentEditOps(dir)
	require.NoError(t, err)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("boom")))
	require.NoError(t, s.Close())

	reopened, err := OpenSegmentEditOps(dir)
	require.NoError(t, err)
	defer reopened.Close()

	ops, err := reopened.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)

	all, err := reopened.AllPending()
	require.NoError(t, err)
	require.Len(t, all, 2)
	for _, p := range all {
		if p.SegmentID == "seg1" {
			assert.Equal(t, 1, p.Attempts)
			assert.Equal(t, "boom", p.LastError)
		}
	}
}

func TestSegmentEditOps_BumpAttemptDoesNotResurrectDoneSegment(t *testing.T) {
	s := newTestEditOps(t)
	require.NoError(t, s.RegisterOp("op1", removeOp("foo")))
	require.NoError(t, s.SnapshotSegments("op1", []string{"seg1", "seg2"}))
	require.NoError(t, s.MarkSegmentDone("op1", "seg1"))

	// A late/duplicate error for an already-completed segment must not revive it.
	require.NoError(t, s.BumpAttempt("op1", "seg1", errors.New("late error")))

	p, err := s.Pending("op1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"seg2"}, p)
}

func TestSegmentEditOps_SnapshotSegmentsRequiresRegisteredOp(t *testing.T) {
	s := newTestEditOps(t)

	err := s.SnapshotSegments("unregistered", []string{"seg1"})
	require.Error(t, err)
	require.ErrorContains(t, err, "not registered")

	// No orphan pending rows were created.
	p, err := s.Pending("unregistered")
	require.NoError(t, err)
	assert.Empty(t, p)
}
