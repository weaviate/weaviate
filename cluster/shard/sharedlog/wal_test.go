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

package sharedlog

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// TestWAL_RotationAndRebuildAcrossSegments drives three interleaved groups
// across many small segments — including a leader-style tail overwrite and a
// snapshot+compaction — then reopens and verifies the index rebuild
// reproduces every group's state exactly.
func TestWAL_RotationAndRebuildAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pad := strings.Repeat("p", 100)

	s := newWALTestStore(t, dir, 1024)
	for r := uint64(1); r <= 8; r++ {
		for g := uint64(1); g <= 3; g++ {
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID:   g,
				Entries:   []raftpb.Entry{mkEntry(r, 1, fmt.Sprintf("g%d-r%d-%s", g, r, pad))},
				HardState: &raftpb.HardState{Term: 1, Commit: r},
			}))
		}
	}
	// Group 2: a new leader overwrites the tail from index 5.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   2,
		Entries:   []raftpb.Entry{mkEntry(5, 2, "new5"), mkEntry(6, 2, "new6")},
		HardState: &raftpb.HardState{Term: 2, Vote: 3, Commit: 6},
	}))
	// Group 3: snapshot at 5, then compact behind it.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 3,
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snap3"),
			Metadata: raftpb.SnapshotMetadata{
				Index:     5,
				Term:      1,
				ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
			},
		},
		HardState: &raftpb.HardState{Term: 1, Commit: 8},
	}))
	require.NoError(t, s.Compact(3, 6))
	require.NoError(t, s.Close())

	require.GreaterOrEqual(t, len(walSegmentPaths(t, dir)), 3, "test needs multiple segments")

	s2 := newWALTestStore(t, dir, 1024)

	g1 := s2.Storage(1)
	fi, err := g1.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), fi)
	li, err := g1.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(8), li)
	ents, err := g1.Entries(1, 9, 1<<20)
	require.NoError(t, err)
	require.Len(t, ents, 8)
	for i, e := range ents {
		assert.Equal(t, fmt.Sprintf("g1-r%d-%s", i+1, pad), string(e.Data))
	}

	g2 := s2.Storage(2)
	li, err = g2.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(6), li, "the tail overwrite must survive the rebuild")
	ents, err = g2.Entries(4, 7, 1<<20)
	require.NoError(t, err)
	require.Len(t, ents, 3)
	assert.Equal(t, fmt.Sprintf("g2-r4-%s", pad), string(ents[0].Data))
	assert.Equal(t, []byte("new5"), ents[1].Data)
	assert.Equal(t, []byte("new6"), ents[2].Data)
	term, err := g2.Term(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), term)
	term, err = g2.Term(4)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), term)
	hs, _, err := g2.InitialState()
	require.NoError(t, err)
	assert.Equal(t, raftpb.HardState{Term: 2, Vote: 3, Commit: 6}, hs)

	g3 := s2.Storage(3)
	fi, err = g3.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(6), fi)
	li, err = g3.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(8), li)
	_, err = g3.Entries(1, 5, 1<<20)
	assert.ErrorIs(t, err, raft.ErrCompacted)
	term, err = g3.Term(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), term, "Term(snapshot.Index) comes from the snapshot metadata")
	_, cs, err := g3.InitialState()
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, cs.Voters, "ConfState falls back to the snapshot after rebuild")

	for g := uint64(1); g <= 3; g++ {
		has, err := s2.HasGroup(g)
		require.NoError(t, err)
		assert.True(t, has)
	}
}

// TestWAL_OversizedRecord: a batch larger than the segment cap is written
// whole into its own segment rather than split or rejected.
func TestWAL_OversizedRecord(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newWALTestStore(t, dir, 512)
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "small")},
		HardState: &raftpb.HardState{Term: 1, Commit: 1},
	}))
	big := strings.Repeat("B", 4096)
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(2, 1, big)},
		HardState: &raftpb.HardState{Term: 1, Commit: 2},
	}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(3, 1, "after")},
		HardState: &raftpb.HardState{Term: 1, Commit: 3},
	}))
	require.NoError(t, s.Close())

	s2 := newWALTestStore(t, dir, 512)
	ents, err := s2.Storage(1).Entries(1, 4, 1<<20)
	require.NoError(t, err)
	require.Len(t, ents, 3)
	assert.Equal(t, []byte("small"), ents[0].Data)
	assert.Equal(t, []byte(big), ents[1].Data)
	assert.Equal(t, []byte("after"), ents[2].Data)
}

// TestWAL_IndependentCompactionAndReclamation: groups compact on their own
// schedules; a segment is reclaimed only once EVERY group's records in it
// are dead, and reclamation strictly pops from the oldest end.
func TestWAL_IndependentCompactionAndReclamation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pad := strings.Repeat("q", 150)

	s := newWALTestStore(t, dir, 1024)
	for r := uint64(1); r <= 10; r++ {
		for g := uint64(1); g <= 2; g++ {
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID:   g,
				Entries:   []raftpb.Entry{mkEntry(r, 1, fmt.Sprintf("g%d-%d-%s", g, r, pad))},
				HardState: &raftpb.HardState{Term: 1, Commit: r},
			}))
		}
	}
	before := len(walSegmentPaths(t, dir))
	require.GreaterOrEqual(t, before, 4, "test needs multiple segments")

	snapAndCompact := func(g uint64) {
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID: g,
			Snapshot: &raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}},
			},
			HardState: &raftpb.HardState{Term: 1, Commit: 10},
		}))
		require.NoError(t, s.Compact(g, 11))
	}

	// Only group 1 compacts: the old segments still carry group 2's live
	// entries, so nothing is reclaimable.
	snapAndCompact(1)
	assert.GreaterOrEqual(t, len(walSegmentPaths(t, dir)), before,
		"segments must survive while another group's records in them are live")

	// Group 2 compacts too: the shared segments are now fully dead.
	snapAndCompact(2)
	after := len(walSegmentPaths(t, dir))
	assert.Less(t, after, before, "fully-dead segments must be reclaimed")

	for g := uint64(1); g <= 2; g++ {
		fi, err := s.Storage(g).FirstIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(11), fi)
		li, err := s.Storage(g).LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(10), li)
	}
	require.NoError(t, s.Close())

	// The reclaimed state must survive replay.
	s2 := newWALTestStore(t, dir, 1024)
	for g := uint64(1); g <= 2; g++ {
		has, err := s2.HasGroup(g)
		require.NoError(t, err)
		assert.True(t, has)
		fi, err := s2.Storage(g).FirstIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(11), fi)
	}
}

// fillUntilRewrite appends filler writes for the given group until the
// oldest segment file changes (the rewrite relocated and removed it), with a
// hard cap so a broken rewrite fails the test rather than spinning.
func fillUntilRewrite(t *testing.T, s *Store, dir string, fillerGroup uint64, startIdx uint64) uint64 {
	t.Helper()
	ctx := context.Background()
	oldest := walSegmentPaths(t, dir)[0]
	pad := strings.Repeat("f", 200)
	i := startIdx
	for n := 0; n < 500; n++ {
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID:   fillerGroup,
			Entries:   []raftpb.Entry{mkEntry(i, 1, pad)},
			HardState: &raftpb.HardState{Term: 1, Commit: i},
		}))
		i++
		// The filler group must keep compacting, or its own live entries
		// would legitimately hold every segment at live>0 forever.
		if i%20 == 0 {
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID: fillerGroup,
				Snapshot: &raftpb.Snapshot{
					Metadata: raftpb.SnapshotMetadata{Index: i - 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}},
				},
				HardState: &raftpb.HardState{Term: 1, Commit: i - 1},
			}))
			require.NoError(t, s.Compact(fillerGroup, i))
		}
		if walSegmentPaths(t, dir)[0] != oldest {
			return i
		}
	}
	t.Fatalf("rewrite never fired: oldest segment still %s", oldest)
	return i
}

// TestWAL_IdleGroupRewrite pins the reclamation liveness mechanism: an idle
// group's few live records (entries + HardState + ConfState) pin their
// segment until the rewrite policy copies them forward, after which the
// segment is deleted and the idle group's state survives — including across
// a restart, which replays the copy records.
func TestWAL_IdleGroupRewrite(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newWALTestStore(t, dir, 512)
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(5, 1, "a5"), mkEntry(6, 1, "a6"), mkEntry(7, 1, "a7")},
		HardState: &raftpb.HardState{Term: 1, Vote: 2, Commit: 7},
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}},
	}))

	// Group 1 now goes idle; group 2 pushes the log past the rewrite
	// threshold until group 1's residue is relocated.
	next := fillUntilRewrite(t, s, dir, 2, 1)

	verify := func(s *Store) {
		t.Helper()
		g := s.Storage(1)
		li, err := g.LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(7), li)
		ents, err := g.Entries(5, 8, 1<<20)
		require.NoError(t, err)
		require.Len(t, ents, 3)
		assert.Equal(t, []byte("a5"), ents[0].Data)
		assert.Equal(t, []byte("a6"), ents[1].Data)
		assert.Equal(t, []byte("a7"), ents[2].Data)
		hs, cs, err := g.InitialState()
		require.NoError(t, err)
		assert.Equal(t, raftpb.HardState{Term: 1, Vote: 2, Commit: 7}, hs)
		assert.Equal(t, []uint64{1, 2}, cs.Voters)
	}
	verify(s)
	require.NoError(t, s.Close())

	// The relocated records must survive replay (copy records applied).
	s2 := newWALTestStore(t, dir, 512)
	verify(s2)

	// The idle group must remain fully writable after relocation: extend,
	// then a leader-style truncate-overwrite, across another restart.
	require.NoError(t, s2.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(8, 3, "a8")},
		HardState: &raftpb.HardState{Term: 3, Commit: 8},
	}))
	require.NoError(t, s2.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(6, 4, "later6")},
		HardState: &raftpb.HardState{Term: 4, Commit: 6},
	}))
	require.NoError(t, s2.Close())

	s3 := newWALTestStore(t, dir, 512)
	g := s3.Storage(1)
	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(6), li)
	ents, err := g.Entries(5, 7, 1<<20)
	require.NoError(t, err)
	require.Len(t, ents, 2)
	assert.Equal(t, []byte("a5"), ents[0].Data)
	assert.Equal(t, []byte("later6"), ents[1].Data)
	_ = next
}

// TestWAL_RewriteNeverResurrectsSupersededEntries pins the hard requirement
// on the rewrite: it copies only currently-LIVE entries, filtered through
// the per-entry index — never whole records. The victim segment here holds a
// record whose tail ([6..7] term 1) was superseded by a truncate-overwrite
// ([6..8] term 2) living in a newer segment. A whole-record copy would
// replay AFTER the overwrite and resurrect the term-1 entries over it; the
// index-built copy carries the live range's current values (old5 plus the
// term-2 overwrites), never the superseded ones.
func TestWAL_RewriteNeverResurrectsSupersededEntries(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newWALTestStore(t, dir, 512)
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(5, 1, "old5"), mkEntry(6, 1, "old6"), mkEntry(7, 1, "old7")},
		HardState: &raftpb.HardState{Term: 1, Commit: 5},
	}))

	// Push the overwrite into a later segment than the original record.
	pad := strings.Repeat("z", 200)
	for i := uint64(1); len(walSegmentPaths(t, dir)) < 2; i++ {
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID:   2,
			Entries:   []raftpb.Entry{mkEntry(i, 1, pad)},
			HardState: &raftpb.HardState{Term: 1, Commit: i},
		}))
	}
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(6, 2, "new6"), mkEntry(7, 2, "new7"), mkEntry(8, 2, "new8")},
		HardState: &raftpb.HardState{Term: 2, Commit: 8},
	}))

	// Group 2 (compacting as it goes) drives the log until the rewrite
	// relocates group 1's residue out of the oldest segment.
	fillUntilRewrite(t, s, dir, 2, 100)

	verify := func(s *Store) {
		t.Helper()
		g := s.Storage(1)
		li, err := g.LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(8), li, "the copy must never truncate the newer overwrite")
		ents, err := g.Entries(5, 9, 1<<20)
		require.NoError(t, err)
		require.Len(t, ents, 4)
		assert.Equal(t, []byte("old5"), ents[0].Data)
		assert.Equal(t, []byte("new6"), ents[1].Data, "superseded old6 must not be resurrected")
		assert.Equal(t, []byte("new7"), ents[2].Data, "superseded old7 must not be resurrected")
		assert.Equal(t, []byte("new8"), ents[3].Data)
		for i, wantTerm := range []uint64{1, 2, 2, 2} {
			term, err := g.Term(uint64(5 + i))
			require.NoError(t, err)
			assert.Equal(t, wantTerm, term)
		}
	}
	verify(s)
	require.NoError(t, s.Close())

	// The decisive half: replay applies the copy record after the overwrite
	// it must not clobber.
	s2 := newWALTestStore(t, dir, 512)
	verify(s2)
}

// TestWAL_RewriteCascadeAcrossSegments pins repeated reclamation over a
// range spanning several old segments: the first rewrite relocates the
// whole live range (including the portion in the second segment), the
// second oldest segment then drains to zero live bytes and reclaims, and
// the range stays intact throughout — live before, after, and across a
// restart that replays only the copies.
func TestWAL_RewriteCascadeAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newWALTestStore(t, dir, 512)
	// The idle group's range spans the first TWO segments.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "a1"), mkEntry(2, 1, "a2")},
		HardState: &raftpb.HardState{Term: 1, Commit: 2},
	}))
	pad := strings.Repeat("y", 200)
	for i := uint64(1); len(walSegmentPaths(t, dir)) < 2; i++ {
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID:   2,
			Entries:   []raftpb.Entry{mkEntry(i, 1, pad)},
			HardState: &raftpb.HardState{Term: 1, Commit: i},
		}))
	}
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(3, 1, "a3"), mkEntry(4, 1, "a4")},
		HardState: &raftpb.HardState{Term: 1, Commit: 4},
	}))

	// First rewrite relocates [1..2]; the cascade's next victims hold the
	// non-prefix remainder [3..4].
	next := fillUntilRewrite(t, s, dir, 2, 100)
	fillUntilRewrite(t, s, dir, 2, next)

	verify := func(s *Store) {
		t.Helper()
		ents, err := s.Storage(1).Entries(1, 5, 1<<20)
		require.NoError(t, err)
		require.Len(t, ents, 4)
		for i, want := range []string{"a1", "a2", "a3", "a4"} {
			assert.Equal(t, []byte(want), ents[i].Data)
		}
	}
	verify(s)
	require.NoError(t, s.Close())

	s2 := newWALTestStore(t, dir, 512)
	verify(s2)
}

// TestWAL_HardStateReplayOrdering: the latest HardState must win the
// rebuild no matter how many superseded ones precede it across segments.
func TestWAL_HardStateReplayOrdering(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pad := strings.Repeat("h", 120)

	s := newWALTestStore(t, dir, 512)
	for r := uint64(1); r <= 10; r++ {
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID:   4,
			Entries:   []raftpb.Entry{mkEntry(r, r, pad)},
			HardState: &raftpb.HardState{Term: r, Vote: r, Commit: r},
		}))
	}
	require.NoError(t, s.Close())
	require.GreaterOrEqual(t, len(walSegmentPaths(t, dir)), 3, "test needs multiple segments")

	s2 := newWALTestStore(t, dir, 512)
	hs, _, err := s2.Storage(4).InitialState()
	require.NoError(t, err)
	assert.Equal(t, raftpb.HardState{Term: 10, Vote: 10, Commit: 10}, hs)
}

// TestWAL_DeleteGroupSharedSegments: the tombstone is durable (the group
// stays gone across a restart), co-tenant groups in the same segments are
// untouched, and the dead group's records stop pinning segments — once the
// co-tenant compacts, the shared segments reclaim.
func TestWAL_DeleteGroupSharedSegments(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pad := strings.Repeat("d", 150)

	s := newWALTestStore(t, dir, 1024)
	for r := uint64(1); r <= 10; r++ {
		for g := uint64(1); g <= 2; g++ {
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID:   g,
				Entries:   []raftpb.Entry{mkEntry(r, 1, fmt.Sprintf("g%d-%d-%s", g, r, pad))},
				HardState: &raftpb.HardState{Term: 1, Commit: r},
			}))
		}
	}
	before := len(walSegmentPaths(t, dir))
	require.GreaterOrEqual(t, before, 4, "test needs multiple segments")

	require.NoError(t, s.DeleteGroup(1))
	require.NoError(t, s.DeleteGroup(1), "DeleteGroup must be idempotent")

	// Group 2 still pins the shared segments.
	assert.Equal(t, before, len(walSegmentPaths(t, dir)))

	require.NoError(t, s.Close())

	s2 := newWALTestStore(t, dir, 1024)
	has, err := s2.HasGroup(1)
	require.NoError(t, err)
	assert.False(t, has, "the tombstone must survive replay")
	li, err := s2.Storage(1).LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), li)
	hs, cs, err := s2.Storage(1).InitialState()
	require.NoError(t, err)
	assert.Zero(t, hs.Term)
	assert.Empty(t, cs.Voters)

	g2 := s2.Storage(2)
	li, err = g2.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(10), li, "the co-tenant group must be untouched")
	ents, err := g2.Entries(1, 11, 1<<20)
	require.NoError(t, err)
	require.Len(t, ents, 10)

	// With group 1 gone, group 2's compaction alone fully deadens the
	// shared segments.
	require.NoError(t, s2.Append(ctx, GroupWrite{
		GroupID: 2,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}},
		},
		HardState: &raftpb.HardState{Term: 1, Commit: 10},
	}))
	require.NoError(t, s2.Compact(2, 11))
	assert.Less(t, len(walSegmentPaths(t, dir)), before,
		"segments shared with a deleted group must reclaim once the survivor compacts")
	require.NoError(t, s2.Close())
}

// TestDeleteGroup_FlushesQueuedWritesBeforeTombstone pins the drop-vs-queued
// -append ordering: a stopped Ready loop abandons its result tickets, but a
// write it already submitted still sits in the batcher pipeline. DeleteGroup
// must drain that pipeline before writing its tombstone; otherwise the final
// append re-applies the group after the purge — and, because its record also
// lands after the tombstone on disk, the ghost survives a restart. The
// flush gate parks the batcher with the write in flight to make the race
// deterministic. (Red without the DeleteGroup flush barrier; the window
// existed in the bbolt engine too.)
func TestDeleteGroup_FlushesQueuedWritesBeforeTombstone(t *testing.T) {
	dir := t.TempDir()
	gate := &flushGate{}
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	s, err := Open(Options{Path: dir, Logger: log, BeforeFlush: gate.hook})
	require.NoError(t, err)
	t.Cleanup(func() {
		gate.open()
		_ = s.Close()
	})
	ctx := context.Background()

	gate.close()
	_, err = s.AppendAsync(ctx, GroupWrite{
		GroupID:   5,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "final-append")},
		HardState: &raftpb.HardState{Term: 1, Commit: 1},
	})
	require.NoError(t, err)

	delDone := make(chan error, 1)
	go func() { delDone <- s.DeleteGroup(5) }()

	// DeleteGroup must not complete while the flush covering the queued
	// write is parked.
	select {
	case err := <-delDone:
		t.Fatalf("DeleteGroup completed before the queued write flushed (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	gate.open()
	require.NoError(t, <-delDone)

	has, err := s.HasGroup(5)
	require.NoError(t, err)
	assert.False(t, has, "the queued write must have flushed BEFORE the tombstone")
	require.NoError(t, s.Close())

	// On-disk order must match: write record, then tombstone.
	s2 := newWALTestStore(t, dir, 0)
	has, err = s2.HasGroup(5)
	require.NoError(t, err)
	assert.False(t, has, "the tombstone must supersede the final append across a restart")
}

// TestWAL_GroupRecreationAfterDelete: a group id can be reused after
// DeleteGroup (same-name shard re-creation); replay must apply the
// tombstone and the re-bootstrap in order.
func TestWAL_GroupRecreationAfterDelete(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newWALTestStore(t, dir, 0)
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "first-life"), mkEntry(2, 1, "x")},
		HardState: &raftpb.HardState{Term: 5, Commit: 2},
	}))
	require.NoError(t, s.DeleteGroup(1))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "second-life")},
		HardState: &raftpb.HardState{Term: 1, Commit: 1},
	}))
	require.NoError(t, s.Close())

	s2 := newWALTestStore(t, dir, 0)
	li, err := s2.Storage(1).LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), li)
	ents, err := s2.Storage(1).Entries(1, 2, 1<<20)
	require.NoError(t, err)
	require.Len(t, ents, 1)
	assert.Equal(t, []byte("second-life"), ents[0].Data)
	hs, _, err := s2.Storage(1).InitialState()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), hs.Term, "the pre-delete HardState must not survive the tombstone")
}
