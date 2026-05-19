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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestStorage_EmptyGroup(t *testing.T) {
	s := newTestStore(t)
	g := s.Storage(7)

	fi, err := g.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), fi)

	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), li)

	term, err := g.Term(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), term)

	_, err = g.Term(1)
	assert.ErrorIs(t, err, raft.ErrUnavailable)

	_, err = g.Entries(1, 2, 1024)
	assert.ErrorIs(t, err, raft.ErrUnavailable)

	snap, err := g.Snapshot()
	require.NoError(t, err)
	assert.True(t, raft.IsEmptySnap(snap))

	hs, cs, err := g.InitialState()
	require.NoError(t, err)
	assert.True(t, hs.Term == 0 && hs.Vote == 0 && hs.Commit == 0)
	assert.Empty(t, cs.Voters)
}

func TestStorage_WithEntries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	ents := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
		{Index: 4, Term: 2, Data: []byte("d")},
		{Index: 5, Term: 3, Data: []byte("e")},
		{Index: 6, Term: 3, Data: []byte("f")},
		{Index: 7, Term: 3, Data: []byte("g")},
		{Index: 8, Term: 3, Data: []byte("h")},
		{Index: 9, Term: 3, Data: []byte("i")},
		{Index: 10, Term: 3, Data: []byte("j")},
	}
	require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Entries: ents}))

	g := s.Storage(1)

	li, _ := g.LastIndex()
	assert.Equal(t, uint64(10), li)

	fi, _ := g.FirstIndex()
	assert.Equal(t, uint64(1), fi)

	term, err := g.Term(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), term)

	got, err := g.Entries(2, 5, 4096)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []byte("b"), got[0].Data)
	assert.Equal(t, []byte("d"), got[2].Data)
}

func TestStorage_EntriesMaxSizeRespected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	payload := make([]byte, 100)
	ents := make([]raftpb.Entry, 5)
	for i := range ents {
		ents[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1, Data: payload}
	}
	require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Entries: ents}))

	g := s.Storage(1)

	got, err := g.Entries(1, 6, 150)
	require.NoError(t, err)
	require.Len(t, got, 1)

	// maxSize=0 must still return one entry — etcd's contract.
	got, err = g.Entries(1, 6, 0)
	require.NoError(t, err)
	require.Len(t, got, 1)

	got, err = g.Entries(1, 6, 1024*1024)
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestStorage_EntriesErrors(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Entries: []raftpb.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
			{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1},
			{Index: 7, Term: 1}, {Index: 8, Term: 1}, {Index: 9, Term: 1},
			{Index: 10, Term: 1},
		},
	}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snap"),
			Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 1},
		},
	}))
	require.NoError(t, s.Compact(1, 5))

	g := s.Storage(1)

	_, err := g.Entries(1, 3, 1024)
	assert.ErrorIs(t, err, raft.ErrCompacted)

	_, err = g.Entries(6, 100, 1024)
	assert.ErrorIs(t, err, raft.ErrUnavailable)

	got, err := g.Entries(6, 11, 1024)
	require.NoError(t, err)
	assert.Len(t, got, 5)
}

func TestStorage_TermErrors(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	g := s.Storage(1)

	term, err := g.Term(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), term)

	ents := make([]raftpb.Entry, 10)
	for i := range ents {
		ents[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1}
	}
	require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Entries: ents}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 2},
		},
	}))
	require.NoError(t, s.Compact(1, 5))

	_, err = g.Term(4)
	assert.ErrorIs(t, err, raft.ErrCompacted)

	// Term(snapshot.Index) returns snapshot.Term (2), not the
	// (now-compacted) entry's term (1).
	term, err = g.Term(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), term)

	term, err = g.Term(10)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), term)

	_, err = g.Term(11)
	assert.ErrorIs(t, err, raft.ErrUnavailable)
}

func TestStorage_InitialStateRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	hsWant := raftpb.HardState{Term: 3, Vote: 7, Commit: 42}
	csWant := raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		HardState: &hsWant,
		ConfState: &csWant,
	}))

	hs, cs, err := s.Storage(1).InitialState()
	require.NoError(t, err)
	assert.Equal(t, hsWant, hs)
	assert.Equal(t, csWant.Voters, cs.Voters)
	assert.Equal(t, csWant.Learners, cs.Learners)
}

func TestStorage_SnapshotRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	snapWant := raftpb.Snapshot{
		Data: []byte("synthetic-snapshot-data"),
		Metadata: raftpb.SnapshotMetadata{
			Index:     20,
			Term:      4,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
	require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Snapshot: &snapWant}))

	got, err := s.Storage(1).Snapshot()
	require.NoError(t, err)
	assert.Equal(t, snapWant.Data, got.Data)
	assert.Equal(t, snapWant.Metadata.Index, got.Metadata.Index)
	assert.Equal(t, snapWant.Metadata.Term, got.Metadata.Term)
	assert.Equal(t, snapWant.Metadata.ConfState.Voters, got.Metadata.ConfState.Voters)
}

func TestStorage_FirstIndexAfterSnapshot(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Snapshot: &raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 5},
		},
	}))

	g := s.Storage(1)

	fi, err := g.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(21), fi)

	// With no entries yet, LastIndex falls back to snapshot.Index.
	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(20), li)

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Entries: []raftpb.Entry{
			{Index: 21, Term: 5}, {Index: 22, Term: 5},
			{Index: 23, Term: 5}, {Index: 24, Term: 5}, {Index: 25, Term: 5},
		},
	}))

	fi, _ = g.FirstIndex()
	li, _ = g.LastIndex()
	assert.Equal(t, uint64(21), fi)
	assert.Equal(t, uint64(25), li)
}

func TestStorage_AcrossGroups(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   100,
		Entries:   []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("A1")}, {Index: 2, Term: 1, Data: []byte("A2")}},
		HardState: &raftpb.HardState{Term: 1, Commit: 2},
	}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   200,
		Entries:   []raftpb.Entry{{Index: 1, Term: 7, Data: []byte("B1")}, {Index: 2, Term: 7, Data: []byte("B2")}, {Index: 3, Term: 7, Data: []byte("B3")}},
		HardState: &raftpb.HardState{Term: 7, Commit: 3},
	}))

	g100 := s.Storage(100)
	g200 := s.Storage(200)

	li100, _ := g100.LastIndex()
	li200, _ := g200.LastIndex()
	assert.Equal(t, uint64(2), li100)
	assert.Equal(t, uint64(3), li200)

	t100, _ := g100.Term(1)
	t200, _ := g200.Term(1)
	assert.Equal(t, uint64(1), t100)
	assert.Equal(t, uint64(7), t200)

	e100, _ := g100.Entries(1, 3, 4096)
	e200, _ := g200.Entries(1, 4, 4096)
	require.Len(t, e100, 2)
	require.Len(t, e200, 3)
	assert.Equal(t, []byte("A1"), e100[0].Data)
	assert.Equal(t, []byte("B1"), e200[0].Data)
}
