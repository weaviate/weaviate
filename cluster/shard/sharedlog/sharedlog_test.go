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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	s, err := Open(Options{
		Path:   filepath.Join(t.TempDir(), "shared.db"),
		Logger: log,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mkEntry(idx, term uint64, data string) raftpb.Entry {
	return raftpb.Entry{Index: idx, Term: term, Data: []byte(data)}
}

func TestOpen_FreshFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	s, err := Open(Options{Path: path, Logger: log})
	require.NoError(t, err)
	defer s.Close()

	hs, cs, err := s.Storage(1).(*groupStorage).InitialState()
	require.NoError(t, err)
	assert.True(t, hs.Term == 0 && hs.Vote == 0 && hs.Commit == 0)
	assert.Empty(t, cs.Voters)
}

func TestOpen_ReopenPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	s, err := Open(Options{Path: path, Logger: log})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   42,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "hello")},
		HardState: &raftpb.HardState{Term: 1, Vote: 7, Commit: 1},
	}))
	require.NoError(t, s.Close())

	s2, err := Open(Options{Path: path, Logger: log})
	require.NoError(t, err)
	defer s2.Close()

	has, err := s2.HasGroup(42)
	require.NoError(t, err)
	assert.True(t, has)

	g := s2.Storage(42)
	hs, _, err := g.InitialState()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), hs.Term)
	assert.Equal(t, uint64(7), hs.Vote)
	assert.Equal(t, uint64(1), hs.Commit)

	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), li)
}

func TestClose_Idempotent(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestAppend_AfterCloseReturnsError(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	s, err := Open(Options{Path: filepath.Join(t.TempDir(), "x.db"), Logger: log})
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err = s.Append(context.Background(), GroupWrite{GroupID: 1})
	assert.ErrorIs(t, err, ErrStoreClosed)
}

func TestAppend_SingleGroupRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Entries: []raftpb.Entry{
			mkEntry(1, 1, "a"),
			mkEntry(2, 1, "b"),
			mkEntry(3, 2, "c"),
		},
		HardState: &raftpb.HardState{Term: 2, Vote: 3, Commit: 3},
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}))

	g := s.Storage(1)
	hs, cs, err := g.InitialState()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), hs.Term)
	assert.Equal(t, []uint64{1, 2, 3}, cs.Voters)

	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), li)

	fi, err := g.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), fi)

	ents, err := g.Entries(1, 4, 1024)
	require.NoError(t, err)
	require.Len(t, ents, 3)
	assert.Equal(t, []byte("a"), ents[0].Data)
	assert.Equal(t, []byte("c"), ents[2].Data)
}

func TestAppend_MultipleGroupsIndependent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   100,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "g100")},
		HardState: &raftpb.HardState{Term: 1, Commit: 1},
	}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   200,
		Entries:   []raftpb.Entry{mkEntry(1, 5, "g200-a"), mkEntry(2, 5, "g200-b")},
		HardState: &raftpb.HardState{Term: 5, Commit: 2},
	}))

	g100 := s.Storage(100)
	g200 := s.Storage(200)

	li100, _ := g100.LastIndex()
	li200, _ := g200.LastIndex()
	assert.Equal(t, uint64(1), li100)
	assert.Equal(t, uint64(2), li200)

	hs100, _, _ := g100.InitialState()
	hs200, _, _ := g200.InitialState()
	assert.Equal(t, uint64(1), hs100.Term)
	assert.Equal(t, uint64(5), hs200.Term)

	ents, err := g100.Entries(1, 2, 1024)
	require.NoError(t, err)
	require.Len(t, ents, 1)
	assert.Equal(t, []byte("g100"), ents[0].Data)
}

func TestAppend_EntryTruncation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Entries: []raftpb.Entry{
			mkEntry(5, 1, "e5"), mkEntry(6, 1, "e6"),
			mkEntry(7, 1, "e7"), mkEntry(8, 1, "e8"),
			mkEntry(9, 1, "e9"), mkEntry(10, 1, "e10"),
		},
	}))

	// Simulate a new leader overwriting the tail from index 7.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Entries: []raftpb.Entry{
			mkEntry(7, 2, "new7"),
			mkEntry(8, 2, "new8"),
		},
	}))

	g := s.Storage(1)
	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(8), li)

	ents, err := g.Entries(5, 9, 4096)
	require.NoError(t, err)
	require.Len(t, ents, 4)
	assert.Equal(t, []byte("e5"), ents[0].Data)
	assert.Equal(t, []byte("e6"), ents[1].Data)
	assert.Equal(t, []byte("new7"), ents[2].Data)
	assert.Equal(t, []byte("new8"), ents[3].Data)
	assert.Equal(t, uint64(2), ents[2].Term)
}

func TestAppend_ConcurrentGroups(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const numGroups = 10
	const entriesPerGroup = 20

	var wg sync.WaitGroup
	for g := 1; g <= numGroups; g++ {
		wg.Add(1)
		go func(groupID uint64) {
			defer wg.Done()
			for i := uint64(1); i <= entriesPerGroup; i++ {
				err := s.Append(ctx, GroupWrite{
					GroupID:   groupID,
					Entries:   []raftpb.Entry{mkEntry(i, 1, "x")},
					HardState: &raftpb.HardState{Term: 1, Commit: i},
				})
				if err != nil {
					t.Errorf("append failed for group %d index %d: %v", groupID, i, err)
					return
				}
			}
		}(uint64(g))
	}
	wg.Wait()

	for g := uint64(1); g <= numGroups; g++ {
		st := s.Storage(g)
		li, err := st.LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(entriesPerGroup), li, "group %d last index", g)
		hs, _, err := st.InitialState()
		require.NoError(t, err)
		assert.Equal(t, uint64(entriesPerGroup), hs.Commit, "group %d commit", g)
	}
}

func TestClose_DrainsPendingAppends(t *testing.T) {
	// Long timer + large batch size keeps writes queued so Close has
	// to actually drain them rather than the batcher flushing first.
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	dbPath := filepath.Join(t.TempDir(), "drain.db")
	s, err := Open(Options{
		Path:         dbPath,
		Logger:       log,
		BatchMaxWait: 50 * time.Millisecond,
		BatchMaxSize: 1000,
	})
	require.NoError(t, err)

	// One group per goroutine: avoids the leader-change truncation
	// rule masking each other's writes during concurrent Appends.
	ctx := context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := make([]uint64, 0)

	const numAppends = 20
	for g := uint64(1); g <= numAppends; g++ {
		wg.Add(1)
		go func(groupID uint64) {
			defer wg.Done()
			err := s.Append(ctx, GroupWrite{
				GroupID:   groupID,
				Entries:   []raftpb.Entry{mkEntry(1, 1, "x")},
				HardState: &raftpb.HardState{Term: 1, Commit: 1},
			})
			if err == nil {
				mu.Lock()
				succeeded = append(succeeded, groupID)
				mu.Unlock()
				return
			}
			// Appends that didn't cross the close-mu boundary before
			// closed=true was set legitimately get ErrStoreClosed.
			assert.ErrorIs(t, err, ErrStoreClosed)
		}(g)
	}

	time.Sleep(10 * time.Millisecond)
	require.NoError(t, s.Close())
	wg.Wait()

	require.NotEmpty(t, succeeded)

	s2, err := Open(Options{Path: dbPath, Logger: log})
	require.NoError(t, err)
	defer s2.Close()

	for _, g := range succeeded {
		has, err := s2.HasGroup(g)
		require.NoError(t, err)
		assert.True(t, has, "group %d HardState missing after Close drained", g)
		li, err := s2.Storage(g).LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), li, "group %d entry missing after Close drained", g)
	}
}

func TestCompact_RemovesEntriesBelowIndex(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	entries := make([]raftpb.Entry, 10)
	for i := range entries {
		entries[i] = mkEntry(uint64(i+1), 1, "e")
	}
	require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Entries: entries}))

	// Snapshot is the Compact prereq; otherwise FirstIndex won't advance.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snap"),
			Metadata: raftpb.SnapshotMetadata{
				Index:     5,
				Term:      1,
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
			},
		},
	}))

	require.NoError(t, s.Compact(1, 5))

	g := s.Storage(1)
	fi, err := g.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(6), fi)

	_, err = g.Entries(1, 5, 1024)
	assert.ErrorIs(t, err, raft.ErrCompacted)

	got, err := g.Entries(6, 11, 4096)
	require.NoError(t, err)
	assert.Len(t, got, 5)
}

func TestCompact_Idempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 1,
		Entries: []raftpb.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 1, "c")},
	}))

	require.NoError(t, s.Compact(1, 2))
	require.NoError(t, s.Compact(1, 2))
	require.NoError(t, s.Compact(1, 1))
}

func TestDeleteGroup_RemovesAllData(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "x"), mkEntry(2, 1, "y")},
		HardState: &raftpb.HardState{Term: 1, Commit: 2},
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}},
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snap"),
			Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1},
		},
	}))

	has, _ := s.HasGroup(1)
	require.True(t, has)

	require.NoError(t, s.DeleteGroup(1))

	has, err := s.HasGroup(1)
	require.NoError(t, err)
	assert.False(t, has)

	g := s.Storage(1)
	hs, cs, err := g.InitialState()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), hs.Term)
	assert.Empty(t, cs.Voters)
	li, _ := g.LastIndex()
	assert.Equal(t, uint64(0), li)
}

func TestDeleteGroup_OtherGroupsUntouched(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   1,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "a")},
		HardState: &raftpb.HardState{Term: 1, Commit: 1},
	}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   2,
		Entries:   []raftpb.Entry{mkEntry(1, 1, "b")},
		HardState: &raftpb.HardState{Term: 1, Commit: 1},
	}))

	require.NoError(t, s.DeleteGroup(1))

	has, _ := s.HasGroup(2)
	assert.True(t, has)
	li, _ := s.Storage(2).LastIndex()
	assert.Equal(t, uint64(1), li)
}

func TestHasGroup_OnlyTrueAfterHardStateWritten(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	has, _ := s.HasGroup(99)
	assert.False(t, has)

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: 99,
		Entries: []raftpb.Entry{mkEntry(1, 1, "x")},
	}))
	has, _ = s.HasGroup(99)
	assert.False(t, has, "entries without HardState do not mark a group as bootstrapped")

	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   99,
		HardState: &raftpb.HardState{Term: 1},
	}))
	has, _ = s.HasGroup(99)
	assert.True(t, has)
}
