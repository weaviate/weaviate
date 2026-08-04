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
	"go.etcd.io/raft/v3/raftpb"
)

// flushGate parks the batcher inside flush (BeforeFlush) while closed.
type flushGate struct {
	mu sync.Mutex
	ch chan struct{}
}

func (g *flushGate) hook() {
	g.mu.Lock()
	ch := g.ch
	g.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

func (g *flushGate) close() {
	g.mu.Lock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *flushGate) open() {
	g.mu.Lock()
	if g.ch != nil {
		close(g.ch)
		g.ch = nil
	}
	g.mu.Unlock()
}

func newGatedTestStore(t *testing.T, gate *flushGate) *Store {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	s, err := Open(Options{
		Path:        filepath.Join(t.TempDir(), "shared.db"),
		Logger:      log,
		BeforeFlush: gate.hook,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		gate.open() // a parked flush must not deadlock Close
		_ = s.Close()
	})
	return s
}

// TestAppendAsync_ResolvesOnlyAfterCoveringFlush pins the durability contract
// AppendAsync carries for the append pipeline: the returned channel must not
// resolve while the covering flush (= fsync) is parked — including for a write
// submitted DURING an in-flight flush, which rides the next one.
func TestAppendAsync_ResolvesOnlyAfterCoveringFlush(t *testing.T) {
	gate := &flushGate{}
	s := newGatedTestStore(t, gate)
	ctx := context.Background()

	gate.close()
	done0, err := s.AppendAsync(ctx, GroupWrite{
		GroupID: 1, Entries: []raftpb.Entry{mkEntry(1, 1, "w0")},
	})
	require.NoError(t, err)

	// Wait until the batcher is parked inside w0's flush, then submit w1 —
	// the pipelining case: it must queue for the NEXT flush, unresolved.
	require.Eventually(t, func() bool { return len(s.reqCh) == 0 }, 2*time.Second, time.Millisecond,
		"batcher never picked up w0")
	done1, err := s.AppendAsync(ctx, GroupWrite{
		GroupID: 1, Entries: []raftpb.Entry{mkEntry(2, 1, "w1")},
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done0:
		t.Fatal("w0 resolved while its covering flush was parked")
	case <-done1:
		t.Fatal("w1 resolved while every flush was parked")
	default:
	}

	gate.open()
	select {
	case err := <-done0:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("w0 never resolved after the flush gate opened")
	}
	select {
	case err := <-done1:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("w1 never resolved after the flush gate opened")
	}

	li, err := s.Storage(1).LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), li)
}

func TestAppendAsync_AfterCloseReturnsError(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	s, err := Open(Options{Path: filepath.Join(t.TempDir(), "x.db"), Logger: log})
	require.NoError(t, err)
	require.NoError(t, s.Close())

	_, err = s.AppendAsync(context.Background(), GroupWrite{GroupID: 1})
	assert.ErrorIs(t, err, ErrStoreClosed)
}

// TestAppendAsync_AbandonedResultDoesNotWedgeClose pins the inflight
// accounting hand-off: results are delivered (and inflight released) by the
// batcher, so a caller that abandons its result channels — the append worker
// shutting down mid-pipeline — cannot wedge Close's inflight drain, and the
// abandoned writes still complete durably.
func TestAppendAsync_AbandonedResultDoesNotWedgeClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "abandon.db")
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	s, err := Open(Options{Path: path, Logger: log})
	require.NoError(t, err)

	ctx := context.Background()
	for i := uint64(1); i <= 8; i++ {
		_, err := s.AppendAsync(ctx, GroupWrite{
			GroupID:   7,
			Entries:   []raftpb.Entry{mkEntry(i, 1, "x")},
			HardState: &raftpb.HardState{Term: 1, Commit: i},
		})
		require.NoError(t, err)
		// Channels deliberately abandoned.
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close wedged behind abandoned AppendAsync results")
	}

	s2, err := Open(Options{Path: path, Logger: log})
	require.NoError(t, err)
	defer s2.Close()
	li, err := s2.Storage(7).LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(8), li, "abandoned writes must still have been flushed durably")
}

// TestFlush_SameBatchOverlappingWrites pins the semantics the append pipeline
// newly relies on: two writes for the SAME group sharing one flush (one bbolt
// tx) must produce the identical end state to serial flushes — the second
// write's truncate-at-first-index must see and overwrite the first write's
// entries within the tx. Before pipelining, same-group writes could never
// share a tx (the append worker submitted them one fsync apart).
func TestFlush_SameBatchOverlappingWrites(t *testing.T) {
	s := newTestStore(t)

	w1 := &batchReq{
		write: GroupWrite{
			GroupID: 3,
			Entries: []raftpb.Entry{
				mkEntry(5, 1, "e5"), mkEntry(6, 1, "e6"), mkEntry(7, 1, "e7"),
			},
			HardState: &raftpb.HardState{Term: 1, Commit: 4},
		},
		done: make(chan error, 1),
	}
	// A new leader's tail overwrite from index 6, in the SAME batch.
	w2 := &batchReq{
		write: GroupWrite{
			GroupID: 3,
			Entries: []raftpb.Entry{
				mkEntry(6, 2, "new6"),
			},
			HardState: &raftpb.HardState{Term: 2, Vote: 9, Commit: 5},
		},
		done: make(chan error, 1),
	}
	// flush accounts inflight per request; mirror AppendAsync's submission
	// increments so the batcher-side release balances.
	s.inflight.Add(2)
	s.flush([]*batchReq{w1, w2})
	require.NoError(t, <-w1.done)
	require.NoError(t, <-w2.done)
	require.Zero(t, s.inflight.Load())

	g := s.Storage(3)
	li, err := g.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(6), li, "the same-tx overwrite must truncate the first write's tail")

	ents, err := g.Entries(5, 7, 4096)
	require.NoError(t, err)
	require.Len(t, ents, 2)
	assert.Equal(t, []byte("e5"), ents[0].Data)
	assert.Equal(t, []byte("new6"), ents[1].Data)
	assert.Equal(t, uint64(2), ents[1].Term)

	hs, _, err := g.InitialState()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), hs.Term, "the later write's HardState must win within the tx")
	assert.Equal(t, uint64(9), hs.Vote)
	assert.Equal(t, uint64(5), hs.Commit)
}
