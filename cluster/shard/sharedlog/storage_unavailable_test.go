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
	"io"
	stdlog "log"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// raftLeaderHarness drives a real etcd RawNode leader (node 1 of voters
// {1, 2}) over a sharedlog-backed storage, persisting Ready output through the
// store exactly as the production append path does (synchronously here). It
// exists to exercise etcd's real snapshot-send path — maybeSendAppend routes
// ANY Term/Entries error into maybeSendSnapshot, which panics with "need
// non-empty snapshot" if Storage.Snapshot returns an empty snapshot with a nil
// error (the contract violation of minor-issues.md #9).
type raftLeaderHarness struct {
	t   *testing.T
	s   *Store
	gid uint64
	rn  *raft.RawNode
}

func newRaftLeaderHarness(t *testing.T, s *Store, gid uint64) *raftLeaderHarness {
	t.Helper()
	ctx := context.Background()
	ents := make([]raftpb.Entry, 5)
	for i := range ents {
		ents[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1, Data: []byte("x")}
	}
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   gid,
		Entries:   ents,
		HardState: &raftpb.HardState{Term: 1, Commit: 5},
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}},
	}))
	rn, err := raft.NewRawNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         s.Storage(gid),
		Applied:         5,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 64,
		Logger:          &raft.DefaultLogger{Logger: stdlog.New(io.Discard, "", 0)},
	})
	require.NoError(t, err)
	return &raftLeaderHarness{t: t, s: s, gid: gid, rn: rn}
}

// drainReady persists entries/hardstate through the sharedlog and returns the
// outbound messages of every pending Ready.
func (h *raftLeaderHarness) drainReady() []raftpb.Message {
	var msgs []raftpb.Message
	for h.rn.HasReady() {
		rd := h.rn.Ready()
		gw := GroupWrite{GroupID: h.gid, Entries: rd.Entries}
		if !raft.IsEmptyHardState(rd.HardState) {
			hs := rd.HardState
			gw.HardState = &hs
		}
		if len(gw.Entries) > 0 || gw.HardState != nil {
			require.NoError(h.t, h.s.Append(context.Background(), gw))
		}
		msgs = append(msgs, rd.Messages...)
		h.rn.Advance(rd)
	}
	return msgs
}

func (h *raftLeaderHarness) becomeLeader() {
	require.NoError(h.t, h.rn.Campaign())
	h.drainReady()
	require.NoError(h.t, h.rn.Step(raftpb.Message{
		From: 2, To: 1, Term: h.rn.BasicStatus().Term, Type: raftpb.MsgVoteResp,
	}))
	h.drainReady()
	require.Equal(h.t, raft.StateLeader, h.rn.BasicStatus().RaftState)
}

// stepLaggingReject feeds the rejection a follower whose log only reaches
// index 2 would send for the leader's post-election probe (prev=5) — the
// catch-up path for a lagging follower.
func (h *raftLeaderHarness) stepLaggingReject() []raftpb.Message {
	require.NoError(h.t, h.rn.Step(raftpb.Message{
		From: 2, To: 1, Term: h.rn.BasicStatus().Term, Type: raftpb.MsgAppResp,
		Reject: true, RejectHint: 2, LogTerm: 0, Index: 5,
	}))
	return h.drainReady()
}

func hasMsgSnap(msgs []raftpb.Message) bool {
	for _, m := range msgs {
		if m.Type == raftpb.MsgSnap {
			return true
		}
	}
	return false
}

// TestStorage_SnapshotSend_HealthyPath pins the healthy catch-up flow: with a
// servable snapshot at 3 and entries 1..3 compacted behind it (production
// order: durable snapshot Append, then Compact), the lagging-follower reject
// yields a MsgSnap.
func TestStorage_SnapshotSend_HealthyPath(t *testing.T) {
	s := newTestStore(t)
	h := newRaftLeaderHarness(t, s, 1)
	h.becomeLeader()

	require.NoError(t, s.Append(context.Background(), GroupWrite{
		GroupID: 1,
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snapdata"),
			Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
		},
	}))
	require.NoError(t, s.Compact(1, 4))

	msgs := h.stepLaggingReject()
	require.True(t, hasMsgSnap(msgs), "healthy compacted leader must send a snapshot to a lagging follower, got %v", msgs)
}

// TestStorage_SnapshotUnavailable_LeaderRetriesAndRecovers is the W-A
// regression test for minor-issues.md #9: a leader whose entry range no longer
// reaches back to a lagging follower's log, with NO servable snapshot, must
// NOT panic ("need non-empty snapshot") and must NOT kill its Ready loop —
// etcd retries against ErrSnapshotTemporarilyUnavailable — and once a snapshot
// becomes servable the follower catch-up proceeds with a MsgSnap.
func TestStorage_SnapshotUnavailable_LeaderRetriesAndRecovers(t *testing.T) {
	s := newTestStore(t)
	h := newRaftLeaderHarness(t, s, 1)
	h.becomeLeader()

	// Entries pruned with no covering snapshot — the split-brain state the
	// live panic requires (out-of-contract Compact represents it directly;
	// see minor-issues.md #9 for the production constructors).
	require.NoError(t, s.Compact(1, 4))

	var msgs []raftpb.Message
	require.NotPanics(t, func() { msgs = h.stepLaggingReject() },
		"lagging-follower catch-up against an unservable snapshot must retry, not panic")
	require.False(t, hasMsgSnap(msgs), "nothing servable yet — no MsgSnap expected")

	// The Ready loop survived; make a snapshot servable and re-probe: the
	// follower must now be offered the snapshot.
	require.NoError(t, s.Append(context.Background(), GroupWrite{
		GroupID: 1,
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snapdata"),
			Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
		},
	}))
	require.NoError(t, h.rn.Step(raftpb.Message{
		From: 2, To: 1, Term: h.rn.BasicStatus().Term, Type: raftpb.MsgHeartbeatResp,
	}))
	msgs = h.drainReady()
	require.True(t, hasMsgSnap(msgs), "snapshot became servable — catch-up must resume with a MsgSnap, got %v", msgs)
}

// TestStorage_SnapshotUnavailable_TombstonedGroupNoPanic covers the gs==nil
// shape of the same contract violation: a group deleted out from under a live
// RawNode (reachable only through lifecycle misuse, but the storage must
// still answer with the retryable sentinel, never empty+nil).
func TestStorage_SnapshotUnavailable_TombstonedGroupNoPanic(t *testing.T) {
	s := newTestStore(t)
	h := newRaftLeaderHarness(t, s, 1)
	h.becomeLeader()

	require.NoError(t, s.DeleteGroup(1))

	var msgs []raftpb.Message
	require.NotPanics(t, func() { msgs = h.stepLaggingReject() },
		"catch-up against a tombstoned group must retry, not panic")
	require.False(t, hasMsgSnap(msgs))
}
