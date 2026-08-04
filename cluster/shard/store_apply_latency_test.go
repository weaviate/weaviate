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

package shard

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	testSaltA = uint64(0xAAAAAAAA) << 32
	testSaltB = uint64(0xBBBBBBBB) << 32
)

// TestWakePending_SaltGuard pins the false-ack regression: a pending
// registered on this store must be unreachable via a reqID carrying another
// node's (or another boot's) salt, even with an identical counter — a
// colliding wake would acknowledge a client write with someone else's entry.
func TestWakePending_SaltGuard(t *testing.T) {
	tests := []struct {
		name      string
		wakeWith  uint64
		wantWoken bool
	}{
		{name: "own reqID wakes the pending", wakeWith: testSaltA | 7, wantWoken: true},
		{name: "foreign salt with colliding counter must not wake", wakeWith: testSaltB | 7, wantWoken: false},
		{name: "bare counter must not wake", wakeWith: 7, wantWoken: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newBareStore(true, 1)
			s.reqIDSalt = testSaltA
			p := &pendingApply{done: make(chan applyResult, 1)}
			s.pending.Store(testSaltA|7, p)
			s.wakePending(tc.wakeWith, applyResult{idx: 99})
			require.Equal(t, tc.wantWoken, len(p.done) == 1)
		})
	}
}

// TestAckCommitted_SaltGuard pins the same false-ack property at the commit
// ack site — the site that now acknowledges client writes: only an entry
// carrying this store instance's salt may ack (and stamp) a local pending; a
// foreign salt with a colliding counter, or a bare counter, must ack nothing.
// A successful ack carries the entry's log index and advances the
// committed-staged watermark.
func TestAckCommitted_SaltGuard(t *testing.T) {
	tests := []struct {
		name      string
		reqID     uint64
		wantAcked bool
	}{
		{name: "own reqID is acked at commit staging", reqID: testSaltA | 7, wantAcked: true},
		{name: "foreign salt with colliding counter must not be acked", reqID: testSaltB | 7, wantAcked: false},
		{name: "bare counter must not be acked", reqID: 7, wantAcked: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newBareStore(true, 1)
			s.config.ClassName, s.config.ShardName = "AckC", "AckS"
			s.reqIDSalt = testSaltA
			p := &pendingApply{done: make(chan applyResult, 1), proposedAt: time.Now()}
			s.pending.Store(testSaltA|7, p)

			stamps := s.ackCommitted([]raftpb.Entry{
				{Type: raftpb.EntryNormal, Index: 42, Data: encodeCmd(tc.reqID, []byte("cmd"))},
			})
			require.Equal(t, tc.wantAcked, len(p.done) == 1)
			if tc.wantAcked {
				res := <-p.done
				require.NoError(t, res.err)
				require.Equal(t, uint64(42), res.idx, "the ack must carry the committed entry's index")
				require.Len(t, stamps, 1)
				require.False(t, stamps[0].IsZero(), "an acked entry must be commit-stamped for the apply worker")
			} else {
				require.Nil(t, stamps, "a batch without local entries must carry no stamps")
			}
			require.Equal(t, uint64(42), s.CommittedIndex(),
				"staging must advance the committed watermark regardless of the proposer")
		})
	}
}

// histSample reads a histogram's cumulative sample count and sum; tests
// assert deltas because the collectors are shared across the package's tests.
func histSample(t *testing.T, o prometheus.Observer) (uint64, float64) {
	h, ok := o.(prometheus.Histogram)
	require.True(t, ok)
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.Histogram.GetSampleCount(), m.Histogram.GetSampleSum()
}

// TestApplyLatencyHistograms drives a batch through ackCommitted →
// applyEntries and pins the observation sites of the histogram pair:
// propose→commit at the commit ack on the Ready-loop side, commit→apply at
// dispatch completion on the apply worker via the stamps carried on the
// applyItem — only for entries proposed by this store instance. Foreign
// entries, empty no-ops, and pendings resolved before commit observe nothing.
func TestApplyLatencyHistograms(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	s := newBareStore(true, 1)
	s.config.ClassName, s.config.ShardName = "LatC", "LatS"
	s.reqIDSalt = testSaltA
	s.log = logger.WithField("t", "test") // silence expected dispatch-failure noise
	s.fsm = NewFSM("LatC", "LatS", "n1", logger)

	pc := shardRaftProposeCommit.WithLabelValues("LatC", "LatS")
	ca := shardRaftCommitApply.WithLabelValues("LatC", "LatS")
	pcCount0, pcSum0 := histSample(t, pc)
	caCount0, _ := histSample(t, ca)

	reqID := testSaltA | 1
	p := &pendingApply{done: make(chan applyResult, 1), proposedAt: time.Now().Add(-time.Second)}
	s.pending.Store(reqID, p)

	entries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Index: 9, Data: encodeCmd(reqID, []byte("cmd"))},
		{Type: raftpb.EntryNormal, Index: 10, Data: encodeCmd(testSaltB|1, []byte("foreign"))},
		{Type: raftpb.EntryNormal, Index: 11}, // empty leader no-op
	}
	stamps := s.ackCommitted(entries)

	pcCount, pcSum := histSample(t, pc)
	require.Equal(t, pcCount0+1, pcCount, "one propose→commit sample at the ack site")
	require.GreaterOrEqual(t, pcSum-pcSum0, 1.0, "propose→commit must span the pre-dated proposal")
	caCount, _ := histSample(t, ca)
	require.Equal(t, caCount0, caCount, "commit→apply must not observe before dispatch")

	require.True(t, s.applyEntries(applyItem{entries: entries, commitStamps: stamps}))
	caCount, _ = histSample(t, ca)
	require.Equal(t, caCount0+1, caCount,
		"exactly one commit→apply sample — the locally-proposed entry, at dispatch completion")

	// A pending resolved before commit (leadership loss, backpressure) is
	// gone from the map when its entry commits — nothing acks or observes.
	resolved := testSaltA | 2
	stamps = s.ackCommitted([]raftpb.Entry{
		{Type: raftpb.EntryNormal, Index: 12, Data: encodeCmd(resolved, []byte("cmd"))},
	})
	require.Nil(t, stamps)
	pcCount, _ = histSample(t, pc)
	require.Equal(t, pcCount0+1, pcCount, "an unregistered pending must not observe")
}

// TestApplyEntries_DispatchFailureCounter pins the post-commit failure
// surface that replaced the client-visible dispatch error when the ack moved
// to commit time: a committed entry whose dispatch fails (here: an
// unmarshalable payload against an FSM with no shard) increments the failure
// counter, and the applied watermark still advances past the entry.
func TestApplyEntries_DispatchFailureCounter(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	s := newBareStore(true, 1)
	s.config.ClassName, s.config.ShardName = "FailC", "FailS"
	s.reqIDSalt = testSaltA
	s.log = logger.WithField("t", "test") // silence expected dispatch-failure noise
	s.fsm = NewFSM("FailC", "FailS", "n1", logger)

	ctr := shardRaftApplyDispatchFailures.WithLabelValues("FailC", "FailS")
	before := testutil.ToFloat64(ctr)

	require.True(t, s.applyEntries(applyItem{entries: []raftpb.Entry{
		{Type: raftpb.EntryNormal, Index: 5, Data: encodeCmd(testSaltA|1, []byte("not-a-proto"))},
	}}))
	require.Equal(t, before+1, testutil.ToFloat64(ctr),
		"a failed post-commit dispatch must be counted for operators")
	require.Equal(t, uint64(5), s.fsm.LastAppliedIndex(),
		"the applied watermark must advance past a failed dispatch")
}
