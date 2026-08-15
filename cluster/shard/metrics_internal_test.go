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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// dropDelta reads the process-global drop counter for a site; tests assert
// deltas because the counters are shared across the package's tests.
func dropDelta(site string) func() float64 {
	before := testutil.ToFloat64(shardRaftDropped.WithLabelValues(site))
	return func() float64 {
		return testutil.ToFloat64(shardRaftDropped.WithLabelValues(site)) - before
	}
}

// newBareStore builds an unstarted Store with just enough wiring for step().
func newBareStore(started bool, queueCap int) *Store {
	logger := logrus.New()
	s := &Store{
		config:  StoreConfig{ClassName: "C", ShardName: "S"},
		log:     logger.WithField("t", "test"),
		groupID: 42,
	}
	if started {
		s.started = true
		s.incomingMsgCh = make(chan raftpb.Message, queueCap)
	}
	return s
}

func TestStep_DropSiteAccounting(t *testing.T) {
	tests := []struct {
		name     string
		store    *Store
		prefill  int
		msg      raftpb.Message
		site     string
		expected float64
	}{
		{
			name:     "local-only message type is rejected at the trust boundary",
			store:    newBareStore(true, 4),
			msg:      raftpb.Message{Type: raftpb.MsgStorageAppendResp},
			site:     dropSiteStepLocalSpoof,
			expected: 1,
		},
		{
			name:     "message before Start is dropped as not-live",
			store:    newBareStore(false, 0),
			msg:      raftpb.Message{Type: raftpb.MsgHeartbeat},
			site:     dropSiteStepNotLive,
			expected: 1,
		},
		{
			name:     "message over a full queue is dropped as queue-full",
			store:    newBareStore(true, 1),
			prefill:  1,
			msg:      raftpb.Message{Type: raftpb.MsgApp},
			site:     dropSiteStepQueueFull,
			expected: 1,
		},
		{
			name:     "deliverable message is not counted as any drop",
			store:    newBareStore(true, 4),
			msg:      raftpb.Message{Type: raftpb.MsgHeartbeat},
			site:     dropSiteStepQueueFull,
			expected: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < tc.prefill; i++ {
				tc.store.incomingMsgCh <- raftpb.Message{}
			}
			delta := dropDelta(tc.site)
			tc.store.step(tc.msg)
			require.Equal(t, tc.expected, delta())
		})
	}
}

func TestRouteMessage_UnknownGroupAccounting(t *testing.T) {
	logger := logrus.New()
	reg := &Registry{log: logger.WithField("t", "test")}
	known := newBareStore(true, 4)
	reg.groups.Store(uint64(7), known)

	tests := []struct {
		name          string
		groupID       uint64
		unknownDelta  float64
		deliveredMsgs int
	}{
		{name: "registered group routes to the store", groupID: 7, unknownDelta: 0, deliveredMsgs: 1},
		{name: "unknown group is counted and dropped", groupID: 999, unknownDelta: 1, deliveredMsgs: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			delta := dropDelta(dropSiteRouteUnknownGroup)
			require.NoError(t, reg.RouteMessage(tc.groupID, raftpb.Message{Type: raftpb.MsgHeartbeat}))
			require.Equal(t, tc.unknownDelta, delta())
			require.Len(t, known.incomingMsgCh, tc.deliveredMsgs)
			for len(known.incomingMsgCh) > 0 {
				<-known.incomingMsgCh
			}
		})
	}
}

func TestEvaluateWedges(t *testing.T) {
	now := time.Now()
	const wedgeAfter = 5 * time.Second
	tests := []struct {
		name        string
		peers       []replicaProgress
		leaderMatch uint64
		prev        map[uint64]wedgeTrack
		wantWedged  []uint64
		wantSince   map[uint64]time.Time // expected clock start in next, keyed by peer
	}{
		{
			name:        "caught-up peer is never wedged",
			peers:       []replicaProgress{{id: 1, match: 100}},
			leaderMatch: 100,
			prev:        map[uint64]wedgeTrack{1: {match: 100, since: now.Add(-time.Hour)}},
			wantWedged:  nil,
			wantSince:   map[uint64]time.Time{1: now},
		},
		{
			name:        "behind peer first sighting starts the clock, no wedge",
			peers:       []replicaProgress{{id: 1, match: 40}},
			leaderMatch: 100,
			prev:        map[uint64]wedgeTrack{},
			wantWedged:  nil,
			wantSince:   map[uint64]time.Time{1: now},
		},
		{
			name:        "behind but advancing restarts the clock, no wedge",
			peers:       []replicaProgress{{id: 1, match: 50}},
			leaderMatch: 100,
			prev:        map[uint64]wedgeTrack{1: {match: 40, since: now.Add(-time.Hour)}},
			wantWedged:  nil,
			wantSince:   map[uint64]time.Time{1: now},
		},
		{
			name:        "behind and static below threshold is not yet wedged",
			peers:       []replicaProgress{{id: 1, match: 40}},
			leaderMatch: 100,
			prev:        map[uint64]wedgeTrack{1: {match: 40, since: now.Add(-wedgeAfter + time.Second)}},
			wantWedged:  nil,
		},
		{
			name:        "behind and static beyond threshold is wedged",
			peers:       []replicaProgress{{id: 1, match: 40}},
			leaderMatch: 100,
			prev:        map[uint64]wedgeTrack{1: {match: 40, since: now.Add(-wedgeAfter)}},
			wantWedged:  []uint64{1},
		},
		{
			name: "mixed cluster wedges only the stuck voter",
			peers: []replicaProgress{
				{id: 1, match: 100},
				{id: 2, match: 4},
			},
			leaderMatch: 100,
			prev: map[uint64]wedgeTrack{
				1: {match: 100, since: now.Add(-time.Hour)},
				2: {match: 4, since: now.Add(-time.Hour)},
			},
			wantWedged: []uint64{2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wedged, next := evaluateWedges(tc.peers, tc.leaderMatch, tc.prev, now, wedgeAfter)
			var got []uint64
			for _, p := range wedged {
				got = append(got, p.id)
			}
			require.Equal(t, tc.wantWedged, got)
			require.Len(t, next, len(tc.peers), "tracking state must cover every current peer")
			for id, wantSince := range tc.wantSince {
				require.Equal(t, wantSince, next[id].since, "peer %d clock", id)
			}
		})
	}
}

func TestLogLimiter(t *testing.T) {
	l := newLogLimiter(50 * time.Millisecond)
	require.True(t, l.Allow("k"), "first emission must pass")
	require.False(t, l.Allow("k"), "second emission within the interval must be suppressed")
	require.True(t, l.Allow("other"), "keys are limited independently")
	time.Sleep(60 * time.Millisecond)
	require.True(t, l.Allow("k"), "emission after the interval must pass")
}

func TestMsgClass(t *testing.T) {
	tests := []struct {
		typ  raftpb.MessageType
		want string
	}{
		{raftpb.MsgHeartbeat, "heartbeat"},
		{raftpb.MsgHeartbeatResp, "heartbeat"},
		{raftpb.MsgApp, "append"},
		{raftpb.MsgAppResp, "response"},
		{raftpb.MsgVoteResp, "response"},
		{raftpb.MsgPreVoteResp, "response"},
		{raftpb.MsgVote, "vote"},
		{raftpb.MsgPreVote, "vote"},
		{raftpb.MsgSnap, "snap"},
		{raftpb.MsgStorageAppend, "storage_local"},
		{raftpb.MsgTransferLeader, "other"},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, msgClass(tc.typ), tc.typ.String())
	}
}

// TestGroupLabelCache pins the alloc-avoidance contract: the same group ID
// yields the identical interned string.
func TestGroupLabelCache(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = groupLabel(12345)
		}()
	}
	wg.Wait()
	a, b := groupLabel(12345), groupLabel(12345)
	require.Equal(t, "12345", a)
	require.Equal(t, a, b)
}

// stubResolver returns a fixed address per node, or "" for unknown nodes.
type stubResolver struct{ addrs map[string]string }

func (r *stubResolver) NodeAddress(nodeID string) string { return r.addrs[nodeID] }

// TestMuxTransportSend_DropSiteAccounting pins exactly-once drop accounting
// on the send path: each discarded frame is counted at the most specific
// site that killed it, and nowhere else. An unknown uint64 dies at enqueue
// (no sender lane is ever created); a known node with no resolvable address
// dies on the lane writer goroutine, so that count lands asynchronously.
func TestMuxTransportSend_DropSiteAccounting(t *testing.T) {
	nodeIDs := newNodeIDMap()
	knownID := nodeIDs.register("known-but-unresolvable")

	logger := logrus.New()
	provider := &ShardAddressProvider{resolver: &stubResolver{addrs: map[string]string{}}, raftPort: 0}
	m, err := NewMuxTransport("127.0.0.1:0", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, provider, nodeIDs, nil, logger, 0)
	require.NoError(t, err)
	defer m.Close()

	tests := []struct {
		name string
		to   uint64
		site string
	}{
		{name: "destination uint64 not in the node-ID map", to: 0xdead, site: dropSitePeerResolve},
		{name: "known node with no resolvable address", to: knownID, site: dropSitePeerResolve},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolveDelta := dropDelta(tc.site)
			m.Send(9, []raftpb.Message{{Type: raftpb.MsgApp, To: tc.to}})
			require.Eventually(t, func() bool { return resolveDelta() == 1 },
				2*time.Second, 5*time.Millisecond, "the dropped frame must be counted exactly once at %s", tc.site)
		})
	}
}
