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

package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// TestNotifyConcurrentCandidates hammers Notify the way concurrent NotifyPeer
// RPC handlers do during cluster bootstrap; run with -race.
func TestNotifyConcurrentCandidates(t *testing.T) {
	ms := NewMockStore(t, "N1", 9526)
	st := ms.Store(nil)
	st.cfg.BootstrapExpect = 1_000_000 // never reach the bootstrap threshold
	st.open.Store(true)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				require.NoError(t, st.Notify(fmt.Sprintf("node-%d-%d", g, j), "10.0.0.1:8300"))
			}
		}(g)
	}
	wg.Wait()
}

// TestNotifyConcurrentBootstrapOnce races notifies across the bootstrap threshold; run with -race.
func TestNotifyConcurrentBootstrapOnce(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "N1", utils.MustGetFreeTCPPort())
	hook := logrustest.NewLocal(m.logger)
	st := m.Store(nil)
	st.cfg.BootstrapExpect = 3
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	srv := NewRaft(mocks.NewMockNodeSelector(), st, nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	defer srv.Close(ctx)

	require.NoError(t, st.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.NoError(t, st.Notify("seed", "10.1.0.1:8300"))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				require.NoError(t, st.Notify(fmt.Sprintf("node-%d-%d", g, j), fmt.Sprintf("10.0.%d.%d:8300", g, j)))
			}
		}(g)
	}
	close(start)
	wg.Wait()

	bootstraps := 0
	for _, e := range hook.AllEntries() {
		if e.Message == "starting cluster bootstrapping" {
			bootstraps++
		}
	}
	require.Equal(t, 1, bootstraps)
	require.True(t, st.bootstrapped.Load())
	require.Zero(t, st.candidatesLen())
}

// raftHeartbeatTimeout is the raft default times the default RAFT_TIMEOUTS_MULTIPLIER:
// an election that waits for it cannot start within 5s.
const raftHeartbeatTimeout = 5 * time.Second

func openRaftForElectionTest(t *testing.T, m *MockStore) *Raft {
	t.Helper()
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
	s.schemaManager.SetReplicationFSM(m.replicationFSM)
	m.store = &s
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	require.NoError(t, srv.Open(context.Background(), m.indexer))
	return srv
}

func newElectionTestStore(t *testing.T) MockStore {
	t.Helper()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	m.cfg.HeartbeatTimeout = raftHeartbeatTimeout
	m.cfg.ElectionTimeout = raftHeartbeatTimeout
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	return m
}

// TestSoleVoterElectsWithoutWaitingForHeartbeatTimeout pins that a node that
// is the only voter elects itself at once, on a first start and on a restart,
// and keeps its configured heartbeat timeout.
func TestSoleVoterElectsWithoutWaitingForHeartbeatTimeout(t *testing.T) {
	ctx := context.Background()
	m := newElectionTestStore(t)
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)

	requireLeaderBeforeHeartbeatTimeout := func(srv *Raft) {
		t.Helper()
		require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, srv.store.IsLeader),
			"node did not become leader within 2s")
		require.Equal(t, raftHeartbeatTimeout, srv.store.raft.Load().ReloadableConfig().HeartbeatTimeout)
	}

	// first start: the single-node bootstrap goes through Notify
	srv := openRaftForElectionTest(t, &m)
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))
	requireLeaderBeforeHeartbeatTimeout(srv)
	require.NoError(t, srv.Close(ctx))

	// restart: the configuration is read back from the raft log
	srv = openRaftForElectionTest(t, &m)
	defer srv.Close(ctx)
	requireLeaderBeforeHeartbeatTimeout(srv)
}

// TestSharedVoteKeepsHeartbeatTimeout pins that a node that shares the vote
// with other voters does not start an election before its heartbeat timeout.
func TestSharedVoteKeepsHeartbeatTimeout(t *testing.T) {
	ctx := context.Background()
	m := newElectionTestStore(t)
	m.cfg.BootstrapExpect = 3
	srv := openRaftForElectionTest(t, &m)
	defer srv.Close(ctx)

	require.NoError(t, srv.store.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.NoError(t, srv.store.Notify("Node-2", "127.0.0.1:1"))
	require.NoError(t, srv.store.Notify("Node-3", "127.0.0.1:2"))
	require.True(t, srv.store.bootstrapped.Load())

	rn := srv.store.raft.Load()
	assert.False(t, tryNTimesWithWait(100, 10*time.Millisecond, func() bool {
		return rn.State() != raft.Follower
	}), "node started an election before its heartbeat timeout")
	assert.Equal(t, raftHeartbeatTimeout, rn.ReloadableConfig().HeartbeatTimeout)
}

func TestIsSoleVoter(t *testing.T) {
	voter := func(id string) raft.Server { return raft.Server{ID: raft.ServerID(id), Suffrage: raft.Voter} }
	nonvoter := func(id string) raft.Server { return raft.Server{ID: raft.ServerID(id), Suffrage: raft.Nonvoter} }

	tests := []struct {
		name    string
		servers []raft.Server
		want    bool
	}{
		{name: "empty configuration", servers: nil, want: false},
		{name: "only voter", servers: []raft.Server{voter("self")}, want: true},
		{name: "only voter with non-voters", servers: []raft.Server{voter("self"), nonvoter("b")}, want: true},
		{name: "another node is the only voter", servers: []raft.Server{voter("b")}, want: false},
		{name: "non-voter beside the only voter", servers: []raft.Server{nonvoter("self"), voter("b")}, want: false},
		{name: "shared vote", servers: []raft.Server{voter("self"), voter("b"), voter("c")}, want: false},
		{name: "staging", servers: []raft.Server{voter("self"), {ID: "b", Suffrage: raft.Staging}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSoleVoter(raft.Configuration{Servers: tt.servers}, "self"))
		})
	}
}
