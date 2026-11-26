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

package raftauth

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type recordingFSM struct {
	mu      sync.Mutex
	applied []string
}

func (f *recordingFSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, string(l.Data))
	return nil
}

func (f *recordingFSM) Snapshot() (raft.FSMSnapshot, error) { return noopSnapshot{}, nil }

func (f *recordingFSM) Restore(rc io.ReadCloser) error { return rc.Close() }

func (f *recordingFSM) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }

func (noopSnapshot) Release() {}

type raftNode struct {
	id   raft.ServerID
	addr raft.ServerAddress
	raft *raft.Raft
	fsm  *recordingFSM
}

func startCluster(t *testing.T, kinds ...peerKind) []*raftNode {
	t.Helper()
	nodes := make([]*raftNode, len(kinds))
	transports := make([]*raft.NetworkTransport, len(kinds))
	var servers []raft.Server
	for i, kind := range kinds {
		transports[i] = newPeer(t, kind, 2*time.Second).trans
		nodes[i] = &raftNode{id: raft.ServerID(fmt.Sprintf("node-%d", i)), addr: transports[i].LocalAddr(), fsm: &recordingFSM{}}
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: nodes[i].id, Address: nodes[i].addr})
	}
	for i, n := range nodes {
		cfg := raft.DefaultConfig()
		cfg.LocalID = n.id
		cfg.HeartbeatTimeout = 300 * time.Millisecond
		cfg.ElectionTimeout = 300 * time.Millisecond
		cfg.LeaderLeaseTimeout = 150 * time.Millisecond
		cfg.CommitTimeout = 10 * time.Millisecond
		cfg.Logger = hclog.NewNullLogger()
		store := raft.NewInmemStore()
		snaps := raft.NewInmemSnapshotStore()
		require.NoError(t, raft.BootstrapCluster(cfg, store, store, snaps, transports[i], raft.Configuration{Servers: servers}))
		r, err := raft.NewRaft(cfg, n.fsm, store, store, snaps, transports[i])
		require.NoError(t, err)
		n.raft = r
		t.Cleanup(func() { require.NoError(t, r.Shutdown().Error()) })
	}
	return nodes
}

func waitLeader(t *testing.T, nodes []*raftNode) *raftNode {
	t.Helper()
	var leader *raftNode
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				leader = n
				return true
			}
		}
		return false
	}, 15*time.Second, 20*time.Millisecond)
	return leader
}

func waitReplicated(t *testing.T, nodes []*raftNode, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.fsm.count() < want {
				return false
			}
		}
		return true
	}, 15*time.Second, 20*time.Millisecond)
}

func TestRaftReplicatesOverAuthenticatedTransport(t *testing.T) {
	tests := []struct {
		name  string
		kinds []peerKind
	}{
		{name: "all verify", kinds: []peerKind{kindVerifyRight, kindVerifyRight, kindVerifyRight}},
		{name: "all send only", kinds: []peerKind{kindSendRight, kindSendRight, kindSendRight}},
		{name: "verify and send only", kinds: []peerKind{kindVerifyRight, kindSendRight, kindSendRight}},
		{name: "send only and off", kinds: []peerKind{kindSendRight, kindOff, kindOff}},
		{name: "two send only and one legacy", kinds: []peerKind{kindSendRight, kindSendRight, kindLegacy}},
		{name: "off and legacy", kinds: []peerKind{kindOff, kindOff, kindLegacy}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := startCluster(t, tt.kinds...)
			applied := 0
			for _, target := range nodes {
				leader := waitLeader(t, nodes)
				if leader != target {
					require.NoError(t, leader.raft.LeadershipTransferToServer(target.id, target.addr).Error())
					require.Eventually(t, func() bool { return target.raft.State() == raft.Leader }, 15*time.Second, 20*time.Millisecond)
				}
				require.NoError(t, target.raft.Apply([]byte(fmt.Sprintf("from-%s", target.id)), 5*time.Second).Error())
				applied++
				waitReplicated(t, nodes, applied)
			}
		})
	}
}

func TestRaftVerifyingClusterExcludesUnauthenticatedMember(t *testing.T) {
	nodes := startCluster(t, kindVerifyRight, kindVerifyRight, kindLegacy)
	leader := waitLeader(t, nodes)
	require.NotEqual(t, nodes[2], leader)
	require.NoError(t, leader.raft.Apply([]byte("x"), 5*time.Second).Error())
	waitReplicated(t, nodes[:2], 1)
	require.Never(t, func() bool { return nodes[2].fsm.count() > 0 }, time.Second, 50*time.Millisecond)
}
