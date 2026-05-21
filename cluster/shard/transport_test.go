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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type mockResolver struct {
	addresses map[string]string
}

func (r *mockResolver) NodeAddress(nodeName string) string {
	return r.addresses[nodeName]
}

func TestShardAddressProvider_Resolution(t *testing.T) {
	resolver := &mockResolver{addresses: map[string]string{
		"node-1": "10.0.0.1",
		"node-2": "10.0.0.2",
		"node-3": "10.0.0.3",
	}}
	provider := &ShardAddressProvider{resolver: resolver, raftPort: 8301}

	addr, err := provider.Resolve("node-1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:8301", addr)

	addr, err = provider.Resolve("node-2")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:8301", addr)

	_, err = provider.Resolve("node-unknown")
	require.Error(t, err)
}

func TestShardAddressProvider_LocalCluster(t *testing.T) {
	resolver := &mockResolver{addresses: map[string]string{
		"node-1": "127.0.0.1",
		"node-2": "127.0.0.1",
		"node-3": "127.0.0.1",
	}}
	provider := &ShardAddressProvider{
		resolver:          resolver,
		raftPort:          8301,
		isLocalCluster:    true,
		nodeNameToPortMap: map[string]int{"node-1": 8301, "node-2": 8311, "node-3": 8321},
	}

	for nodeID, want := range map[string]string{
		"node-1": "127.0.0.1:8301",
		"node-2": "127.0.0.1:8311",
		"node-3": "127.0.0.1:8321",
	} {
		addr, err := provider.Resolve(nodeID)
		require.NoError(t, err)
		assert.Equal(t, want, addr)
	}
}

func TestShardAddressProvider_LocalCluster_FallbackPort(t *testing.T) {
	resolver := &mockResolver{addresses: map[string]string{
		"node-1": "127.0.0.1",
		"node-4": "127.0.0.1",
	}}
	provider := &ShardAddressProvider{
		resolver:          resolver,
		raftPort:          8301,
		isLocalCluster:    true,
		nodeNameToPortMap: map[string]int{"node-1": 8301},
	}

	// node-4 is not in the port map, should fall back to raftPort.
	addr, err := provider.Resolve("node-4")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8301", addr)
}

// captureRouter records every routed message; the test-side MessageRouter.
type captureRouter struct {
	mu   sync.Mutex
	msgs []routedMsg
}

type routedMsg struct {
	groupID uint64
	msg     raftpb.Message
}

func (c *captureRouter) RouteMessage(groupID uint64, msg raftpb.Message) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, routedMsg{groupID: groupID, msg: msg})
	c.mu.Unlock()
	return nil
}

func (c *captureRouter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func (c *captureRouter) all() []routedMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]routedMsg(nil), c.msgs...)
}

type testMuxNode struct {
	id      string
	addr    string
	mux     *MuxTransport
	router  *captureRouter
	nodeIDs *nodeIDMap
}

// setupMuxNodes binds n MuxTransports on loopback and wires each with a
// capturing router and a nodeID map pre-seeded with every node.
func setupMuxNodes(t *testing.T, n int, logger *logrus.Logger) []testMuxNode {
	t.Helper()

	nodes := make([]testMuxNode, n)
	addresses := make(map[string]string, n)
	portMap := make(map[string]int, n)

	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = ln

		nodeID := fmt.Sprintf("node-%d", i)
		addr := ln.Addr().(*net.TCPAddr)
		addresses[nodeID] = addr.IP.String()
		portMap[nodeID] = addr.Port
		nodes[i].id = nodeID
		nodes[i].addr = ln.Addr().String()
	}
	for _, ln := range listeners {
		ln.Close()
	}

	resolver := &mockResolver{addresses: addresses}

	for i := 0; i < n; i++ {
		provider := &ShardAddressProvider{
			resolver:          resolver,
			raftPort:          portMap[nodes[i].id],
			isLocalCluster:    true,
			nodeNameToPortMap: portMap,
		}
		advertise, err := net.ResolveTCPAddr("tcp", nodes[i].addr)
		require.NoError(t, err)

		nodeIDs := newNodeIDMap()
		for j := range nodes {
			nodeIDs.register(nodes[j].id)
		}
		router := &captureRouter{}

		mux, err := NewMuxTransport(nodes[i].addr, advertise, provider, nodeIDs, router, logger)
		require.NoError(t, err)

		nodes[i].mux = mux
		nodes[i].router = router
		nodes[i].nodeIDs = nodeIDs
		t.Cleanup(func() { mux.Close() })
	}

	return nodes
}

// TestMuxTransport_SendReceive verifies framed raft messages reach the peer's
// router tagged with the correct group ID.
func TestMuxTransport_SendReceive(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nodes := setupMuxNodes(t, 2, logger)

	sender := nodes[0]
	to := sender.nodeIDs.register(nodes[1].id)
	from := sender.nodeIDs.register(nodes[0].id)

	sender.mux.Send(42, []raftpb.Message{
		{Type: raftpb.MsgApp, To: to, From: from, Term: 7},
		{Type: raftpb.MsgHeartbeat, To: to, From: from, Term: 7},
	})

	require.Eventually(t, func() bool {
		return nodes[1].router.count() == 2
	}, 3*time.Second, 10*time.Millisecond, "expected both messages to be routed")

	for _, m := range nodes[1].router.all() {
		assert.Equal(t, uint64(42), m.groupID)
		assert.Equal(t, from, m.msg.From)
	}
}

// TestMuxTransport_DemuxesByGroup verifies messages for different groups over
// one peer connection arrive tagged with their own group ID.
func TestMuxTransport_DemuxesByGroup(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nodes := setupMuxNodes(t, 2, logger)

	to := nodes[0].nodeIDs.register(nodes[1].id)
	nodes[0].mux.Send(1, []raftpb.Message{{Type: raftpb.MsgApp, To: to}})
	nodes[0].mux.Send(2, []raftpb.Message{{Type: raftpb.MsgApp, To: to}})

	require.Eventually(t, func() bool {
		return nodes[1].router.count() == 2
	}, 3*time.Second, 10*time.Millisecond)

	groups := make(map[uint64]bool)
	for _, m := range nodes[1].router.all() {
		groups[m.groupID] = true
	}
	assert.True(t, groups[1], "group 1 message not routed")
	assert.True(t, groups[2], "group 2 message not routed")
}

// TestMuxTransport_Send_UnknownDestination verifies a message to an
// unresolvable node is dropped without panicking.
func TestMuxTransport_Send_UnknownDestination(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nodes := setupMuxNodes(t, 1, logger)

	assert.NotPanics(t, func() {
		nodes[0].mux.Send(1, []raftpb.Message{{Type: raftpb.MsgApp, To: 999999}})
	})
}

// TestMuxTransport_SessionReconnect verifies that closing a yamux session
// causes the next Dial to create a new one.
func TestMuxTransport_SessionReconnect(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nodes := setupMuxNodes(t, 2, logger)

	addr1 := nodes[1].mux.listener.Addr().String()
	session1, err := nodes[0].mux.getOrDialSession(addr1)
	require.NoError(t, err)
	require.False(t, session1.IsClosed())

	session1.Close()
	require.True(t, session1.IsClosed())

	session2, err := nodes[0].mux.getOrDialSession(addr1)
	require.NoError(t, err)
	require.False(t, session2.IsClosed())

	assert.NotSame(t, session1, session2)
}
