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
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardKeyHeader_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"simple", "MyClass/shard1"},
		{"long_class", "VeryLongClassName/shard-abc-123"},
		{"unicode", "TestClass/shard-\u00e9\u00e8\u00ea"},
		{"single_char", "A/B"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := writeShardKeyHeader(&buf, tc.key)
			require.NoError(t, err)

			got, err := readShardKeyHeader(&buf)
			require.NoError(t, err)
			assert.Equal(t, tc.key, got)
		})
	}
}

func TestShardKeyHeader_EmptyKey(t *testing.T) {
	var buf bytes.Buffer
	// Write a zero-length header manually
	buf.Write([]byte{0, 0})
	_, err := readShardKeyHeader(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty shard key")
}

func TestShardAddressProvider_Resolution(t *testing.T) {
	resolver := &mockResolver{
		addresses: map[string]string{
			"node-1": "10.0.0.1",
			"node-2": "10.0.0.2",
			"node-3": "10.0.0.3",
		},
	}

	provider := &ShardAddressProvider{
		resolver: resolver,
		raftPort: 8301,
	}

	addr, err := provider.ServerAddr("node-1")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("10.0.0.1:8301"), addr)

	addr, err = provider.ServerAddr("node-2")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("10.0.0.2:8301"), addr)

	// Unknown node
	_, err = provider.ServerAddr("node-unknown")
	require.Error(t, err)
}

func TestShardAddressProvider_LocalCluster(t *testing.T) {
	resolver := &mockResolver{
		addresses: map[string]string{
			"node-1": "127.0.0.1",
			"node-2": "127.0.0.1",
			"node-3": "127.0.0.1",
		},
	}

	provider := &ShardAddressProvider{
		resolver:       resolver,
		raftPort:       8301,
		isLocalCluster: true,
		nodeNameToPortMap: map[string]int{
			"node-1": 8301,
			"node-2": 8311,
			"node-3": 8321,
		},
	}

	addr, err := provider.ServerAddr("node-1")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("127.0.0.1:8301"), addr)

	addr, err = provider.ServerAddr("node-2")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("127.0.0.1:8311"), addr)

	addr, err = provider.ServerAddr("node-3")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("127.0.0.1:8321"), addr)
}

func TestShardAddressProvider_LocalCluster_FallbackPort(t *testing.T) {
	resolver := &mockResolver{
		addresses: map[string]string{
			"node-1": "127.0.0.1",
			"node-4": "127.0.0.1",
		},
	}

	provider := &ShardAddressProvider{
		resolver:       resolver,
		raftPort:       8301,
		isLocalCluster: true,
		nodeNameToPortMap: map[string]int{
			"node-1": 8301,
		},
	}

	// node-4 is not in the port map, should fall back to raftPort
	addr, err := provider.ServerAddr("node-4")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("127.0.0.1:8301"), addr)
}

// TestMuxTransport_SingleShard_ThreeNodes creates 3 MuxTransport instances,
// registers one shard on each, forms a RAFT cluster, and verifies leader election.
func TestMuxTransport_SingleShard_ThreeNodes(t *testing.T) {
	logger, _ := test.NewNullLogger()

	nodes := setupMuxNodes(t, 3, logger)

	className := "TestClass"
	shardName := "shard1"

	// Create shard transport on each node
	transports := make([]raft.Transport, 3)
	for i, n := range nodes {
		tr, err := n.mux.CreateShardTransport(className, shardName, logger)
		require.NoError(t, err)
		transports[i] = tr
	}

	// Build RAFT cluster
	stores := startRaftCluster(t, nodes, transports, className, shardName, logger)
	defer stopStores(stores)

	// Wait for leader election
	leader := waitForLeader(t, stores, 10*time.Second)
	require.NotNil(t, leader, "expected a leader to be elected")
}

// TestMuxTransport_MultipleShards_SharedConnections creates 2 MuxTransport
// instances, registers 10 shards on each, and verifies all 10 RAFT clusters
// elect leaders (validates multiplexing).
func TestMuxTransport_MultipleShards_SharedConnections(t *testing.T) {
	logger, _ := test.NewNullLogger()

	nodes := setupMuxNodes(t, 2, logger)

	className := "TestClass"
	numShards := 10

	allStores := make([][]*Store, numShards)

	for s := 0; s < numShards; s++ {
		shardName := fmt.Sprintf("shard-%d", s)

		transports := make([]raft.Transport, 2)
		for i, n := range nodes {
			tr, err := n.mux.CreateShardTransport(className, shardName, logger)
			require.NoError(t, err)
			transports[i] = tr
		}

		stores := startRaftCluster(t, nodes, transports, className, shardName, logger)
		allStores[s] = stores
	}

	defer func() {
		for _, stores := range allStores {
			stopStores(stores)
		}
	}()

	// Verify each shard cluster elects a leader
	for s := 0; s < numShards; s++ {
		leader := waitForLeader(t, allStores[s], 10*time.Second)
		require.NotNil(t, leader, "shard-%d: expected a leader to be elected", s)
	}
}

// TestMuxTransport_SessionReconnect verifies that closing a yamux session
// causes the next Dial to create a new one.
func TestMuxTransport_SessionReconnect(t *testing.T) {
	logger, _ := test.NewNullLogger()

	nodes := setupMuxNodes(t, 2, logger)

	// Dial from node 0 to node 1
	addr1 := raft.ServerAddress(nodes[1].mux.listener.Addr().String())
	session1, err := nodes[0].mux.getOrDialSession(addr1)
	require.NoError(t, err)
	require.False(t, session1.IsClosed())

	// Close the session
	session1.Close()
	require.True(t, session1.IsClosed())

	// Next dial should create a new session
	session2, err := nodes[0].mux.getOrDialSession(addr1)
	require.NoError(t, err)
	require.False(t, session2.IsClosed())

	// Should be a different session
	assert.NotSame(t, session1, session2)
}

// --- Test helpers ---

type testMuxNode struct {
	id   string
	mux  *MuxTransport
	addr string
}

type mockResolver struct {
	addresses map[string]string
}

func (r *mockResolver) NodeAddress(nodeName string) string {
	return r.addresses[nodeName]
}

func setupMuxNodes(t *testing.T, n int, logger *logrus.Logger) []testMuxNode {
	t.Helper()

	// Create listeners on random ports
	nodes := make([]testMuxNode, n)
	addresses := make(map[string]string, n)
	portMap := make(map[string]int, n)

	// First pass: create listeners to get ports
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

	// Close temp listeners so MuxTransport can bind
	for _, ln := range listeners {
		ln.Close()
	}

	resolver := &mockResolver{addresses: addresses}

	// Second pass: create MuxTransports on the same ports
	for i := 0; i < n; i++ {
		provider := &ShardAddressProvider{
			resolver:          resolver,
			raftPort:          portMap[nodes[i].id],
			isLocalCluster:    true,
			nodeNameToPortMap: portMap,
		}

		advertise, err := net.ResolveTCPAddr("tcp", nodes[i].addr)
		require.NoError(t, err)

		mux, err := NewMuxTransport(nodes[i].addr, advertise, provider, logger)
		require.NoError(t, err)
		nodes[i].mux = mux

		t.Cleanup(func() { mux.Close() })
	}

	return nodes
}

func startRaftCluster(
	t *testing.T,
	nodes []testMuxNode,
	transports []raft.Transport,
	className, shardName string,
	logger *logrus.Logger,
) []*Store {
	t.Helper()

	members := make([]string, len(nodes))
	for i, n := range nodes {
		members[i] = n.id
	}

	stores := make([]*Store, len(nodes))
	for i, n := range nodes {
		cfg := StoreConfig{
			ClassName:          className,
			ShardName:          shardName,
			NodeID:             n.id,
			DataPath:           t.TempDir(),
			Members:            members,
			Logger:             logger,
			Transport:          transports[i],
			HeartbeatTimeout:   150 * time.Millisecond,
			ElectionTimeout:    150 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			SnapshotInterval:   10 * time.Second,
			SnapshotThreshold:  1024,
		}

		store, err := NewStore(cfg)
		require.NoError(t, err)

		// FSM is created with nil shard, which is fine for leader election
		// tests that don't apply commands.
		ctx := context.Background()
		err = store.Start(ctx)
		require.NoError(t, err)

		stores[i] = store
	}

	return stores
}

func stopStores(stores []*Store) {
	for _, s := range stores {
		if s != nil {
			s.Stop()
		}
	}
}

func waitForLeader(t *testing.T, stores []*Store, timeout time.Duration) *Store {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for leader election")
			return nil
		case <-ticker.C:
			for _, s := range stores {
				if s.IsLeader() {
					return s
				}
			}
		}
	}
}
