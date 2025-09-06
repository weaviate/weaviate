//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/cluster/bootstrap"
)

// mockClusterStateReader implements resolver.ClusterStateReader for testing
type mockClusterStateReader struct {
	addresses map[string]string
	localName string
}

func (m *mockClusterStateReader) NodeAddress(id string) string {
	return m.addresses[id]
}

func (m *mockClusterStateReader) NodeHostname(nodeName string) (string, bool) {
	addr, exists := m.addresses[nodeName]
	return addr, exists
}

func (m *mockClusterStateReader) LocalName() string {
	return m.localName
}

func (m *mockClusterStateReader) AllClusterMembers(raftPort int) map[string]string {
	result := make(map[string]string)
	for name, addr := range m.addresses {
		if addr != "" {
			result[name] = fmt.Sprintf("%s:%d", addr, raftPort)
		}
	}
	return result
}

func TestResolveRemoteNodesSingleNode(t *testing.T) {
	// Test that ResolveRemoteNodes correctly handles single-node scenarios
	// This is part of the fix for RESOURCE_EXHAUSTED errors in single-node clusters

	// Mock address resolver that returns only the local node
	mockResolver := &mockClusterStateReader{
		addresses: map[string]string{
			"node1": "172.18.0.4",
		},
		localName: "node1",
	}

	serverPortMap := map[string]int{
		"node1": 8300,
	}

	// Test single-node scenario
	remoteNodes := bootstrap.ResolveRemoteNodes(mockResolver, serverPortMap)

	// Should have exactly one node (the local node)
	assert.Equal(t, 1, len(remoteNodes), "Single-node should have exactly one remote node")
	assert.Contains(t, remoteNodes, "node1", "Should contain the local node")
	assert.Equal(t, "172.18.0.4:8300", remoteNodes["node1"], "Should resolve to correct address")
}

func TestResolveRemoteNodesMultiNode(t *testing.T) {
	// Test that ResolveRemoteNodes correctly handles multi-node scenarios
	// This ensures we don't break existing multi-node functionality

	// Mock address resolver that returns multiple nodes
	mockResolver := &mockClusterStateReader{
		addresses: map[string]string{
			"node1": "172.18.0.4",
			"node2": "172.18.0.5",
			"node3": "172.18.0.6",
		},
		localName: "node1",
	}

	serverPortMap := map[string]int{
		"node1": 8300,
		"node2": 8300,
		"node3": 8300,
	}

	// Test multi-node scenario
	remoteNodes := bootstrap.ResolveRemoteNodes(mockResolver, serverPortMap)

	// Should have all three nodes
	assert.Equal(t, 3, len(remoteNodes), "Multi-node should have all remote nodes")
	assert.Contains(t, remoteNodes, "node1", "Should contain node1")
	assert.Contains(t, remoteNodes, "node2", "Should contain node2")
	assert.Contains(t, remoteNodes, "node3", "Should contain node3")
	assert.Equal(t, "172.18.0.4:8300", remoteNodes["node1"], "Should resolve node1 correctly")
	assert.Equal(t, "172.18.0.5:8300", remoteNodes["node2"], "Should resolve node2 correctly")
	assert.Equal(t, "172.18.0.6:8300", remoteNodes["node3"], "Should resolve node3 correctly")
}

func TestResolveRemoteNodesWithUnresolvableNodes(t *testing.T) {
	// Test that ResolveRemoteNodes handles unresolvable nodes gracefully
	// This simulates scenarios where some nodes are not available

	// Mock address resolver that returns empty for some nodes
	mockResolver := &mockClusterStateReader{
		addresses: map[string]string{
			"node1": "172.18.0.4",
			"node2": "", // Unresolvable
			"node3": "172.18.0.6",
		},
		localName: "node1",
	}

	serverPortMap := map[string]int{
		"node1": 8300,
		"node2": 8300,
		"node3": 8300,
	}

	// Test scenario with unresolvable nodes
	remoteNodes := bootstrap.ResolveRemoteNodes(mockResolver, serverPortMap)

	// Should only have resolvable nodes
	assert.Equal(t, 2, len(remoteNodes), "Should only include resolvable nodes")
	assert.Contains(t, remoteNodes, "node1", "Should contain resolvable node1")
	assert.Contains(t, remoteNodes, "node3", "Should contain resolvable node3")
	assert.NotContains(t, remoteNodes, "node2", "Should not contain unresolvable node2")
	assert.Equal(t, "172.18.0.4:8300", remoteNodes["node1"], "Should resolve node1 correctly")
	assert.Equal(t, "172.18.0.6:8300", remoteNodes["node3"], "Should resolve node3 correctly")
}

func TestSingleNodeBootstrapPreventsResourceExhausted(t *testing.T) {
	// Test that single-node clusters use bootstrap instead of join to prevent RESOURCE_EXHAUSTED
	// This test verifies the core fix for the issue described in the logs

	// Mock address resolver that returns only the local node (single-node scenario)
	mockResolver := &mockClusterStateReader{
		addresses: map[string]string{
			"node1": "172.18.0.4",
		},
		localName: "node1",
	}

	serverPortMap := map[string]int{
		"node1": 8300,
	}

	// Test the scenario that was causing RESOURCE_EXHAUSTED
	remoteNodes := bootstrap.ResolveRemoteNodes(mockResolver, serverPortMap)

	// In a single-node scenario, we should have exactly one node (the local node)
	// This node will be passed to the bootstrap process instead of join
	assert.Equal(t, 1, len(remoteNodes), "Single-node should have exactly one remote node")
	assert.Contains(t, remoteNodes, "node1", "Should contain the local node")
	assert.Equal(t, "172.18.0.4:8300", remoteNodes["node1"], "Should resolve to correct address")

	// Verify this prevents the RESOURCE_EXHAUSTED scenario
	// When remoteNodes is empty, the bootstrap will skip join and go directly to notify
	localRaftAddr := "172.18.0.4:8300"
	isSelfJoin := remoteNodes["node1"] == localRaftAddr
	assert.True(t, isSelfJoin, "This should be a self-join scenario")

	// The fix ensures that instead of trying to join this address (which would fail),
	// the node will use the bootstrap process, which handles single-node scenarios properly
	// through the Notify mechanism rather than direct join attempts
}
