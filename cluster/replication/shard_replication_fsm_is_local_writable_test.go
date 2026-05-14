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

package replication

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

// TestShardReplicationFSM_IsLocalShardWritable pins the source-side
// rejection contract: once an op for which the local node is the source
// has reached DEHYDRATING, the local FSM stops reporting it as a write
// target. PREPARE handlers consult this to refuse stale-routed writes
// with StatusRouteStale, forcing the coordinator to refresh routing.
func TestShardReplicationFSM_IsLocalShardWritable(t *testing.T) {
	const (
		sourceNode = "node1"
		targetNode = "node2"
		otherNode  = "node3"
		class      = "TestClass"
		shard      = "shard1"
	)

	cases := []struct {
		name        string
		state       api.ShardReplicationState
		node        string
		wantWriteOK bool
	}{
		// Source-side: writable in early lifecycle, fenced from DEHYDRATING.
		{"source writable in REGISTERED", api.REGISTERED, sourceNode, true},
		{"source writable in HYDRATING", api.HYDRATING, sourceNode, true},
		{"source writable in FINALIZING", api.FINALIZING, sourceNode, true},
		{"source NOT writable in DEHYDRATING", api.DEHYDRATING, sourceNode, false},
		{"source writable in READY", api.READY, sourceNode, true},

		// Target-side: ALWAYS writable. The fence only fires for sources that
		// have reached DEHYDRATING. A coord routing a PREPARE to a target has
		// a view of the world ahead of the target's own FSM; the storage
		// layer is the right place to surface "shard not ready", not this
		// FSM filter (which is only consulted as a write-rejection fence).
		{"target writable in REGISTERED", api.REGISTERED, targetNode, true},
		{"target writable in HYDRATING", api.HYDRATING, targetNode, true},
		{"target writable in FINALIZING", api.FINALIZING, targetNode, true},
		{"target writable in DEHYDRATING", api.DEHYDRATING, targetNode, true},
		{"target writable in READY", api.READY, targetNode, true},

		// Uninvolved node: always writable regardless of op state.
		{"unrelated node writable in HYDRATING", api.HYDRATING, otherNode, true},
		{"unrelated node writable in DEHYDRATING", api.DEHYDRATING, otherNode, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOp(t, fsm, 1)
			driveToState(t, fsm, 1, tc.state)

			got := fsm.IsLocalShardWritable(tc.node, class, shard)
			assert.Equal(t, tc.wantWriteOK, got)
		})
	}
}

// TestShardReplicationFSM_IsLocalShardWritable_NoOp guarantees the
// happy path: with no replication op touching (class, shard), every
// node passes the source-side fence.
func TestShardReplicationFSM_IsLocalShardWritable_NoOp(t *testing.T) {
	fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	assert.True(t, fsm.IsLocalShardWritable("anyNode", "AnyClass", "anyShard"))
}
