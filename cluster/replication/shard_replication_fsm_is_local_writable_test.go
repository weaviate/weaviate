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

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

const (
	srcNode   = "node1"
	tgtNode   = "node2"
	otherNode = "node3"
	testClass = "TestClass"
	testShard = "shard1"
)

// TestShardReplicationFSM_IsLocalShardWritable_MOVE: MOVE/DEHYDRATING on
// source rejects unconditionally; targets and uninvolved nodes always pass.
func TestShardReplicationFSM_IsLocalShardWritable_MOVE(t *testing.T) {
	cases := []struct {
		name        string
		state       api.ShardReplicationState
		node        string
		wantAllowed bool
	}{
		{"source writable in REGISTERED", api.REGISTERED, srcNode, true},
		{"source writable in HYDRATING", api.HYDRATING, srcNode, true},
		{"source writable in FINALIZING", api.FINALIZING, srcNode, true},
		{"source NOT writable in DEHYDRATING", api.DEHYDRATING, srcNode, false},
		{"source writable in READY", api.READY, srcNode, true},

		// Target-side: ALWAYS writable. The fence only fires on the source.
		{"target writable in REGISTERED", api.REGISTERED, tgtNode, true},
		{"target writable in DEHYDRATING", api.DEHYDRATING, tgtNode, true},
		{"target writable in READY", api.READY, tgtNode, true},

		// Uninvolved node: always writable.
		{"unrelated node writable in HYDRATING", api.HYDRATING, otherNode, true},
		{"unrelated node writable in DEHYDRATING", api.DEHYDRATING, otherNode, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOpOfType(t, fsm, 1, api.MOVE)
			driveToState(t, fsm, 1, tc.state)

			// schemaVersion is irrelevant for MOVE — the rejection is unconditional.
			allowed, _ := fsm.IsLocalShardWritable(tc.node, testClass, testShard, 0)
			assert.Equal(t, tc.wantAllowed, allowed)
		})
	}
}

// TestShardReplicationFSM_IsLocalShardWritable_VersionGated: INTEGRATING/READY
// on source rejects when schemaVersion < AddReplicaVersion, with catchUp set
// to AddReplicaVersion. Parametrised over TransferType — the rule is
// state-keyed, not TT-keyed.
func TestShardReplicationFSM_IsLocalShardWritable_VersionGated(t *testing.T) {
	cases := []struct {
		name              string
		transferType      api.ShardReplicationTransferType
		state             api.ShardReplicationState
		addReplicaVersion uint64
		schemaVersion     uint64
		node              string
		wantAllowed       bool
		wantCatchUp       uint64
	}{
		// Pre-INTEGRATING states: no fence (target not yet a routable write replica).
		{"COPY: source allowed REGISTERED any sv", api.COPY, api.REGISTERED, 42, 0, srcNode, true, 0},
		{"COPY: source allowed HYDRATING any sv", api.COPY, api.HYDRATING, 42, 0, srcNode, true, 0},
		{"COPY: source allowed FINALIZING any sv", api.COPY, api.FINALIZING, 42, 0, srcNode, true, 0},

		// INTEGRATING: version-gated. Same rule for both TTs.
		{"COPY: INTEGRATING reject sv<floor", api.COPY, api.INTEGRATING, 42, 41, srcNode, false, 42},
		{"COPY: INTEGRATING reject sv=0", api.COPY, api.INTEGRATING, 42, 0, srcNode, false, 42},
		{"COPY: INTEGRATING allow sv=floor", api.COPY, api.INTEGRATING, 42, 42, srcNode, true, 0},
		{"COPY: INTEGRATING allow sv>floor", api.COPY, api.INTEGRATING, 42, 100, srcNode, true, 0},
		{"MOVE: INTEGRATING reject sv<floor", api.MOVE, api.INTEGRATING, 42, 41, srcNode, false, 42},
		{"MOVE: INTEGRATING allow sv>=floor", api.MOVE, api.INTEGRATING, 42, 42, srcNode, true, 0},

		// READY: same version gate.
		{"COPY: READY reject sv<floor", api.COPY, api.READY, 42, 41, srcNode, false, 42},
		{"COPY: READY allow sv>=floor", api.COPY, api.READY, 42, 42, srcNode, true, 0},

		// AddReplicaVersion == 0 → fence disabled (pre-upgrade snapshot case).
		{"COPY: INTEGRATING allow when AddReplicaVersion=0", api.COPY, api.INTEGRATING, 0, 0, srcNode, true, 0},
		{"MOVE: INTEGRATING allow when AddReplicaVersion=0", api.MOVE, api.INTEGRATING, 0, 0, srcNode, true, 0},

		// Target-side: ALWAYS allowed — fence only fires on the source.
		{"COPY: target allow INTEGRATING any sv", api.COPY, api.INTEGRATING, 42, 0, tgtNode, true, 0},
		{"MOVE: target allow INTEGRATING any sv", api.MOVE, api.INTEGRATING, 42, 0, tgtNode, true, 0},

		// Uninvolved node: always allowed.
		{"unrelated allow INTEGRATING any sv", api.COPY, api.INTEGRATING, 42, 0, otherNode, true, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOpOfType(t, fsm, 1, tc.transferType)
			if tc.addReplicaVersion != 0 {
				require.NoError(t, fsm.SetAddReplicaVersion(1, tc.addReplicaVersion))
			}
			driveToState(t, fsm, 1, tc.state)

			allowed, catchUp := fsm.IsLocalShardWritable(tc.node, testClass, testShard, tc.schemaVersion)
			assert.Equal(t, tc.wantAllowed, allowed, "allowed")
			assert.Equal(t, tc.wantCatchUp, catchUp, "catchUpIndex")
		})
	}
}

// TestShardReplicationFSM_IsLocalShardWritable_NoOp: no op on the shard
// means the fence passes for any node regardless of schemaVersion.
func TestShardReplicationFSM_IsLocalShardWritable_NoOp(t *testing.T) {
	fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	allowed, catchUp := fsm.IsLocalShardWritable("anyNode", "AnyClass", "anyShard", 100)
	assert.True(t, allowed)
	assert.Equal(t, uint64(0), catchUp)
}

// TestShardReplicationFSM_IsLocalShardWritable_MaxAcrossOps: on multiple
// qualifying ops the highest AddReplicaVersion wins as catchUpIndex.
func TestShardReplicationFSM_IsLocalShardWritable_MaxAcrossOps(t *testing.T) {
	fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	seedOpOfType(t, fsm, 1, api.COPY)
	uuid2 := strfmt.UUID("00000000-0000-0000-0000-000000000002")
	require.NoError(t, fsm.Replicate(2, &api.ReplicationReplicateShardRequest{
		Version:          api.ReplicationCommandVersionV0,
		Uuid:             uuid2,
		SourceNode:       srcNode,
		SourceCollection: testClass,
		SourceShard:      testShard,
		TargetNode:       "node-extra",
		TransferType:     api.COPY.String(),
	}))
	require.NoError(t, fsm.SetAddReplicaVersion(1, 30))
	require.NoError(t, fsm.SetAddReplicaVersion(2, 42))
	driveToState(t, fsm, 1, api.INTEGRATING)
	driveToState(t, fsm, 2, api.INTEGRATING)

	// schemaVersion 25 < both addReplicaVersions → reject with the higher (42).
	allowed, catchUp := fsm.IsLocalShardWritable(srcNode, testClass, testShard, 25)
	assert.False(t, allowed)
	assert.Equal(t, uint64(42), catchUp)

	// schemaVersion 35 < 42 but >= 30 → still reject (because of op 2).
	allowed, catchUp = fsm.IsLocalShardWritable(srcNode, testClass, testShard, 35)
	assert.False(t, allowed)
	assert.Equal(t, uint64(42), catchUp)

	// schemaVersion 42 >= both → allow.
	allowed, catchUp = fsm.IsLocalShardWritable(srcNode, testClass, testShard, 42)
	assert.True(t, allowed)
	assert.Equal(t, uint64(0), catchUp)
}
