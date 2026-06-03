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

// TestShardReplicationFSM_FilterReplicas_ByState: read/write filter
// contract across a COPY lifecycle. The target receives no direct writes while
// it catches up (HYDRATING/FINALIZING) — the change-capture-log is the sole,
// ordered, LWW-safe catchup path — and is promoted to a counted read+write
// replica only at INTEGRATING.
func TestShardReplicationFSM_FilterReplicas_ByState(t *testing.T) {
	const (
		class  = "TestClass"
		shard  = "shard1"
		source = "node1"
		target = "node2"
	)
	replicas := []string{source, target}

	cases := []struct {
		name           string
		state          api.ShardReplicationState
		wantRead       []string
		wantWrite      []string
		wantAdditional []string
	}{
		{
			name:           "REGISTERED: target not yet routable",
			state:          api.REGISTERED,
			wantRead:       []string{source},
			wantWrite:      []string{source},
			wantAdditional: []string{},
		},
		{
			name:           "HYDRATING: target not yet routable",
			state:          api.HYDRATING,
			wantRead:       []string{source},
			wantWrite:      []string{source},
			wantAdditional: []string{},
		},
		{
			name:           "FINALIZING: target receives no direct writes (CCL-only catchup)",
			state:          api.FINALIZING,
			wantRead:       []string{source},
			wantWrite:      []string{source},
			wantAdditional: []string{},
		},
		{
			name:           "INTEGRATING: target is a counted read+write replica, not additional",
			state:          api.INTEGRATING,
			wantRead:       []string{source, target},
			wantWrite:      []string{source, target},
			wantAdditional: []string{},
		},
		{
			name:           "READY: target fully promoted",
			state:          api.READY,
			wantRead:       []string{source, target},
			wantWrite:      []string{source, target},
			wantAdditional: []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOp(t, fsm, 1)
			driveToState(t, fsm, 1, tc.state)

			gotRead := fsm.FilterOneShardReplicasRead(class, shard, replicas)
			assert.ElementsMatch(t, tc.wantRead, gotRead, "read replicas")

			gotWrite, gotAdditional := fsm.FilterOneShardReplicasWrite(class, shard, replicas)
			assert.ElementsMatch(t, tc.wantWrite, gotWrite, "write replicas")
			assert.ElementsMatch(t, tc.wantAdditional, gotAdditional, "additional write replicas")
		})
	}
}
