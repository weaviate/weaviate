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

package replication_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

func TestFSMRewindVoidsPeerConvergence(t *testing.T) {
	const opID = uint64(5)
	peers := []string{"node1", "node2", "node3"}
	type step struct {
		state  api.ShardReplicationState
		report api.ShardReplicationState
		round  uint64
	}
	advance := func(states ...api.ShardReplicationState) []step {
		steps := make([]step, 0, len(states))
		for _, s := range states {
			steps = append(steps, step{state: s})
		}
		return steps
	}
	reportAll := func(state api.ShardReplicationState, round uint64) []step {
		return []step{{report: state, round: round}}
	}
	concat := func(parts ...[]step) []step {
		var out []step
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	toIntegrating := advance(api.HYDRATING, api.FINALIZING, api.INTEGRATING)
	tests := []struct {
		name        string
		steps       []step
		wantAtLeast bool
		wantRewinds uint64
	}{
		{
			name:        "reports without a rewind converge",
			steps:       concat(toIntegrating, reportAll(api.INTEGRATING, 1)),
			wantAtLeast: true,
		},
		{
			name:        "older senders converge without a rewind",
			steps:       concat(toIntegrating, reportAll(api.INTEGRATING, 0)),
			wantAtLeast: true,
		},
		{
			name:        "a rewind voids the reports of the previous round",
			steps:       concat(toIntegrating, reportAll(api.INTEGRATING, 1), advance(api.HYDRATING)),
			wantRewinds: 1,
		},
		{
			name: "a stale-round report after the re-advance is dropped",
			steps: concat(toIntegrating, reportAll(api.INTEGRATING, 1), advance(api.HYDRATING),
				toIntegrating, reportAll(api.INTEGRATING, 1)),
			wantRewinds: 1,
		},
		{
			name: "a current-round report after the re-advance converges",
			steps: concat(toIntegrating, reportAll(api.INTEGRATING, 1), advance(api.HYDRATING),
				toIntegrating, reportAll(api.INTEGRATING, 2)),
			wantAtLeast: true,
			wantRewinds: 1,
		},
		{
			name: "an older sender's report ahead of a rewound op is dropped",
			steps: concat(toIntegrating, reportAll(api.INTEGRATING, 0), advance(api.HYDRATING),
				reportAll(api.INTEGRATING, 0), advance(api.FINALIZING, api.INTEGRATING)),
			wantRewinds: 1,
		},
		{
			name: "an older sender's report is kept once the rewound op catches up",
			steps: concat(toIntegrating, reportAll(api.INTEGRATING, 0), advance(api.HYDRATING),
				toIntegrating, reportAll(api.INTEGRATING, 0)),
			wantAtLeast: true,
			wantRewinds: 1,
		},
		{
			name:        "a forward transition is no rewind",
			steps:       concat(toIntegrating, reportAll(api.INTEGRATING, 1), advance(api.DEHYDRATING)),
			wantAtLeast: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOp(t, fsm, opID)
			for _, s := range tc.steps {
				if s.state != "" {
					require.NoError(t, fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{Id: opID, State: s.state}))
					continue
				}
				for _, peer := range peers {
					require.NoError(t, fsm.NodeReachedState(&api.ReplicationNodeReachedStateRequest{
						Id: opID, NodeId: peer, State: s.report, Round: s.round,
					}))
				}
			}
			require.Equal(t, tc.wantAtLeast, fsm.AllPeersAtLeast(opID, api.INTEGRATING, peers))
			op, ok := fsm.GetOpById(opID)
			require.True(t, ok)
			require.Equal(t, tc.wantRewinds, op.Status.Rewinds)
		})
	}
}
