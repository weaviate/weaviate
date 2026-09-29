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

package selfrecovery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/entities/models"
)

func TestRunOneLeavesAnInflightOpItsShard(t *testing.T) {
	type row struct {
		name        string
		ref         ShardRef
		wiped       bool
		tenant      bool
		op          *api.ReplicationDetailsResponse
		listErr     error
		answer      probeAnswer
		wantDirs    shardDirs
		wantOutcome string
		wantProbes  bool
	}
	startup := ShardRef{Collection: "c", Shard: "S"}
	activation := activationRef("c", "S")
	var rows []row
	for _, trig := range []struct {
		name  string
		ref   ShardRef
		wiped bool
	}{{"startup", startup, false}, {"wiped startup", startup, true}, {"activation", activation, true}} {
		for _, tenant := range []bool{false, true} {
			suffix := trig.name
			if tenant {
				suffix += " tenant"
			}
			rows = append(rows,
				row{
					name: "cancellable HYDRATING op, peers empty, " + suffix, ref: trig.ref, wiped: trig.wiped, tenant: tenant,
					op: srDetails(api.HYDRATING, "self", false, false), answer: answerHostedEmpty,
					wantDirs: shardDirs{recovery: true}, wantOutcome: "skipped",
				},
				row{
					name: "uncancellable FINALIZING op before the promote, peers empty, " + suffix, ref: trig.ref, wiped: trig.wiped, tenant: tenant,
					op: srDetails(api.FINALIZING, "self", true, false), answer: answerHostedEmpty,
					wantDirs: shardDirs{recovery: true}, wantOutcome: "skipped",
				},
				row{
					name: "no op, peers empty, " + suffix, ref: trig.ref, wiped: trig.wiped, tenant: tenant,
					answer: answerHostedEmpty, wantDirs: shardDirs{live: true, recovery: true}, wantOutcome: "empty_fallback", wantProbes: true,
				},
				row{
					name: "terminal op, peers empty, " + suffix, ref: trig.ref, wiped: trig.wiped, tenant: tenant,
					op: srDetails(api.CANCELLED, "self", false, false), answer: answerHostedEmpty,
					wantDirs: shardDirs{live: true, recovery: true}, wantOutcome: "empty_fallback", wantProbes: true,
				},
			)
		}
	}
	rows = append(rows,
		row{
			name: "op on another node does not block", ref: startup, op: srDetails(api.HYDRATING, "other-node", false, false),
			answer: answerHostedEmpty, wantDirs: shardDirs{live: true, recovery: true}, wantOutcome: "empty_fallback", wantProbes: true,
		},
		row{
			name: "op lookup failure never falls back", ref: startup, listErr: errors.New("leader unavailable"),
			answer: answerHostedEmpty, wantDirs: shardDirs{recovery: true}, wantOutcome: "failure",
		},
		row{
			name: "in-flight op is not re-registered", ref: startup, op: srDetails(api.HYDRATING, "self", false, false),
			answer: answerHostedData, wantDirs: shardDirs{recovery: true}, wantOutcome: "skipped",
		},
	)
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			live, recovery := prepareShardDirs(t, root, shardDirs{recovery: true})
			ns := &stubNodeSelector{addrs: map[string]string{"peer1": "10.0.0.1"}, ports: map[string]int{"peer1": 50051}}
			var probes atomic.Int32
			clientFactory := func(context.Context, string) (copier.FileReplicationServiceClient, error) {
				return &stubFileReplicationClient{
					probeShardData: func(context.Context, *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
						probes.Add(1)
						return tc.answer()
					},
				}, nil
			}
			raft := newInflightOpRaft(inflightOpCase{op: tc.op, listErr: tc.listErr})
			var schemaR SchemaReader = stubSchema{replicas: []string{"self", "peer1"}}
			if tc.tenant {
				s := &stubSchemaWithStatus{stubSchema: stubSchema{replicas: []string{"self", "peer1"}}}
				s.status.Store(models.TenantActivityStatusHOT)
				schemaR = s
			}
			o := newOrchestratorForTest(t, raft, schemaR, ns, clientFactory, stubPathResolver{root: root})
			o.enabled = true
			o.onRecoveryComplete = func(context.Context, string, string) error { return nil }
			before := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues(tc.wantOutcome))

			o.runOne(context.Background(), tc.ref, tc.wiped)

			requireShardDirs(t, live, recovery, tc.wantDirs)
			require.InDelta(t, before+1, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues(tc.wantOutcome)), 0.001)
			require.Equal(t, tc.wantProbes, probes.Load() > 0)
			require.Empty(t, raft.registeredCalls)
		})
	}
}
