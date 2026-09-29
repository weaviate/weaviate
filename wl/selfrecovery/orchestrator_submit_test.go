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
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/copier"
)

func TestSubmit_StartedWithoutRaftStatePicksBucket(t *testing.T) {
	for _, started := range []bool{true, false} {
		t.Run(fmt.Sprintf("started_without_raft_state=%v", started), func(t *testing.T) {
			ns := &stubNodeSelector{
				addrs: map[string]string{"peer1": "10.0.0.1"},
				ports: map[string]int{"peer1": 50051},
			}
			clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
				return &stubFileReplicationClient{
					probeShardData: func(_ context.Context, _ *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
						return &protocol.ProbeShardDataResponse{HasData: false}, nil
					},
				}, nil
			}
			o := newOrchestratorForTest(t, &stubRaft{},
				stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})
			o.enabled = true
			wantBenign := testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal)
			wantCritical := testutil.ToFloat64(o.metrics.NoDataEmptyTotal)
			if started {
				wantBenign++
			} else {
				wantCritical++
			}

			require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, started))

			require.Eventually(t, func() bool {
				return testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal) == wantBenign &&
					testutil.ToFloat64(o.metrics.NoDataEmptyTotal) == wantCritical
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
