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

package replica_test

import (
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestTargetHostAddrsForShard(t *testing.T) {
	const (
		class = "C"
		local = "A"
		shard = "s1"
	)

	replicas := func(rs ...types.Replica) types.ReadRoutingPlan {
		return types.ReadRoutingPlan{Shard: shard, ReplicaSet: types.ReadReplicaSet{Replicas: rs}}
	}

	tests := []struct {
		name          string
		plan          types.ReadRoutingPlan
		planErr       error
		localAddr     string
		localResolves bool
		want          []string
		wantErr       bool
	}{
		{
			name: "excludes the local node by name",
			plan: replicas(
				types.Replica{NodeName: "A", ShardName: shard, HostAddr: "10.0.0.1:8080"},
				types.Replica{NodeName: "B", ShardName: shard, HostAddr: "10.0.0.2:8080"},
				types.Replica{NodeName: "D", ShardName: shard, HostAddr: "10.0.0.3:8080"},
			),
			localAddr:     "10.0.0.1:8080",
			localResolves: true,
			want:          []string{"10.0.0.2:8080", "10.0.0.3:8080"},
		},
		{
			name: "excludes a replica resolving to the local addr",
			plan: replicas(
				types.Replica{NodeName: "A", ShardName: shard, HostAddr: "10.0.0.1:8080"},
				types.Replica{NodeName: "B", ShardName: shard, HostAddr: "10.0.0.1:8080"},
				types.Replica{NodeName: "D", ShardName: shard, HostAddr: "10.0.0.3:8080"},
			),
			localAddr:     "10.0.0.1:8080",
			localResolves: true,
			want:          []string{"10.0.0.3:8080"},
		},
		{
			name: "local-only replica set yields no targets",
			plan: replicas(
				types.Replica{NodeName: "A", ShardName: shard, HostAddr: "10.0.0.1:8080"},
			),
			localAddr:     "10.0.0.1:8080",
			localResolves: true,
			want:          []string{},
		},
		{
			name:          "routing plan error propagates",
			planErr:       errors.New("shard not found"),
			localResolves: true,
			wantErr:       true,
		},
		{
			name:    "unresolvable local node errors",
			plan:    replicas(types.Replica{NodeName: "B", ShardName: shard, HostAddr: "10.0.0.2:8080"}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			metrics, err := replica.NewMetrics(monitoring.GetMetrics())
			require.NoError(t, err)

			mr := types.NewMockRouter(t)
			mr.EXPECT().BuildRoutingPlanOptions(shard, shard, types.ConsistencyLevelOne, "").
				Return(types.RoutingPlanBuildOptions{Shard: shard, Tenant: shard, ConsistencyLevel: types.ConsistencyLevelOne})
			mr.EXPECT().
				BuildReadRoutingPlan(mock.MatchedBy(func(o types.RoutingPlanBuildOptions) bool { return o.LocalOnly })).
				Return(tt.plan, tt.planErr)

			nodeResolver := cluster.NewMockNodeResolver(t)
			nodeResolver.EXPECT().NodeHostname(local).Return(tt.localAddr, tt.localResolves).Maybe()

			finder := replica.NewFinder(class, mr, nodeResolver, local,
				replica.NewMockRClient(t), metrics, logger, func() string { return "" })

			hosts, err := finder.TargetHostAddrsForShard(shard)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if len(tt.want) == 0 {
				assert.Empty(t, hosts)
			} else {
				assert.Equal(t, tt.want, hosts)
			}
		})
	}
}

func BenchmarkTargetHostAddrsForShard(b *testing.B) {
	f := newFakeFactory(b, "C", "s1", []string{"A", "B", "D"}, true)
	finder := f.newFinder("A")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := finder.TargetHostAddrsForShard("s1"); err != nil {
			b.Fatal(err)
		}
	}
}
