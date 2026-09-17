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

package replica

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestTargetHostAddrsForShardBufferReuse(t *testing.T) {
	const (
		class = "C"
		local = "A"
	)
	plans := map[string]types.ReadRoutingPlan{
		"s1": {Shard: "s1", ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "s1", HostAddr: "10.0.0.1:8080"},
			{NodeName: "B", ShardName: "s1", HostAddr: "10.0.0.2:8080"},
			{NodeName: "D", ShardName: "s1", HostAddr: "10.0.0.3:8080"},
		}}},
		"s2": {Shard: "s2", ReplicaSet: types.ReadReplicaSet{Replicas: []types.Replica{
			{NodeName: "A", ShardName: "s2", HostAddr: "10.0.0.1:8080"},
			{NodeName: "E", ShardName: "s2", HostAddr: "10.0.0.4:8080"},
		}}},
	}

	logger, _ := test.NewNullLogger()
	metrics, err := NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err)

	mr := types.NewMockRouter(t)
	mr.EXPECT().BuildRoutingPlanOptions(mock.Anything, mock.Anything, types.ConsistencyLevelOne, "").
		RunAndReturn(func(shard, tenant string, cl types.ConsistencyLevel, _ string) types.RoutingPlanBuildOptions {
			return types.RoutingPlanBuildOptions{Shard: shard, Tenant: tenant, ConsistencyLevel: cl}
		})
	mr.EXPECT().BuildReadRoutingPlan(mock.Anything).
		RunAndReturn(func(o types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
			return plans[o.Shard], nil
		})

	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname(local).Return("10.0.0.1:8080", true)

	finder := NewFinder(class, mr, nodeResolver, local,
		NewMockRClient(t), metrics, logger, func() string { return "" })

	first, err := finder.targetHostAddrsForShard("s1", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.2:8080", "10.0.0.3:8080"}, first)

	retained := first[0]
	second, err := finder.targetHostAddrsForShard("s2", first)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.4:8080"}, second)
	assert.Same(t, &first[0], &second[0])
	assert.Equal(t, "10.0.0.2:8080", retained)
}
