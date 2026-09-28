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

package router_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// A replica on a node memberlist declared dead must still count towards the consistency level.
func TestRouter_UnreachableReplicaCountsTowardsConsistencyLevel(t *testing.T) {
	const (
		collection = "TestClass"
		shard      = "shard1"
		tenant     = "alice"
	)
	allNodes := []string{"node1", "node2", "node3"}

	tests := []struct {
		name       string
		reachable  []string
		level      types.ConsistencyLevel
		wantErr    bool
		wantIntCL  int
		wantPlanOK int
	}{
		{name: "all reachable, ALL", reachable: allNodes, level: types.ConsistencyLevelAll, wantIntCL: 3, wantPlanOK: 3},
		{name: "all reachable, QUORUM", reachable: allNodes, level: types.ConsistencyLevelQuorum, wantIntCL: 2, wantPlanOK: 3},
		{name: "all reachable, ONE", reachable: allNodes, level: types.ConsistencyLevelOne, wantIntCL: 1, wantPlanOK: 3},
		{name: "one down, ALL fails", reachable: []string{"node1", "node2"}, level: types.ConsistencyLevelAll, wantErr: true},
		{name: "one down, QUORUM succeeds", reachable: []string{"node1", "node2"}, level: types.ConsistencyLevelQuorum, wantIntCL: 2, wantPlanOK: 2},
		{name: "one down, ONE succeeds", reachable: []string{"node1", "node2"}, level: types.ConsistencyLevelOne, wantIntCL: 1, wantPlanOK: 2},
		{name: "two down, ALL fails", reachable: []string{"node1"}, level: types.ConsistencyLevelAll, wantErr: true},
		{name: "two down, QUORUM fails", reachable: []string{"node1"}, level: types.ConsistencyLevelQuorum, wantErr: true},
		{name: "two down, ONE succeeds", reachable: []string{"node1"}, level: types.ConsistencyLevelOne, wantIntCL: 1, wantPlanOK: 1},
	}

	newSingleTenant := func(t *testing.T, reachable []string) types.Router {
		schemaReader := schema.NewMockSchemaReader(t)
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		state := createShardingStateWithShards([]string{shard})
		schemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
				return readFunc(&models.Class{Class: className}, state)
			}).Maybe()
		schemaReader.EXPECT().ShardReplicas(collection, shard).Return(allNodes, nil).Maybe()
		fsm.EXPECT().FilterOneShardReplicasRead(collection, shard, allNodes).Return(allNodes).Maybe()
		fsm.EXPECT().FilterOneShardReplicasWrite(collection, shard, allNodes).Return(allNodes).Maybe()
		return router.NewBuilder(collection, false, mocks.NewMockNodeSelector(reachable...),
			schema.NewMockSchemaGetter(t), schemaReader, fsm).Build()
	}

	newMultiTenant := func(t *testing.T, reachable []string) types.Router {
		schemaReader := schema.NewMockSchemaReader(t)
		schemaGetter := schema.NewMockSchemaGetter(t)
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		schemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, collection, tenant).
			Return(map[string]string{tenant: models.TenantActivityStatusHOT}, nil).Maybe()
		schemaReader.EXPECT().ShardReplicas(collection, tenant).Return(allNodes, nil).Maybe()
		fsm.EXPECT().FilterOneShardReplicasRead(collection, tenant, allNodes).Return(allNodes).Maybe()
		fsm.EXPECT().FilterOneShardReplicasWrite(collection, tenant, allNodes).Return(allNodes).Maybe()
		return router.NewBuilder(collection, true, mocks.NewMockNodeSelector(reachable...),
			schemaGetter, schemaReader, fsm).Build()
	}

	routers := []struct {
		name  string
		build func(*testing.T, []string) types.Router
		opts  types.RoutingPlanBuildOptions
	}{
		{name: "single-tenant", build: newSingleTenant, opts: types.RoutingPlanBuildOptions{Shard: shard}},
		{name: "multi-tenant", build: newMultiTenant, opts: types.RoutingPlanBuildOptions{Tenant: tenant}},
	}

	for _, rt := range routers {
		for _, tt := range tests {
			t.Run(rt.name+"/write/"+tt.name, func(t *testing.T) {
				opts := rt.opts
				opts.ConsistencyLevel = tt.level
				plan, err := rt.build(t, tt.reachable).BuildWriteRoutingPlan(opts)
				if tt.wantErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), "unreachable")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.wantIntCL, plan.IntConsistencyLevel)
				require.Len(t, plan.ReplicaSet.Replicas, tt.wantPlanOK)
			})
			t.Run(rt.name+"/read/"+tt.name, func(t *testing.T) {
				opts := rt.opts
				opts.ConsistencyLevel = tt.level
				plan, err := rt.build(t, tt.reachable).BuildReadRoutingPlan(opts)
				if tt.wantErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), "unreachable")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.wantIntCL, plan.IntConsistencyLevel)
				require.Len(t, plan.ReplicaSet.Replicas, tt.wantPlanOK)
			})
		}
	}
}
