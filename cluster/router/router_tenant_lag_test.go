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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	lagCollection = "TestClass"
	lagTenant     = "tenant-7"
)

type fakeShardingStateQuerier struct {
	shards map[string][]string
	err    error
	calls  int
}

func (f *fakeShardingStateQuerier) QueryShardingStateByCollectionAndShard(
	_ context.Context, collection, _ string,
) (api.ShardingState, error) {
	f.calls++
	if f.err != nil {
		return api.ShardingState{}, f.err
	}
	return api.ShardingState{Collection: collection, Shards: f.shards}, nil
}

// lagRouter builds a multi-tenant router whose local sharding state does not list the tenant
// while the leader confirms it is HOT -- the shape behind the production line
// "shard not found : class %q shard %q". A nil querier is the local-only behaviour.
func lagRouter(t *testing.T, querier router.ShardingStateQuerier, write bool) types.Router {
	t.Helper()

	localState := createShardingStateWithShards([]string{})

	nodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	schemaGetter := schema.NewMockSchemaGetter(t)
	schemaReader := schema.NewMockSchemaReader(t)
	fsm := replicationTypes.NewMockReplicationFSMReader(t)

	schemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, lagCollection, lagTenant).
		Return(map[string]string{lagTenant: models.TenantActivityStatusHOT}, nil)

	schemaReader.EXPECT().ClassInfo(mock.Anything).Return(clusterSchema.ClassInfo{}).Maybe()
	schemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(className string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(&models.Class{Class: className}, localState)
		}).Maybe()
	schemaReader.EXPECT().ShardReplicas(lagCollection, lagTenant).
		Return(nil, clusterSchema.ErrShardNotFound).Maybe()

	if write {
		fsm.EXPECT().FilterOneShardReplicasWrite(lagCollection, lagTenant, []string{"node1", "node2"}).
			Return([]string{"node1", "node2"}).Maybe()
	} else {
		fsm.EXPECT().FilterOneShardReplicasRead(lagCollection, lagTenant, []string{"node1", "node2"}).
			Return([]string{"node1", "node2"}).Maybe()
	}

	b := router.NewBuilder(lagCollection, true, nodeSelector, schemaGetter, schemaReader, fsm)
	if querier != nil {
		b = b.WithShardingStateQuerier(querier)
	}
	return b.Build()
}

func lagPlanOptions() types.RoutingPlanBuildOptions {
	return types.RoutingPlanBuildOptions{
		Shard:            lagTenant,
		Tenant:           lagTenant,
		ConsistencyLevel: types.ConsistencyLevelOne,
	}
}

// TestMultiTenantPlanFallsBackToTheLeaderForPlacement: a tenant this node has not seen yet
// resolves from the leader instead of failing, for reads and writes alike.
func TestMultiTenantPlanFallsBackToTheLeaderForPlacement(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		q := &fakeShardingStateQuerier{shards: map[string][]string{lagTenant: {"node1", "node2"}}}

		plan, err := lagRouter(t, q, false).BuildReadRoutingPlan(lagPlanOptions())
		require.NoError(t, err,
			"the leader confirmed the tenant is HOT, so a read must not fail on this node being behind")
		require.Len(t, plan.ReplicaSet.Replicas, 2)
		assert.Equal(t, 1, q.calls, "the leader is asked once, only because local state missed")
	})

	t.Run("write", func(t *testing.T) {
		q := &fakeShardingStateQuerier{shards: map[string][]string{lagTenant: {"node1", "node2"}}}

		plan, err := lagRouter(t, q, true).BuildWriteRoutingPlan(lagPlanOptions())
		require.NoError(t, err,
			"the leader confirmed the tenant is HOT, so a write must not fail on this node being behind")
		require.Len(t, plan.ReplicaSet.Replicas, 2)
		assert.Equal(t, 1, q.calls)
	})
}

// TestMultiTenantPlanKeepsTheLocalErrorWhenTheLeaderCannotHelp: the fallback must not mask a
// tenant that is genuinely gone or a leader that is down.
func TestMultiTenantPlanKeepsTheLocalErrorWhenTheLeaderCannotHelp(t *testing.T) {
	tests := []struct {
		name    string
		querier *fakeShardingStateQuerier
	}{
		{
			name:    "the leader does not hold the shard either",
			querier: &fakeShardingStateQuerier{err: errors.New("not found: tenant-7")},
		},
		{
			name:    "the leader cannot be reached",
			querier: &fakeShardingStateQuerier{err: errors.New("leader not found")},
		},
		{
			name:    "the leader answers without the shard",
			querier: &fakeShardingStateQuerier{shards: map[string][]string{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := lagRouter(t, tt.querier, false).BuildReadRoutingPlan(lagPlanOptions())
			require.ErrorIs(t, err, clusterSchema.ErrShardNotFound,
				"the local error is the one the caller can act on")
		})
	}
}

// TestMultiTenantPlanWithoutAQuerierIsUnchanged: a router built without the capability stays
// local-only.
func TestMultiTenantPlanWithoutAQuerierIsUnchanged(t *testing.T) {
	_, err := lagRouter(t, nil, false).BuildReadRoutingPlan(lagPlanOptions())
	require.ErrorIs(t, err, clusterSchema.ErrShardNotFound)
}
