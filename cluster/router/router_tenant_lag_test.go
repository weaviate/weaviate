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
	"time"

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
	// lagLeaderTimeout is long enough that the lookup never expires by accident; the timeout
	// itself is exercised by TestMultiTenantPlanGivesUpOnASlowLeader.
	lagLeaderTimeout = 2 * time.Second
)

// leaderHolding is a replication manager whose leader answers with shards for the
// tenant. Once, so the tests also pin that local state is consulted first.
func leaderHolding(t *testing.T, shards map[string][]string) replicationTypes.Manager {
	m := replicationTypes.NewMockManager(t)
	m.EXPECT().QueryShardingStateByCollectionAndShard(mock.Anything, lagCollection, lagTenant).
		Return(api.ShardingState{Collection: lagCollection, Shards: shards}, nil).Once()
	return m
}

// leaderSlow is a leader that neither answers nor refuses, which is what the timeout exists for:
// it must not hold the read open past its own deadline.
func leaderSlow(t *testing.T) replicationTypes.Manager {
	m := replicationTypes.NewMockManager(t)
	m.EXPECT().QueryShardingStateByCollectionAndShard(mock.Anything, lagCollection, lagTenant).
		RunAndReturn(func(ctx context.Context, _, _ string) (api.ShardingState, error) {
			<-ctx.Done()
			return api.ShardingState{}, ctx.Err()
		}).Once()
	return m
}

// leaderFailing is a replication manager whose leader cannot answer.
func leaderFailing(t *testing.T, err error) replicationTypes.Manager {
	m := replicationTypes.NewMockManager(t)
	m.EXPECT().QueryShardingStateByCollectionAndShard(mock.Anything, lagCollection, lagTenant).
		Return(api.ShardingState{}, err).Once()
	return m
}

// lagRouter builds a multi-tenant router whose local sharding state does not list the
// tenant while the leader confirms it is HOT -- the shape behind the production line
// "shard not found : class %q shard %q". A nil manager is the local-only behaviour.
func lagRouter(t *testing.T, manager replicationTypes.Manager, write bool,
	leaderTimeout ...time.Duration,
) types.Router {
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

	timeout := lagLeaderTimeout
	if len(leaderTimeout) > 0 {
		timeout = leaderTimeout[0]
	}

	b := router.NewBuilder(lagCollection, true, nodeSelector, schemaGetter, schemaReader, fsm)
	if manager != nil {
		b = b.WithReplicationManager(manager, timeout)
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

func TestMultiTenantPlanFallsBackToTheLeaderForPlacement(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		plan, err := lagRouter(t, leaderHolding(t, map[string][]string{lagTenant: {"node1", "node2"}}), false).
			BuildReadRoutingPlan(lagPlanOptions())
		require.NoError(t, err,
			"the leader confirmed the tenant is HOT, so a read must not fail on this node being behind")
		require.Len(t, plan.ReplicaSet.Replicas, 2)
	})

	t.Run("write", func(t *testing.T) {
		plan, err := lagRouter(t, leaderHolding(t, map[string][]string{lagTenant: {"node1", "node2"}}), true).
			BuildWriteRoutingPlan(lagPlanOptions())
		require.NoError(t, err,
			"the leader confirmed the tenant is HOT, so a write must not fail on this node being behind")
		require.Len(t, plan.ReplicaSet.Replicas, 2)
	})
}

// The fallback must not mask a tenant that is genuinely gone, or a leader that is down.
func TestMultiTenantPlanKeepsTheLocalErrorWhenTheLeaderCannotHelp(t *testing.T) {
	tests := []struct {
		name    string
		manager func(*testing.T) replicationTypes.Manager
	}{
		{
			name: "the leader does not hold the shard either",
			manager: func(t *testing.T) replicationTypes.Manager {
				return leaderFailing(t, errors.New("not found: tenant-7"))
			},
		},
		{
			name:    "the leader cannot be reached",
			manager: func(t *testing.T) replicationTypes.Manager { return leaderFailing(t, errors.New("leader not found")) },
		},
		{
			name:    "the leader answers without the shard",
			manager: func(t *testing.T) replicationTypes.Manager { return leaderHolding(t, map[string][]string{}) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := lagRouter(t, tt.manager(t), false).BuildReadRoutingPlan(lagPlanOptions())
			require.ErrorIs(t, err, clusterSchema.ErrShardNotFound,
				"the local error is the one the caller can act on")
		})
	}
}

func TestMultiTenantPlanWithoutAReplicationManagerIsUnchanged(t *testing.T) {
	_, err := lagRouter(t, nil, false).BuildReadRoutingPlan(lagPlanOptions())
	require.ErrorIs(t, err, clusterSchema.ErrShardNotFound)
}

// A leader that goes quiet must cost the read its own deadline and no more, falling back to the
// local error rather than holding the caller open.
func TestMultiTenantPlanGivesUpOnASlowLeader(t *testing.T) {
	started := time.Now()
	_, err := lagRouter(t, leaderSlow(t), false, 50*time.Millisecond).
		BuildReadRoutingPlan(lagPlanOptions())
	require.ErrorIs(t, err, clusterSchema.ErrShardNotFound,
		"the local error is the one the caller can act on")
	require.Less(t, time.Since(started), time.Second,
		"the lookup must be bounded by its timeout, not by the caller giving up (took %s)",
		time.Since(started))
}
