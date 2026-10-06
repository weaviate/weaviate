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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestPullPassesThePlansSchemaVersionToEveryAttempt pins the version reaching the replicas.
// It is resolved once with the routing plan, so every worker and retry of one read is judged
// against the same version.
func TestPullPassesThePlansSchemaVersionToEveryAttempt(t *testing.T) {
	const (
		cls           = "C1"
		shard         = "S1"
		schemaVersion = uint64(4711)
	)

	hosts := []types.Replica{
		{NodeName: "A", ShardName: shard, HostAddr: "a:7001"},
		{NodeName: "B", ShardName: shard, HostAddr: "b:7001"},
	}

	logger, _ := test.NewNullLogger()
	metrics, err := replica.NewMetrics(nil)
	require.NoError(t, err)

	options := types.RoutingPlanBuildOptions{Shard: shard, ConsistencyLevel: types.ConsistencyLevelAll}
	router := types.NewMockRouter(t)
	router.EXPECT().BuildRoutingPlanOptions(shard, shard, types.ConsistencyLevelAll, "").Return(options).Once()
	router.EXPECT().BuildReadRoutingPlan(options).Return(types.ReadRoutingPlan{
		Shard:               shard,
		ReplicaSet:          types.ReadReplicaSet{Replicas: hosts},
		ConsistencyLevel:    types.ConsistencyLevelAll,
		IntConsistencyLevel: 2,
		SchemaVersion:       schemaVersion,
	}, nil).Once()

	c := replica.NewReadCoordinator[int](router, metrics, cls, shard, "", logger)

	var (
		mu   sync.Mutex
		seen []uint64
	)
	op := func(_ context.Context, _ string, _ bool, got uint64) (int, error) {
		mu.Lock()
		seen = append(seen, got)
		mu.Unlock()
		return 1, nil
	}

	replyCh, level, err := c.Pull(context.Background(), types.ConsistencyLevelAll, op, "", time.Minute)
	require.NoError(t, err)
	require.Equal(t, 2, level)
	for range replyCh {
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, len(hosts), "one attempt per replica")
	for _, got := range seen {
		require.Equal(t, schemaVersion, got)
	}
}

// TestPullSendsNoVersionWhenThePlanHasNone covers a plan with no version: the replica receives
// 0, cannot rule out lag, and keeps treating a miss as lag.
func TestPullSendsNoVersionWhenThePlanHasNone(t *testing.T) {
	const (
		cls   = "C1"
		shard = "S1"
	)

	hosts := []types.Replica{{NodeName: "A", ShardName: shard, HostAddr: "a:7001"}}

	logger, _ := test.NewNullLogger()
	metrics, err := replica.NewMetrics(nil)
	require.NoError(t, err)

	options := types.RoutingPlanBuildOptions{Shard: shard, ConsistencyLevel: types.ConsistencyLevelOne}
	router := types.NewMockRouter(t)
	router.EXPECT().BuildRoutingPlanOptions(shard, shard, types.ConsistencyLevelOne, "").Return(options).Once()
	router.EXPECT().BuildReadRoutingPlan(options).Return(types.ReadRoutingPlan{
		Shard:               shard,
		ReplicaSet:          types.ReadReplicaSet{Replicas: hosts},
		ConsistencyLevel:    types.ConsistencyLevelOne,
		IntConsistencyLevel: 1,
	}, nil).Once()

	c := replica.NewReadCoordinator[int](router, metrics, cls, shard, "", logger)

	var got uint64 = 1
	op := func(_ context.Context, _ string, _ bool, schemaVersion uint64) (int, error) {
		got = schemaVersion
		return 1, nil
	}

	replyCh, _, err := c.Pull(context.Background(), types.ConsistencyLevelOne, op, "", time.Minute)
	require.NoError(t, err)
	for range replyCh {
	}
	require.Equal(t, uint64(0), got)
}
