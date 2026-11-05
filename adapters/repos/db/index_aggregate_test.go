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

package db

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestIndexAggregate_RunsShardsConcurrently pins that an aggregation asks all
// shards at once: each remote shard answers only after every shard has been
// asked, so shards taken one at a time never get past the first.
func TestIndexAggregate_RunsShardsConcurrently(t *testing.T) {
	const className = "AggregateShards"
	// three, as the aggregation runs at least three shards at a time on any CPU count
	shards := []string{"shard-a", "shard-b", "shard-c"}

	idx, _ := refCountTestIndex(t, className)

	router := types.NewMockRouter(t)
	plan := types.ReadRoutingPlan{}
	for _, shard := range shards {
		plan.ReplicaSet.Replicas = append(plan.ReplicaSet.Replicas,
			types.Replica{NodeName: "node2", ShardName: shard, HostAddr: "127.0.0.2"})
	}
	router.EXPECT().BuildReadRoutingPlan(mock.Anything).Return(plan, nil).Maybe()
	router.EXPECT().GetReadReplicasLocation(className, mock.Anything, mock.Anything).Return(
		types.ReadReplicaSet{Replicas: []types.Replica{{NodeName: "node2", HostAddr: "127.0.0.2"}}}, nil,
	).Maybe()
	idx.router = router

	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname("node2").Return("127.0.0.2", true).Maybe()
	client := newBarrierAggregateClient(len(shards))
	idx.remote = sharding.NewRemoteIndex(className, remoteShardsOnNode2{}, nodeResolver, client)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	res, err := idx.aggregate(ctx, nil, aggregation.Params{ClassName: className}, nil)
	require.NoError(t, err)
	require.Len(t, res.Groups, 1)
	require.Equal(t, len(shards), res.Groups[0].Count)
}

// barrierAggregateClient answers an aggregation once want of them are waiting.
type barrierAggregateClient struct {
	FakeRemoteClient
	want    int32
	arrived atomic.Int32
	all     chan struct{}
}

func newBarrierAggregateClient(want int) *barrierAggregateClient {
	return &barrierAggregateClient{want: int32(want), all: make(chan struct{})}
}

func (c *barrierAggregateClient) Aggregate(ctx context.Context, _, _, _ string,
	_ aggregation.Params,
) (*aggregation.Result, error) {
	if c.arrived.Add(1) == c.want {
		close(c.all)
	}
	select {
	case <-c.all:
		return &aggregation.Result{Groups: []aggregation.Group{{Count: 1}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type remoteShardsOnNode2 struct{}

func (remoteShardsOnNode2) ShardOwner(_, _ string) (string, error) { return "node2", nil }

func (remoteShardsOnNode2) ShardReplicas(_, _ string) ([]string, error) {
	return []string{"node2"}, nil
}
