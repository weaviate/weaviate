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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/namespaces"
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

// TestIndexAggregate_SpreadsShardsOverReplicas pins that shards with replicas on
// several nodes are each scanned on a different node, even where the cluster
// state would send them all to the same one.
func TestIndexAggregate_SpreadsShardsOverReplicas(t *testing.T) {
	const className = "AggregateSpread"
	idx, _ := refCountTestIndex(t, className)

	router := types.NewMockRouter(t)
	plan := types.ReadRoutingPlan{LocalHostname: "node1"}
	for _, shard := range []string{"shard-a", "shard-b"} {
		for _, node := range []string{"node2", "node3"} {
			plan.ReplicaSet.Replicas = append(plan.ReplicaSet.Replicas,
				types.Replica{NodeName: node, ShardName: shard, HostAddr: node})
		}
	}
	router.EXPECT().BuildReadRoutingPlan(mock.Anything).Return(plan, nil).Maybe()
	idx.router = router

	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname("node2").Return("127.0.0.2", true).Maybe()
	nodeResolver.EXPECT().NodeHostname("node3").Return("127.0.0.3", true).Maybe()
	client := newRecordingAggregateClient()
	idx.remote = sharding.NewRemoteIndex(className, remoteShardsOnNode2{}, nodeResolver, client)

	res, err := idx.aggregate(t.Context(), nil, aggregation.Params{ClassName: className}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, res.Groups[0].Count)
	require.ElementsMatch(t, []string{"127.0.0.2", "127.0.0.3"},
		[]string{client.hosts["shard-a"], client.hosts["shard-b"]})
}

// recordingAggregateClient answers every aggregation except those sent to a
// failing host, and records every host asked and where each shard was answered.
type recordingAggregateClient struct {
	FakeRemoteClient
	failing map[string]bool
	mu      sync.Mutex
	asked   []string
	hosts   map[string]string
}

func newRecordingAggregateClient(failing ...string) *recordingAggregateClient {
	c := &recordingAggregateClient{failing: map[string]bool{}, hosts: map[string]string{}}
	for _, host := range failing {
		c.failing[host] = true
	}
	return c
}

func (c *recordingAggregateClient) Aggregate(_ context.Context, host, _, shard string,
	_ aggregation.Params,
) (*aggregation.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asked = append(c.asked, host)
	if c.failing[host] {
		return nil, errors.New("replica unavailable")
	}
	c.hosts[shard] = host
	return &aggregation.Result{Groups: []aggregation.Group{{Count: 1}}}, nil
}

// aggregateTestIndex returns an index whose read plan is replicas, with node1 as
// the local node and every remote call going to client.
func aggregateTestIndex(t *testing.T, client *recordingAggregateClient, replicas ...types.Replica) *Index {
	t.Helper()
	const className = "AggregateReplicas"
	idx, _ := refCountTestIndex(t, className)
	// replicated, so a local read never creates a missing shard
	idx.Config.ReplicationFactor = 2

	router := types.NewMockRouter(t)
	router.EXPECT().BuildReadRoutingPlan(mock.Anything).Return(types.ReadRoutingPlan{
		LocalHostname: "node1",
		ReplicaSet:    types.ReadReplicaSet{Replicas: replicas},
	}, nil).Maybe()
	router.EXPECT().GetReadReplicasLocation(className, mock.Anything, mock.Anything).Return(
		types.ReadReplicaSet{Replicas: replicas}, nil,
	).Maybe()
	idx.router = router

	nodeResolver := cluster.NewMockNodeResolver(t)
	for _, node := range []string{"node2", "node3", "node4"} {
		nodeResolver.EXPECT().NodeHostname(node).Return(node+"-host", true).Maybe()
	}
	// the cluster state lists node4, which the read plan leaves out
	idx.remote = sharding.NewRemoteIndex(className, shardReplicasOn{"node4"}, nodeResolver, client)
	return idx
}

func replicasOf(shard string, nodes ...string) []types.Replica {
	out := make([]types.Replica, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, types.Replica{NodeName: node, ShardName: shard, HostAddr: node + "-host"})
	}
	return out
}

// A shard falls over only to the read plan's other replicas, never to a node the
// plan leaves out, and asks each at most once.
func TestIndexAggregate_FailsOverWithinReadPlan(t *testing.T) {
	tests := []struct {
		name      string
		replicas  []types.Replica
		failing   []string
		wantErr   bool
		wantAsked []string
		answered  string
	}{
		{
			name:      "a failing replica falls over to the other",
			replicas:  replicasOf("shard-a", "node2", "node3"),
			failing:   []string{"node2-host"},
			wantAsked: []string{"node2-host", "node3-host"},
			answered:  "node3-host",
		},
		{
			name:      "with every read replica failing, the shard fails",
			replicas:  replicasOf("shard-a", "node2", "node3"),
			failing:   []string{"node2-host", "node3-host"},
			wantErr:   true,
			wantAsked: []string{"node2-host", "node3-host"},
		},
		{
			name:      "a local replica that is not loaded falls over to a remote one",
			replicas:  replicasOf("shard-a", "node1", "node2"),
			wantAsked: []string{"node2-host"},
			answered:  "node2-host",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newRecordingAggregateClient(tt.failing...)
			idx := aggregateTestIndex(t, client, tt.replicas...)

			res, err := idx.aggregate(t.Context(), nil, aggregation.Params{ClassName: "AggregateReplicas"}, nil)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, res.Groups[0].Count)
				require.Equal(t, tt.answered, client.hosts["shard-a"])
			}
			// the picked replica is random, so only the set is fixed when one fails
			if len(tt.failing) > 0 && !tt.wantErr {
				require.Subset(t, tt.wantAsked, client.asked)
				require.Contains(t, client.asked, tt.answered)
			} else {
				require.ElementsMatch(t, tt.wantAsked, client.asked)
			}
		})
	}
}

// A read sent to a peer is refused whenever a local read would be.
func TestIndexAggregate_RefusesRemoteReadsLikeLocalOnes(t *testing.T) {
	tests := []struct {
		name    string
		refuse  func(t *testing.T, idx *Index)
		wantErr error
	}{
		{
			name: "closed index",
			refuse: func(t *testing.T, idx *Index) {
				idx.closeLock.Lock()
				idx.closed = true
				idx.closeLock.Unlock()
				t.Cleanup(func() {
					idx.closeLock.Lock()
					idx.closed = false
					idx.closeLock.Unlock()
				})
			},
			wantErr: errAlreadyShutdown,
		},
		{
			name: "suspended namespace",
			refuse: func(t *testing.T, idx *Index) {
				namespace, exister := idx.namespace, idx.namespacesExister
				idx.namespace = "alpha"
				idx.namespacesExister = existerWithState(t, api.NamespaceStateSuspended)
				t.Cleanup(func() { idx.namespace, idx.namespacesExister = namespace, exister })
			},
			wantErr: namespaces.ErrNamespaceSuspended,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newRecordingAggregateClient()
			idx := aggregateTestIndex(t, client, replicasOf("shard-a", "node2")...)
			tt.refuse(t, idx)

			_, err := idx.aggregate(t.Context(), nil, aggregation.Params{ClassName: "AggregateReplicas"}, nil)
			require.ErrorIs(t, err, tt.wantErr)
			require.Empty(t, client.asked)
		})
	}
}

type shardReplicasOn []string

func (s shardReplicasOn) ShardOwner(_, _ string) (string, error) { return s[0], nil }

func (s shardReplicasOn) ShardReplicas(_, _ string) ([]string, error) { return s, nil }

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
