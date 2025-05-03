package router_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
)

func TestRouterWithFSM(t *testing.T) {
	logger, _ := test.NewNullLogger()
	clusterState := clusterMocks.NewMockNodeSelector("node1", "node2")
	schemaReaderMock := schemaTypes.NewMockSchemaReader(t)
	schemaReaderMock.On("ShardReplicas", mock.Anything, mock.Anything).Return(func(class string, shard string) ([]string, error) {
		return []string{"node1", "node2"}, nil
	})
	reg := prometheus.NewRegistry()
	shardReplicationFSM := replication.NewShardReplicationFSM(reg)
	myRouter := router.New(logger, clusterState, schemaReaderMock, shardReplicationFSM)

	getReadRoutingPlan := func() *types.RoutingPlan {
		myRoutingPlan, err := myRouter.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
			Collection: "collection1",
			Shard:      "shard1",
		})
		require.NoError(t, err)
		return &myRoutingPlan
	}
	setOpStatus := func(state api.ShardReplicationState) {
		shardReplicationFSM.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      1,
			State:   state,
		})
	}

	shardReplicationFSM.Replicate(1, &api.ReplicationReplicateShardRequest{
		Version:          api.ReplicationCommandVersionV0,
		SourceNode:       "node1",
		SourceCollection: "collection1",
		SourceShard:      "shard1",
		TargetNode:       "node2",
		Uuid:             "00000000-0000-0000-0000-000000000000",
	})
	require.Equal(t, []string{"node1"}, getReadRoutingPlan().Replicas)

	setOpStatus(api.HYDRATING)
	require.Equal(t, []string{"node1"}, getReadRoutingPlan().Replicas)

	setOpStatus(api.FINALIZING)
	require.Equal(t, []string{"node1"}, getReadRoutingPlan().Replicas)

	setOpStatus(api.READY)
	require.Equal(t, []string{"node1", "node2"}, getReadRoutingPlan().Replicas)

	setOpStatus(api.DEHYDRATING)
	// TODO should this be only node2?
	require.Equal(t, []string{"node1"}, getReadRoutingPlan().Replicas)

	setOpStatus(api.ABORTED)
	require.Equal(t, []string{"node1"}, getReadRoutingPlan().Replicas)

	shardReplicationFSM.DeleteReplicationOp(&api.ReplicationDeleteOpRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      1,
	})
	require.Equal(t, []string{"node1", "node2"}, getReadRoutingPlan().Replicas)
}
