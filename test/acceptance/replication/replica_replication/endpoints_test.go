//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica_replication

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

type ReplicationTestSuiteEndpoints struct {
	suite.Suite
}

func (suite *ReplicationTestSuiteEndpoints) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicationTestSuiteEndpoints(t *testing.T) {
	suite.Run(t, new(ReplicationTestSuiteEndpoints))
}

func (suite *ReplicationTestSuiteEndpoints) TestReplicationDeleteReplicaEndpoints() {
	t := suite.T()

	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{
		Factor: 3,
	}

	t.Run("create schema", func(t *testing.T) {
		helper.DeleteClass(t, paragraphClass.Class)
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("delete replica going below RF", func(t *testing.T) {
		request := getRequest(t, paragraphClass.Class)
		_, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&paragraphClass.Class).WithNodeID(request.SourceNodeName).WithShard(request.ShardID), nil)
		require.Error(t, err)
	})

	t.Run("delete replica no class", func(t *testing.T) {
		request := getRequest(t, paragraphClass.Class)
		deleted, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithNodeID(request.DestinationNodeName).WithShard(request.ShardID), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaBadRequest(), err)
		require.Nil(t, deleted)
	})

	t.Run("delete replica no shard", func(t *testing.T) {
		request := getRequest(t, paragraphClass.Class)
		deleted, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&paragraphClass.Class).WithNodeID(request.DestinationNodeName), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaBadRequest(), err)
		require.Nil(t, deleted)
	})

	t.Run("delete replica no node ID", func(t *testing.T) {
		request := getRequest(t, paragraphClass.Class)
		deleted, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&paragraphClass.Class).WithShard(request.ShardID), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaBadRequest(), err)
		require.Nil(t, deleted)
	})

	t.Run("delete replica with invalid collection", func(t *testing.T) {
		nonExistingCollection := "non-existing"
		request := getRequest(t, paragraphClass.Class)
		resp, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&nonExistingCollection).WithNodeID(request.DestinationNodeName).WithShard(request.ShardID), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaNotFound(), err)
		require.Nil(t, resp)
	})

	t.Run("delete replica with invalid node ID", func(t *testing.T) {
		nonExistingNodeID := "non-existing"
		request := getRequest(t, paragraphClass.Class)
		resp, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&paragraphClass.Class).WithNodeID(&nonExistingNodeID).WithShard(request.ShardID), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaNotFound(), err)
		require.Nil(t, resp)
	})

	t.Run("delete replica with invalid shard", func(t *testing.T) {
		nonExistingShard := "non-existing"
		request := getRequest(t, paragraphClass.Class)
		resp, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&paragraphClass.Class).WithNodeID(request.DestinationNodeName).WithShard(&nonExistingShard), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaNotFound(), err)
		require.Nil(t, resp)
	})

	t.Run("delete replica with all invalid parameters", func(t *testing.T) {
		nonExistingCollection := "non-existing"
		nonExistingNodeID := "non-existing"
		nonExistingShard := "non-existing"
		resp, err := helper.Client(t).Replication.DeleteReplica(replication.NewDeleteReplicaParams().WithCollection(&nonExistingCollection).WithNodeID(&nonExistingNodeID).WithShard(&nonExistingShard), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicaNotFound(), err)
		require.Nil(t, resp)
	})
}

func (suite *ReplicationTestSuiteEndpoints) TestReplicationReplicateEndpoints() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		helper.DeleteClass(t, paragraphClass.Class)
		helper.CreateClass(t, paragraphClass)
	})

	var id strfmt.UUID
	t.Run("get collection sharding state", func(t *testing.T) {
		shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().WithCollection(&paragraphClass.Class), nil)
		require.Nil(t, err)
		require.NotNil(t, shardingState)
		require.NotNil(t, shardingState.Payload)
		require.NotNil(t, shardingState.Payload.ShardingState)
		require.NotNil(t, shardingState.Payload.ShardingState.Collection)
		require.NotNil(t, shardingState.Payload.ShardingState.Shards)
		require.Equal(t, paragraphClass.Class, shardingState.Payload.ShardingState.Collection)
		require.Len(t, shardingState.Payload.ShardingState.Shards, 3)
		for _, shard := range shardingState.Payload.ShardingState.Shards {
			require.Len(t, shard.Replicas, 1)
		}
	})

	t.Run("get collection and shard sharding state", func(t *testing.T) {
		shard := getRequest(t, paragraphClass.Class).ShardID
		shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().WithCollection(&paragraphClass.Class).WithShard(shard), nil)
		require.Nil(t, err)
		require.NotNil(t, shardingState)
		require.NotNil(t, shardingState.Payload)
		require.NotNil(t, shardingState.Payload.ShardingState)
		require.NotNil(t, shardingState.Payload.ShardingState.Collection)
		require.NotNil(t, shardingState.Payload.ShardingState.Shards)
		require.Equal(t, paragraphClass.Class, shardingState.Payload.ShardingState.Collection)
		require.Len(t, shardingState.Payload.ShardingState.Shards, 1)
		require.Equal(t, *shard, shardingState.Payload.ShardingState.Shards[0].Shard)
		require.Len(t, shardingState.Payload.ShardingState.Shards[0].Replicas, 1)
	})

	t.Run("get sharding state for non-existing collection", func(t *testing.T) {
		collection := "non-existing"
		_, err := helper.Client(t).Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().WithCollection(&collection), nil)
		require.Error(t, err)
		require.IsType(t, replication.NewGetCollectionShardingStateNotFound(), err)
	})

	t.Run("get sharding state for non-existing collection and shard", func(t *testing.T) {
		collection := "non-existing"
		shard := "non-existing"
		_, err := helper.Client(t).Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().WithCollection(&collection).WithShard(&shard), nil)
		require.Error(t, err)
		require.IsType(t, replication.NewGetCollectionShardingStateNotFound(), err)
	})
	t.Run("get sharding state for existing collection and non-existing shard", func(t *testing.T) {
		shard := "non-existing"
		_, err := helper.Client(t).Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().WithCollection(&paragraphClass.Class).WithShard(&shard), nil)
		require.Error(t, err)
		require.IsType(t, replication.NewGetCollectionShardingStateNotFound(), err)
	})

	t.Run("create replication operation", func(t *testing.T) {
		created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getRequest(t, paragraphClass.Class)), nil)
		require.Nil(t, err)
		require.NotNil(t, created)
		require.NotNil(t, created.Payload)
		require.NotNil(t, created.Payload.ID)
		id = *created.Payload.ID
	})

	t.Run("get replication operation", func(t *testing.T) {
		details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.NotNil(t, details.Payload.ID)
		require.Equal(t, id, *details.Payload.ID)
	})

	t.Run("get replication operation by collection", func(t *testing.T) {
		details, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithCollection(&paragraphClass.Class), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.Len(t, details.Payload, 1)
		require.NotNil(t, details.Payload[0])
		require.NotNil(t, details.Payload[0].ID)
		require.Equal(t, id, *details.Payload[0].ID)
	})

	t.Run("get replication operation by collection and shard", func(t *testing.T) {
		shard := getRequest(t, paragraphClass.Class).ShardID
		details, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithCollection(&paragraphClass.Class).WithShard(shard), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.Len(t, details.Payload, 1)
		require.NotNil(t, details.Payload[0])
		require.NotNil(t, details.Payload[0].ID)
		require.Equal(t, id, *details.Payload[0].ID)
	})

	t.Run("get replication operation by target node", func(t *testing.T) {
		nodeID := getRequest(t, paragraphClass.Class).DestinationNodeName
		details, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithNodeID(nodeID), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.Len(t, details.Payload, 1)
		require.NotNil(t, details.Payload[0])
		require.NotNil(t, details.Payload[0].ID)
		require.Equal(t, id, *details.Payload[0].ID)
	})

	t.Run("get non-existing replication operation", func(t *testing.T) {
		_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(strfmt.UUID(uuid.New().String())), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewReplicationDetailsNotFound(), err)
	})

	t.Run("get non-existing replication operation by collection", func(t *testing.T) {
		collection := "non-existing"
		_, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithCollection(&collection), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewListReplicationNotFound(), err)
	})

	t.Run("get non-existing replication operation by collection and shard", func(t *testing.T) {
		collection := "non-existing"
		shard := "non-existing"
		_, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithCollection(&collection).WithShard(&shard), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewListReplicationNotFound(), err)
	})

	t.Run("get non-existing replication operation with valid collection and non-existing shard", func(t *testing.T) {
		collection := paragraphClass.Class
		shard := "non-existing"
		_, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithCollection(&collection).WithShard(&shard), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewListReplicationNotFound(), err)
	})

	t.Run("get non-existing replication operation by target node", func(t *testing.T) {
		nodeID := "non-existing"
		_, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams().WithNodeID(&nodeID), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewListReplicationNotFound(), err)
	})

	t.Run("cancel replication operation", func(t *testing.T) {
		cancelled, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(id), nil)
		require.Nil(t, err)
		require.NotNil(t, cancelled)
	})

	t.Run("wait for replication operation to be cancelled", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
			require.Nil(t, err)
			assert.Equal(ct, string(api.CANCELLED), details.Payload.Status.State)
		}, 30*time.Second, 1*time.Second, "replication operation should be cancelled")
	})

	t.Run("delete replication operation", func(t *testing.T) {
		deleted, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(id), nil)
		require.Nil(t, err)
		require.NotNil(t, deleted)
	})

	t.Run("wait for replication operation to be deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
			require.NotNil(ct, err)
			assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
		}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
	})

	t.Run("create one op and immediately delete all replication ops", func(t *testing.T) {
		created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getRequest(t, paragraphClass.Class)), nil)
		require.Nil(t, err)
		require.NotNil(t, created)
		require.NotNil(t, created.Payload)
		require.NotNil(t, created.Payload.ID)
		id = *created.Payload.ID

		deleted, err := helper.Client(t).Replication.DeleteAllReplications(replication.NewDeleteAllReplicationsParams(), nil)
		require.Nil(t, err)
		require.NotNil(t, deleted)
	})

	t.Run("wait for second replication operation to be deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
			require.NotNil(ct, err)
			assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
		}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
	})

	t.Run("assert that there are no replication operations", func(t *testing.T) {
		details, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams(), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.Len(t, details.Payload, 0)
	})
}

func getRequest(t *testing.T, className string) *models.ReplicationReplicateReplicaRequest {
	t.Helper()

	verbose := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), nil)
	require.Nil(t, err)
	transferType := models.ReplicationReplicateReplicaRequestTransferTypeCOPY
	return &models.ReplicationReplicateReplicaRequest{
		CollectionID:        &className,
		SourceNodeName:      &nodes.Payload.Nodes[0].Name,
		DestinationNodeName: &nodes.Payload.Nodes[1].Name,
		ShardID:             &nodes.Payload.Nodes[0].Shards[0].Name,
		TransferType:        &transferType,
	}
}
