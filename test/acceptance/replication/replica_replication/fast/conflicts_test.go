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

package replication

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationReplicateConflictsCOPY() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	helper.DeleteClass(t, paragraphClass.Class)
	helper.CreateClass(t, paragraphClass)

	batch := make([]*models.Object, 10000)
	for i := 0; i < 10000; i++ {
		batch[i] = articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", i)).
			Object()
	}
	helper.CreateObjectsBatch(t, batch)

	req := getRequest(t, paragraphClass.Class)

	var id strfmt.UUID
	t.Run("create COPY replication operation and wait until the replica is in the sharding state", func(t *testing.T) {
		created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(req), nil)
		require.Nil(t, err)
		id = *created.Payload.ID
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			shardingState, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.
					NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.Nil(t, err)
			replicaPresent := false
			for _, shard := range shardingState.Payload.ShardingState.Shards {
				if shard.Shard != *req.ShardID {
					continue
				}
				for _, replica := range shard.Replicas {
					if replica == *req.DestinationNodeName {
						replicaPresent = true
						break
					}
				}
			}
			require.True(ct, replicaPresent)
		}, 60*time.Second, 100*time.Millisecond, "replica should be present in the destination node")
	})

	t.Run("verify uncancelable state when getting details", func(t *testing.T) {
		details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.True(t, details.Payload.Uncancelable)
	})

	t.Run("fail to cancel replication operation due to operation being uncancellable", func(t *testing.T) {
		_, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(id), nil)
		require.NotNil(t, err)
		require.IsType(t, &replication.CancelReplicationConflict{}, err)
	})

	// Wait until the replication operation is READY
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		status, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.Equal(ct, "READY", status.Payload.Status.State)
	}, 180*time.Second, 100*time.Millisecond, "Replication operation should be in READY state")

	t.Run("succeed to delete the replication operation without a conflict", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(id), nil)
		require.Nil(t, err)
	})

	t.Run("ensure target and source shards are still there", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
		require.Nil(t, err)
		foundSrc := false
		foundDst := false
		for _, node := range nodes.Payload.Nodes {
			if *req.SourceNodeName == node.Name {
				for _, shard := range node.Shards {
					if shard.Name == *req.ShardID {
						foundSrc = true
					}
				}
			}
			if *req.DestinationNodeName == node.Name {
				for _, shard := range node.Shards {
					if shard.Name == *req.ShardID {
						foundDst = true
					}
				}
			}
		}
		require.True(t, foundSrc, "source shard should be there")
		require.True(t, foundDst, "destination shard should be there")
	})
}

func (suite *ReplicationTestSuite) TestReplicationReplicateConflictsMOVE() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	helper.DeleteClass(t, paragraphClass.Class)
	helper.CreateClass(t, paragraphClass)

	req := getRequest(t, paragraphClass.Class)

	move := "MOVE"
	req.TransferType = &move
	// Create MOVE replication operation and wait until the shard is in the sharding state (meaning it is uncancellable)
	created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(req), nil)
	require.Nil(t, err)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		shardingState, err := helper.Client(t).Replication.
			GetCollectionShardingState(replication.
				NewGetCollectionShardingStateParams().
				WithCollection(&paragraphClass.Class), nil)
		require.Nil(t, err)
		replicaPresent := false
		for _, shard := range shardingState.Payload.ShardingState.Shards {
			if shard.Shard != *req.ShardID {
				continue
			}
			for _, replica := range shard.Replicas {
				if replica == *req.DestinationNodeName {
					replicaPresent = true
					break
				}
			}
		}
		require.True(ct, replicaPresent)
	}, 60*time.Second, 100*time.Millisecond, "replica should be present in the destination node")
	id := *created.Payload.ID

	t.Run("fail to cancel replication operation due to uncancellable state", func(t *testing.T) {
		_, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(id), nil)
		require.NotNil(t, err)
		require.IsType(t, &replication.CancelReplicationConflict{}, err)
	})

	t.Run("fail to delete replication operation due to uncancellable state", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(id), nil)
		require.NotNil(t, err)
		require.IsType(t, &replication.DeleteReplicationConflict{}, err)
	})

	// Wait until the replication operation is READY
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		status, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.Equal(ct, "READY", status.Payload.Status.State)
	}, 180*time.Second, 100*time.Millisecond, "Replication operation should be in READY state")

	t.Run("succeed to delete the replication operation without a conflict", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(id), nil)
		require.Nil(t, err)
	})

	// Ensure that the source shard is still there and the destination shard is there
	t.Run("ensure target and source shards are still there", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
		require.Nil(t, err)
		foundSrc := false
		foundDst := false
		for _, node := range nodes.Payload.Nodes {
			if *req.SourceNodeName == node.Name {
				for _, shard := range node.Shards {
					if shard.Name == *req.ShardID {
						foundSrc = true
					}
				}
			}
			if *req.DestinationNodeName == node.Name {
				for _, shard := range node.Shards {
					if shard.Name == *req.ShardID {
						foundDst = true
					}
				}
			}
		}
		require.False(t, foundSrc, "source replica should not be there")
		require.True(t, foundDst, "destination replica should be there")
	})
}
