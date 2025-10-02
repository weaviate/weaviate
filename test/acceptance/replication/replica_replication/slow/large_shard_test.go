//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package slow

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationReplicateOfLargeShard() {
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

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: 1,
	}
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	t.Log("Loading data into tenant...")
	tenantName := "tenant"
	batch := make([]*models.Object, 0, 10000)
	start := time.Now()
	for j := 0; j < 1000000; j++ {
		batch = append(batch, articles.
			NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", j)).
			WithTenant(tenantName).
			Object(),
		)
		if len(batch) == 10000 {
			helper.CreateObjectsBatch(t, batch)
			t.Logf("Loaded %d objects", len(batch))
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		helper.CreateObjectsBatch(t, batch)
		t.Logf("Loaded remaining %d objects", len(batch))
	}
	t.Logf("Data loading took %s", time.Since(start))

	nodes, err := helper.Client(t).Nodes.NodesGet(nil, nil)
	require.Nil(t, err)

	nodeNames := make([]string, len(nodes.GetPayload().Nodes))
	for i, node := range nodes.GetPayload().Nodes {
		nodeNames[i] = node.Name
	}

	// Find node with shard
	shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(
		replication.NewGetCollectionShardingStateParams().WithCollection(&cls.Class), nil,
	)
	require.Nil(t, err)
	require.Len(t, shardingState.GetPayload().ShardingState.Shards, 1)

	sourceNode := shardingState.GetPayload().ShardingState.Shards[0].Replicas[0]

	targetNode := ""
	for _, node := range nodeNames {
		if node != sourceNode {
			targetNode = node
			break
		}
	}

	move := models.ReplicationReplicateReplicaRequestTypeMOVE
	t.Logf("Replicating from %s to %s", sourceNode, targetNode)
	// Replicate the shard
	res, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().
			WithBody(&models.ReplicationReplicateReplicaRequest{
				SourceNode: &sourceNode,
				TargetNode: &targetNode,
				Collection: &cls.Class,
				Shard:      &shardingState.GetPayload().ShardingState.Shards[0].Shard,
				Type:       &move,
			}), nil,
	)
	require.Nil(t, err)
	id := *res.GetPayload().ID

	t.Logf("Replication started with ID: %s", id)
	// Wait for replication to finish
	t.Log("Waiting for replication to finish...")
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		status, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.Equal(ct, "READY", status.Payload.Status.State)
	}, 180*time.Second, 100*time.Millisecond, "Replication operation should be in READY state")

	t.Log("Replication completed successfully")
	// Verify that the target node has the shard
	shardingState, err = helper.Client(t).Replication.GetCollectionShardingState(
		replication.NewGetCollectionShardingStateParams().WithCollection(&cls.Class), nil,
	)
	require.Nil(t, err)
	require.Len(t, shardingState.GetPayload().ShardingState.Shards, 1)
	require.Equal(t, targetNode, shardingState.GetPayload().ShardingState.Shards[0].Replicas[0])
}
