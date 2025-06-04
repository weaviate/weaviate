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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationForceDeleteOperations() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()
	// Create the class
	helper.DeleteClass(t, paragraphClass.Class)
	helper.CreateClass(t, paragraphClass)

	// Load data
	batch := make([]*models.Object, 10000)
	for i := 0; i < 10000; i++ {
		batch[i] = articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", i)).
			Object()
	}
	helper.CreateObjectsBatch(t, batch)

	howManyNodes := 3
	shardsPerNode := make(map[string]string, howManyNodes)

	t.Run("start replication operations", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(paragraphClass.Class).WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.Len(t, nodes.Payload.Nodes, 3)
		for i, src := range nodes.Payload.Nodes {
			for j, tgt := range nodes.Payload.Nodes {
				if i != (j+1)%howManyNodes {
					continue
				}
				shard := src.Shards[0]
				shardsPerNode[src.Name] = shard.Name
				_, err := helper.Client(t).Replication.Replicate(
					replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
						SourceNodeName:      &src.Name,
						DestinationNodeName: &tgt.Name,
						ShardID:             &shard.Name,
						CollectionID:        &paragraphClass.Class,
					}),
					nil,
				)
				require.Nil(t, err, "failed to start replication from %s to %s", src.Name, tgt.Name)
			}
		}
	})

	t.Run(fmt.Sprintf("assert that %d operations were started", howManyNodes), func(t *testing.T) {
		resp, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams(),
			nil,
		)
		require.Nil(t, err, "failed to list replication details")
		require.Len(t, resp.Payload, howManyNodes, "there should be %d replication operations started", howManyNodes)
	})

	t.Run("force delete all operations with dryRun=true", func(t *testing.T) {
		dryRun := true
		resp, err := helper.Client(t).Replication.ForceDeleteReplications(
			replication.NewForceDeleteReplicationsParams().WithBody(&models.ReplicationReplicateForceDeleteRequest{
				DryRun: &dryRun,
			}),
			nil,
		)
		require.Nil(t, err, "failed to force delete all replication operations")
		// calling forceDelete with dryRun=true should return the UUIDs of the operations that would be deleted
		require.Len(t, resp.Payload.Deleted, howManyNodes, "there should be %d replication operations to be deleted", howManyNodes)
		require.Equal(t, resp.Payload.DryRun, true, "dry run should be false, we are not in dry run mode")
	})

	t.Run("force delete all operations dryRun=false", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.ForceDeleteReplications(
			replication.NewForceDeleteReplicationsParams(),
			nil,
		)
		require.Nil(t, err, "failed to force delete all replication operations")
		// calling forceDelete with dryRun=false cannot return the UUIDs of the operations that were deleted so we expect an empty slice
		require.Len(t, resp.Payload.Deleted, 0, "there should be %d replication operations deleted", 0)
		require.Equal(t, resp.Payload.DryRun, false, "dry run should be false, we are not in dry run mode")
	})

	t.Run("assert that there are no replication operations left", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams(),
			nil,
		)
		require.Nil(t, err, "failed to list replication details")
		require.Len(t, resp.Payload, 0, "there should be no replication operations left after force delete")
	})

	t.Run("assert that the sharding state is still intact", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		resp, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithClassName(paragraphClass.Class).WithOutput(&verbose),
			nil,
		)
		require.Nil(t, err, "failed to get nodes for class %s", paragraphClass.Class)
		require.Len(t, resp.Payload.Nodes, howManyNodes, "there should be %d nodes for class %s", howManyNodes, paragraphClass.Class)

		for _, node := range resp.Payload.Nodes {
			require.Len(t, node.Shards, 1, "each node should have exactly one shard")
			require.Equal(t, shardsPerNode[node.Name], node.Shards[0].Name, "shard name should match the one we started with")
		}
	})
}
