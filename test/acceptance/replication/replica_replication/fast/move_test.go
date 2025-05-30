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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationReplicateMOVEDeletesSourceReplica() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	helper.DeleteClass(t, paragraphClass.Class)
	helper.CreateClass(t, paragraphClass)

	// Create paragraphs
	batch := make([]*models.Object, 10000)
	for i := 0; i < 10000; i++ {
		batch[i] = articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", i)).
			Object()
	}
	helper.CreateObjectsBatch(t, batch)

	req := getRequest(t, paragraphClass.Class)

	move := "MOVE"
	req.TransferType = &move
	// Create MOVE replication operation and wait until the shard is in the sharding state (meaning it is uncancellable)
	created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(req), nil)
	require.Nil(t, err)
	id := *created.Payload.ID

	// Wait until the replication operation is READY
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		status, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.Equal(ct, "READY", status.Payload.Status.State)
	}, 180*time.Second, 100*time.Millisecond, "Replication operation should be in READY state")

	t.Run("ensure target and source replicas are there/gone respectively", func(t *testing.T) {
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
