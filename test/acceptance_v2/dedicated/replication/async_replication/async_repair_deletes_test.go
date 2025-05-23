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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestAsyncRepairObjectDeleteScenario(t *testing.T) {
	t.Parallel()
	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With3NodeCluster().WithText2VecContextionary()
	})
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	t.Cleanup(cancel)

	clusterSize := 3

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           int64(clusterSize),
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			AsyncEnabled:     true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"

		helper.CreateClassWithClient(t, client, paragraphClass)
	})

	paragraphCount := len(paragraphIDs)

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, paragraphCount)
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}

		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
	})

	node := 2

	t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, node)
	})

	for _, id := range paragraphIDs {
		client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
		toDelete, err := helper.GetObjectCL(t, client, paragraphClass.Class, id, types.ConsistencyLevelOne)
		require.NoError(t, err)
		helper.DeleteObjectCLWithClient(t, client, toDelete.Class, toDelete.ID, types.ConsistencyLevelQuorum)
	}

	t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, node)
	})

	t.Run("verify that all nodes are running", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			client := helper.ClientFromURI(t, compose.GetWeaviate().URI())

			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := client.Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			resp := body.Payload
			require.Len(ct, resp.Nodes, clusterSize)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				assert.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("assert node has objects already deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLGet(t, compose.ContainerURI(node), "Paragraph", types.ConsistencyLevelOne)
			assert.Len(ct, resp, 0)
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated")
	})
}
