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
	"math/rand"
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

func TestAsyncRepairObjectUpdateScenario(t *testing.T) {
	t.Parallel()
	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With3NodeCluster().WithText2VecContextionary()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	t.Cleanup(cancel)

	clusterSize := 3
	paragraphClass := articles.ParagraphsClass()
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(clusterSize),
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"

		helper.CreateClassWithClient(t, client, paragraphClass)
	})

	node := 2

	t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, node)
	})

	t.Run("upsert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}

		// choose one more node to insert the objects into
		var targetNode int
		for {
			targetNode = 1 + rand.Intn(clusterSize)
			if targetNode != node {
				break
			}
		}

		common.CreateObjectsCL(t, compose.GetWeaviateNode(targetNode).URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, node)
		time.Sleep(5 * time.Second)
	})

	t.Run("verify that all nodes are running", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
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

	t.Run(fmt.Sprintf("assert node %d has all the objects at its latest version", node), func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			count := common.CountObjects(t, compose.GetWeaviateNode(node).URI(), paragraphClass.Class)
			assert.EqualValues(ct, len(paragraphIDs), count)

			for i, id := range paragraphIDs {
				resp, err := common.GetObjectCL(t, compose.GetWeaviateNode(node).URI(), paragraphClass.Class, id, types.ConsistencyLevelOne)
				assert.NoError(ct, err)
				assert.NotNil(ct, resp)

				if resp == nil {
					continue
				}

				assert.Equal(ct, id, resp.ID)

				props := resp.Properties.(map[string]interface{})
				props["contents"] = fmt.Sprintf("paragraph#%d", i)
			}
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated")
	})
}
