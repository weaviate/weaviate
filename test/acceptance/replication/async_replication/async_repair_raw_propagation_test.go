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
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestAsyncRepairRawPropagation exercises the raw-bytes propagation path
// (ASYNC_REPLICATION_RAW_PROPAGATION=true): a node misses a batch while down,
// then on restart must converge by receiving the raw on-disk object bytes. It
// asserts not just convergence but that properties and vectors survive the
// round-trip, proving the object store, inverted index and vector index were
// rebuilt correctly from the raw payload.
func TestAsyncRepairRawPropagation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clusterSize := 3

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithWeaviateEnv("ASYNC_REPLICATION_RAW_PROPAGATION", "true").
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{
		Factor:       int64(clusterSize),
		AsyncEnabled: true,
	}
	paragraphClass.Vectorizer = "text2vec-contextionary"

	t.Run("create schema", func(t *testing.T) {
		helper.CreateClass(t, paragraphClass)
	})

	node := 2

	t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, node)
	})

	expectedContents := make(map[string]struct{}, len(paragraphIDs))
	t.Run("insert paragraphs while node is down", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i := range paragraphIDs {
			contents := fmt.Sprintf("paragraph#%d", i)
			expectedContents[contents] = struct{}{}
			batch[i] = articles.NewParagraph().
				WithID(paragraphIDs[i]).
				WithContents(contents).
				Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, node)
	})

	t.Run("verify all nodes healthy", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, clusterSize)
			for _, n := range body.Payload.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("recovered node has all objects with intact properties and vectors", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLGet(t, compose.ContainerURI(node), "Paragraph",
				types.ConsistencyLevelOne, "contents", "_additional{id vector}")
			require.Len(ct, resp, len(paragraphIDs))

			gotContents := make(map[string]struct{}, len(resp))
			for _, obj := range resp {
				m, ok := obj.(map[string]interface{})
				require.True(ct, ok)

				contents, ok := m["contents"].(string)
				require.True(ct, ok)
				gotContents[contents] = struct{}{}

				additional, ok := m["_additional"].(map[string]interface{})
				require.True(ct, ok)
				vec, ok := additional["vector"].([]interface{})
				require.True(ct, ok)
				require.NotEmpty(ct, vec)
			}
			require.Equal(ct, expectedContents, gotContents)
		}, 120*time.Second, 5*time.Second, "objects not converged via raw propagation")
	})
}
