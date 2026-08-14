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

// Package rawpropagation isolates the propagation e2e in its own go-test
// invocation (separate timeout budget) from the main async suite.
package rawpropagation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
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

var paragraphIDs = []strfmt.UUID{
	strfmt.UUID("3bf331ac-8c86-4f95-b127-2f8f96bbc093"),
	strfmt.UUID("47b26ba1-6bc9-41f8-a655-8b9a5b60e1a3"),
	strfmt.UUID("5fef6289-28d2-4ea2-82a9-48eb501200cd"),
	strfmt.UUID("34a673b4-8859-4cb4-bb30-27f5622b47e9"),
	strfmt.UUID("9fa362f5-c2dc-4fb8-b5b2-11701adc5f75"),
	strfmt.UUID("63735238-6723-4caf-9eaa-113120968ff4"),
	strfmt.UUID("2236744d-b2d2-40e5-95d8-2574f20a7126"),
	strfmt.UUID("1a54e25d-aaf9-48d2-bc3c-bef00b556297"),
	strfmt.UUID("0b8a0e70-a240-44b2-ac6d-26dda97523b9"),
	strfmt.UUID("50566856-5d0a-4fb1-a390-e099bc236f66"),
}

// TestAsyncRepairObjectPropagation exercises the node-recovery convergence path,
// which always ships raw on-disk object bytes, and asserts properties and
// vectors survive the round-trip on the recovered node.
func TestAsyncRepairObjectPropagation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clusterSize := 3

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{
		Factor: int64(clusterSize),
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
