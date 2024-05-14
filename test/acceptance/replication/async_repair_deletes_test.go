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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func asyncRepairObjectDeleteScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clusterSize := 3

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(clusterSize),
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	itCount := 2
	paragraphCount := 2

	for it := 0; it < itCount; it++ {
		// pick one node to be down during upserts
		node := 2 + rand.Intn(clusterSize-1)

		t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
			stopNodeAt(ctx, t, compose, node)
		})

		t.Run("upsert paragraphs", func(t *testing.T) {
			batch := make([]*models.Object, paragraphCount)
			for i, id := range paragraphIDs[:paragraphCount] {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d_%d", it, i)).
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

			createObjectsCL(t, compose.GetWeaviateNode(targetNode).URI(), batch, replica.One)
		})

		t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
			err := compose.StartAt(ctx, node)
			require.NoError(t, err)
		})
	}

	// wait for some time for async replication to repair missing object
	time.Sleep(3 * time.Second)

	objectNotDeletedAt := make(map[strfmt.UUID]int)

	for _, id := range paragraphIDs[:paragraphCount] {
		// pick one node to be down during upserts
		node := 2 + rand.Intn(clusterSize-1)

		t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
			stopNodeAt(ctx, t, compose, node)
		})

		objectNotDeletedAt[id] = node

		// choose one more node to insert the objects into
		var targetNode int
		for {
			targetNode = 1 + rand.Intn(clusterSize)
			if targetNode != node {
				break
			}
		}

		host := compose.GetWeaviateNode(targetNode).URI()

		helper.SetupClient(host)

		toDelete, err := helper.GetObjectCL(t, paragraphClass.Class, id, replica.One)
		require.NoError(t, err)

		helper.DeleteObjectCL(t, toDelete, replica.Quorum)

		t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
			err := compose.StartAt(ctx, node)
			require.NoError(t, err)
		})
	}

	// wait for some time for async replication to repair missing object
	time.Sleep(3 * time.Second)

	t.Run("assert each node has all the objects at its latest version when object was not deleted", func(t *testing.T) {
		for i, id := range paragraphIDs[:paragraphCount] {
			node, notDeleted := objectNotDeletedAt[id]
			if notDeleted {
				resp, err := getObjectCL(t, compose.GetWeaviateNode(node).URI(), paragraphClass.Class, id, replica.One)
				require.NoError(t, err)
				require.Equal(t, id, resp.ID)

				props := resp.Properties.(map[string]interface{})
				props["contents"] = fmt.Sprintf("paragraph#%d_%d", itCount, i)
			} else {
				resp, err := objectExistsCL(t, compose.GetWeaviateNode(1+(node+1)%clusterSize).URI(), paragraphClass.Class, id, replica.Quorum)
				require.NoError(t, err)
				require.False(t, resp)
			}
		}
	})
}
