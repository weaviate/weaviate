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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func (suite *AsyncReplicationTestSuite) TestAsyncRepairObjectDeleteScenario() {
	t := suite.T()
	mainCtx := context.Background()

	clusterSize := 3

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           int64(clusterSize),
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			AsyncEnabled:     true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	paragraphCount := 5

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, paragraphCount)
		for i, id := range paragraphIDs[:paragraphCount] {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}

		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, replica.All)
	})

	objectNotDeletedAt := make(map[strfmt.UUID]int)

	for _, id := range paragraphIDs[:paragraphCount] {
		// pick one node to be down during upserts
		node := 2 + rand.Intn(clusterSize-1)

		t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, node)
		})

		objectNotDeletedAt[id] = node

		// choose one more node to delete the object
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

		helper.DeleteObjectCL(t, toDelete.Class, toDelete.ID, replica.Quorum)

		t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
			common.StartNodeAt(ctx, t, compose, node)
			time.Sleep(5 * time.Second)
		})
	}

	// wait for some time for async replication to propagate deleted objects
	t.Run("assert each node has all the objects at its latest version when object was not deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, id := range paragraphIDs[:paragraphCount] {
				node := objectNotDeletedAt[id]

				resp, err := common.ObjectExistsCL(t, compose.GetWeaviateNode(node).URI(), paragraphClass.Class, id, replica.Quorum)
				assert.NoError(ct, err)
				assert.False(ct, resp)
			}
		}, 30*time.Second, 500*time.Millisecond, "not all the objects have been asynchronously replicated")
	})
}
