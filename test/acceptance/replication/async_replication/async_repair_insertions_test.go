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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func (suite *AsyncReplicationTestSuite) TestAsyncRepairObjectInsertionScenario() {
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
			Factor:       int64(clusterSize),
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	node := 2

	t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, node)
	})

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}

		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, replica.One)
	})

	t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, node)
		time.Sleep(5 * time.Second)
	})

	t.Run(fmt.Sprintf("assert node %d has all the objects", node), func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			count := common.CountObjects(t, compose.GetWeaviateNode(node).URI(), paragraphClass.Class)
			assert.EqualValues(ct, len(paragraphIDs), count)
		}, 60*time.Second, 1*time.Second, "not all the objects have been asynchronously replicated")
	})
}
