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

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReadRepair() {
	t := suite.T()
	mainCtx := context.Background()

	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)
	defer cancel()

	compose := suite.compose

	helper.SetupClient(compose.ContainerURI(1))
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("CreateSchema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           3,
			DeletionStrategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           3,
			DeletionStrategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
		}
		helper.CreateClass(t, articleClass)
	})

	time.Sleep(time.Second) // remove once eventual consistency has been addressed

	t.Run("InsertParagraphs/Node-1", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.ContainerURI(1), batch)
	})

	t.Run("InsertArticles/Node-3", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.ContainerURI(3), batch)
	})

	t.Run("StopNode-3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	repairObj := models.Object{
		ID:    "e5390693-5a22-44b8-997d-2a213aaf5884",
		Class: "Paragraph",
		Properties: map[string]interface{}{
			"contents": "a new paragraph",
		},
	}
	t.Run("AddObjectToNode-1", func(t *testing.T) {
		common.CreateObjectCL(t, compose.ContainerURI(1), &repairObj, types.ConsistencyLevelOne)
	})

	t.Run("RestartNode-3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
		common.WaitForNodeReadyForClass(t, compose.ContainerURI(3), "Paragraph", paragraphIDs[0])
	})

	t.Run("TriggerRepairQuorumOnNode-3", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			// re-trigger read repair on every attempt; node 3 may still be loading right after restart
			resp, err := common.GetObjectCL(t, compose.ContainerURI(3),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelQuorum)
			require.Nil(collect, err)
			require.Equal(collect, repairObj.ID, resp.ID)
			require.Equal(collect, repairObj.Class, resp.Class)
			require.EqualValues(collect, repairObj.Properties, resp.Properties)
			require.EqualValues(collect, repairObj.Vector, resp.Vector)
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("StopNode-3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	replaceObj := repairObj
	replaceObj.Properties = map[string]interface{}{
		"contents": "this paragraph was replaced",
	}

	t.Run("ReplaceObjectOneOnNode2", func(t *testing.T) {
		common.UpdateObjectCL(t, compose.ContainerURI(2), &replaceObj, types.ConsistencyLevelOne)
	})

	t.Run("RestartNode-3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
		common.WaitForNodeReadyForClass(t, compose.ContainerURI(3), "Paragraph", paragraphIDs[0])
	})

	t.Run("TriggerRepairAllOnNode1", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			// re-trigger read repair on every attempt; node 3 may still be loading right after restart
			exists, err := common.ObjectExistsCL(t, compose.ContainerURI(1),
				replaceObj.Class, replaceObj.ID, types.ConsistencyLevelAll)
			require.Nil(collect, err)
			require.True(collect, exists)
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("UpdatedObjectRepairedOnNode-3", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			exists, err := common.ObjectExistsCL(t, compose.ContainerURI(3),
				replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
			require.Nil(collect, err)
			require.True(collect, exists)

			resp, err := common.GetObjectCL(t, compose.ContainerURI(3),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			require.Nil(collect, err)
			require.Equal(collect, replaceObj.ID, resp.ID)
			require.Equal(collect, replaceObj.Class, resp.Class)
			require.EqualValues(collect, replaceObj.Properties, resp.Properties)
			require.EqualValues(collect, replaceObj.Vector, resp.Vector)
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("stop node2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	t.Run("delete article with consistency level ONE and node2 down", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())
		helper.DeleteObjectCL(t, replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
	})

	t.Run("restart node2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
		common.WaitForNodeReadyForClass(t, compose.GetWeaviateNode2().URI(), "Paragraph", paragraphIDs[0])
	})

	t.Run("deleted article should be present in node2", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
				replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
			require.Nil(collect, err)
			require.True(collect, exists)
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("run exists to trigger read repair with deleted object resolution", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
				replaceObj.Class, replaceObj.ID, types.ConsistencyLevelAll)
			require.Nil(collect, err)
			require.False(collect, exists)
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("deleted article should still be present in node2 (object deletion is not resolved)", func(t *testing.T) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
				replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
			require.Nil(collect, err)
			require.True(collect, exists)
		}, 30*time.Second, 500*time.Millisecond)
	})
}
