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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReadRepairTimebasedResolution() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
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

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           3,
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           3,
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("insert articles", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviateNode2().URI(), batch)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	repairObj := models.Object{
		ID:    "e5390693-5a22-44b8-997d-2a213aaf5884",
		Class: "Paragraph",
		Properties: map[string]interface{}{
			"contents": "a new paragraph",
		},
	}

	t.Run("add new object to node1 with node2 down", func(t *testing.T) {
		common.CreateObjectCL(t, compose.GetWeaviate().URI(), &repairObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	t.Run("run fetch to trigger read repair", func(t *testing.T) {
		_, err := common.GetObjectCL(t, compose.GetWeaviate().URI(), repairObj.Class, repairObj.ID, types.ConsistencyLevelAll)
		require.Nil(t, err)
	})

	t.Run("require new object read repair was made", func(t *testing.T) {
		resp, err := common.GetObjectCL(t, compose.GetWeaviateNode2().URI(),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.Equal(t, repairObj.ID, resp.ID)
		require.Equal(t, repairObj.Class, resp.Class)
		require.EqualValues(t, repairObj.Properties, resp.Properties)
		require.EqualValues(t, repairObj.Vector, resp.Vector)
	})

	replaceObj := repairObj
	replaceObj.Properties = map[string]interface{}{
		"contents": "this paragraph was replaced",
	}

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	t.Run("replace object", func(t *testing.T) {
		common.UpdateObjectCL(t, compose.GetWeaviateNode2().URI(), &replaceObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("run exists to trigger read repair", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelAll)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("require updated object read repair was made", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode3().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)

		resp, err := common.GetObjectCL(t, compose.GetWeaviateNode3().URI(),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.Equal(t, replaceObj.ID, resp.ID)
		require.Equal(t, replaceObj.Class, resp.Class)
		require.EqualValues(t, replaceObj.Properties, resp.Properties)
		require.EqualValues(t, replaceObj.Vector, resp.Vector)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	t.Run("delete article with consistency level ONE and node2 down", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())
		helper.DeleteObjectCL(t, replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
	})

	t.Run("restart node 2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	t.Run("stop node3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	replaceObj.Properties = map[string]interface{}{
		"contents": "this paragraph was replaced for second time",
	}

	t.Run("replace object in node2", func(t *testing.T) {
		err := common.UpdateObjectCL(t, compose.GetWeaviateNode2().URI(), &replaceObj, types.ConsistencyLevelOne)
		require.Nil(t, err)
	})

	t.Run("deleted article should be present in node2", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("deleted article should not be present in node3", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode3().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.False(t, exists)
	})

	t.Run("run exists to trigger read repair with deleted object resolution", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelAll)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("deleted article should be present in node2", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("deleted article should be present in node3", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode3().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)
	})
}
