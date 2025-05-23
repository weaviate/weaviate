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
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestReadRepair(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	t.Cleanup(cancel)

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With3NodeCluster().WithText2VecContextionary()
	})

	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("CreateSchema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClassWithClient(t, client, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		helper.CreateClassWithClient(t, client, articleClass)
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
		time.Sleep(time.Second)
	})

	t.Run("TriggerRepairQuorumOnNode-3", func(t *testing.T) {
		resp, err := common.GetObjectCL(t, compose.ContainerURI(3),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelQuorum)
		require.Nil(t, err)
		require.Equal(t, repairObj.ID, resp.ID)
		require.Equal(t, repairObj.Class, resp.Class)
		require.EqualValues(t, repairObj.Properties, resp.Properties)
		require.EqualValues(t, repairObj.Vector, resp.Vector)
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
	})

	t.Run("TriggerRepairAllOnNode1", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.ContainerURI(1),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelAll)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("UpdatedObjectRepairedOnNode-3", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.ContainerURI(3),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)

		resp, err := common.GetObjectCL(t, compose.ContainerURI(1),
			repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.Equal(t, replaceObj.ID, resp.ID)
		require.Equal(t, replaceObj.Class, resp.Class)
		require.EqualValues(t, replaceObj.Properties, resp.Properties)
		require.EqualValues(t, replaceObj.Vector, resp.Vector)
	})

	t.Run("stop node2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	t.Run("delete article with consistency level ONE and node2 down", func(t *testing.T) {
		client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
		helper.DeleteObjectCLWithClient(t, client, replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
	})

	t.Run("restart node2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	t.Run("deleted article should be present in node2", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("run exists to trigger read repair with deleted object resolution", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelAll)
		require.Nil(t, err)
		require.False(t, exists)
	})

	t.Run("deleted article should still be present in node2 (object deletion is not resolved)", func(t *testing.T) {
		exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
		require.Nil(t, err)
		require.True(t, exists)
	})
}
