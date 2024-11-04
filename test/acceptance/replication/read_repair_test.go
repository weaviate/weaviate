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
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func readRepair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.ContainerURI(1))
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("CreateSchema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
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
		createObjects(t, compose.ContainerURI(1), batch)
	})

	t.Run("InsertArticles/Node-3", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		createObjects(t, compose.ContainerURI(3), batch)
	})

	t.Run("StopNode-3", func(t *testing.T) {
		stopNodeAt(ctx, t, compose, 3)
	})

	repairObj := models.Object{
		ID:    "e5390693-5a22-44b8-997d-2a213aaf5884",
		Class: "Paragraph",
		Properties: map[string]interface{}{
			"contents": "a new paragraph",
		},
	}
	t.Run("AddObjectToNode-1", func(t *testing.T) {
		createObjectCL(t, compose.ContainerURI(1), &repairObj, replica.One)
	})

	t.Run("RestartNode-3", func(t *testing.T) {
		startNodeAt(ctx, t, compose, 3)
		time.Sleep(time.Second)
	})

	t.Run("TriggerRepairQuorumOnNode-3", func(t *testing.T) {
		resp, err := getObjectCL(t, compose.ContainerURI(3),
			repairObj.Class, repairObj.ID, replica.Quorum)
		require.Nil(t, err)
		assert.Equal(t, repairObj.ID, resp.ID)
		assert.Equal(t, repairObj.Class, resp.Class)
		assert.EqualValues(t, repairObj.Properties, resp.Properties)
		assert.EqualValues(t, repairObj.Vector, resp.Vector)
	})

	t.Run("StopNode-3", func(t *testing.T) {
		stopNodeAt(ctx, t, compose, 3)
	})

	replaceObj := repairObj
	replaceObj.Properties = map[string]interface{}{
		"contents": "this paragraph was replaced",
	}

	t.Run("ReplaceObjectOneOnNode2", func(t *testing.T) {
		updateObjectCL(t, compose.ContainerURI(2), &replaceObj, replica.One)
	})

	t.Run("RestartNode-3", func(t *testing.T) {
		startNodeAt(ctx, t, compose, 3)
	})

	t.Run("TriggerRepairAllOnNode1", func(t *testing.T) {
		exists, err := objectExistsCL(t, compose.ContainerURI(1),
			replaceObj.Class, replaceObj.ID, replica.All)
		require.Nil(t, err)
		require.True(t, exists)
	})

	t.Run("UpdatedObjectRepairedOnNode-3", func(t *testing.T) {
		exists, err := objectExistsCL(t, compose.ContainerURI(3),
			replaceObj.Class, replaceObj.ID, replica.One)
		require.Nil(t, err)
		require.True(t, exists)

		resp, err := getObjectCL(t, compose.ContainerURI(1),
			repairObj.Class, repairObj.ID, replica.One)
		require.Nil(t, err)
		assert.Equal(t, replaceObj.ID, resp.ID)
		assert.Equal(t, replaceObj.Class, resp.Class)
		assert.EqualValues(t, replaceObj.Properties, resp.Properties)
		assert.EqualValues(t, replaceObj.Vector, resp.Vector)
	})

	t.Run("delete article with consistency level ONE and node2 down", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())
		helper.DeleteObjectCL(t, replaceObj.Class, replaceObj.ID, replica.One)
	})

	t.Run("restart node 2", func(t *testing.T) {
		err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
		require.Nil(t, err)
	})

	t.Run("run exists to trigger read repair with deleted object resolution", func(t *testing.T) {
		exists, err := objectExistsCL(t, compose.GetWeaviateNode2().URI(),
			replaceObj.Class, replaceObj.ID, replica.All)
		require.Nil(t, err)
		require.False(t, exists)
	})
}
