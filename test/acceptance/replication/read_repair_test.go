//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
		WithWeaviateCluster().
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminte test containers: %s", err.Error())
		}
	}()

	// create classes
	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		helper.CreateClass(t, articleClass)
	})

	// create objects

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		createObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("insert articles", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		createObjects(t, compose.GetWeaviateNode2().URI(), batch)
	})

	// take a node down
	t.Run("stop node 2", func(t *testing.T) {
		stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		time.Sleep(10 * time.Second)
	})

	repairObj := models.Object{
		ID:    "e5390693-5a22-44b8-997d-2a213aaf5884",
		Class: "Paragraph",
		Properties: map[string]interface{}{
			"contents": "a new paragraph",
		},
		Vector: []float32{1, 2, 3, 4, 5},
	}

	// add new object on remaining node
	t.Run("add new object to node one", func(t *testing.T) {
		createObjectCL(t, compose.GetWeaviate().URI(), &repairObj, replica.One)
	})

	// restart downed node
	t.Run("restart node 2", func(t *testing.T) {
		err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
		require.Nil(t, err)
	})

	// read request object
	t.Run("run fetch to trigger read repair", func(t *testing.T) {
		resp, err := getObject(t, compose.GetWeaviate().URI(), repairObj.Class, repairObj.ID)
		require.Nil(t, err)
		// TODO: replace with resp assertions
		t.Logf("resp: %+v", resp)
	})

	// assert that repair was made
	t.Run("assert read repair was made", func(t *testing.T) {
		stopNode(ctx, t, compose, compose.GetWeaviate().Name())
		time.Sleep(10 * time.Second)

		resp, err := getObjectCL(t, compose.GetWeaviateNode2().URI(), repairObj.Class, repairObj.ID, replica.One)
		require.Nil(t, err)
		assert.Equal(t, repairObj.ID, resp.ID)
		assert.Equal(t, repairObj.Class, resp.Class)
		assert.EqualValues(t, repairObj.Properties, resp.Properties)
	})
}
