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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func multiShardScaleOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster().
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ShardingConfig = map[string]interface{}{
		"desiredCount": 1,
	}
	articleClass := articles.ArticlesClass()
	articleClass.ShardingConfig = map[string]interface{}{
		"desiredCount": 1,
	}

	t.Run("create schema", func(t *testing.T) {
		helper.CreateClass(t, paragraphClass)
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

	t.Run("add references", func(t *testing.T) {
		refs := make([]*models.BatchReference, len(articleIDs))
		for i := range articleIDs {
			refs[i] = &models.BatchReference{
				From: strfmt.URI(crossref.NewSource("Article", "hasParagraphs", articleIDs[i]).String()),
				To:   strfmt.URI(crossref.NewLocalhost("Paragraph", paragraphIDs[i]).String()),
			}
		}
		addReferences(t, compose.GetWeaviate().URI(), refs)
	})

	t.Run("scale out paragraphs", func(t *testing.T) {
		c := getClass(t, compose.GetWeaviate().URI(), paragraphClass.Class)
		c.ReplicationConfig.Factor = 2
		updateClass(t, compose.GetWeaviate().URI(), c)
	})

	t.Run("assert paragraphs were scaled out", func(t *testing.T) {
		n := getNodes(t, compose.GetWeaviate().URI())
		var shardsFound int
		for _, node := range n.Nodes {
			for _, shard := range node.Shards {
				if shard.Class == paragraphClass.Class {
					assert.EqualValues(t, 10, shard.ObjectCount)
					shardsFound++
				}
			}
		}
		assert.Equal(t, 2, shardsFound)
	})

	t.Run("scale out articles", func(t *testing.T) {
		c := getClass(t, compose.GetWeaviate().URI(), articleClass.Class)
		c.ReplicationConfig.Factor = 2
		updateClass(t, compose.GetWeaviate().URI(), c)
	})

	t.Run("assert articles were scaled out", func(t *testing.T) {
		n := getNodes(t, compose.GetWeaviate().URI())
		var shardsFound int
		for _, node := range n.Nodes {
			for _, shard := range node.Shards {
				if shard.Class == articleClass.Class {
					assert.EqualValues(t, 10, shard.ObjectCount)
					shardsFound++
				}
			}
		}
		assert.Equal(t, 2, shardsFound)
	})

	t.Run("kill a node and check contents of remaining node", func(t *testing.T) {
		stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		p := gqlGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, replica.One)
		assert.Len(t, p, 10)
		a := gqlGet(t, compose.GetWeaviate().URI(), articleClass.Class, replica.One)
		assert.Len(t, a, 10)
	})
}
