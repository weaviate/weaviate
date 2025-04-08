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
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationFactorIncrease() {
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

	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ShardingConfig = map[string]interface{}{
		"desiredCount": 1,
	}
	articleClass := articles.ArticlesClass()
	articleClass.ShardingConfig = map[string]interface{}{
		"desiredCount": 1,
	}

	t.Run("CreateSchema", func(t *testing.T) {
		helper.CreateClass(t, paragraphClass)
		helper.CreateClass(t, articleClass)
	})

	t.Run("InsertParagraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("InsertArticles", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviateNode(2).URI(), batch)
	})

	t.Run("AddReferences", func(t *testing.T) {
		refs := make([]*models.BatchReference, len(articleIDs))
		for i := range articleIDs {
			refs[i] = &models.BatchReference{
				From: strfmt.URI(crossref.NewSource("Article", "hasParagraphs", articleIDs[i]).String()),
				To:   strfmt.URI(crossref.NewLocalhost("Paragraph", paragraphIDs[i]).String()),
			}
		}
		common.AddReferences(t, compose.GetWeaviate().URI(), refs)
	})

	t.Run("scale out paragraphs", func(t *testing.T) {
		c := common.GetClass(t, compose.GetWeaviate().URI(), paragraphClass.Class)
		c.ReplicationConfig.Factor = 2
		common.UpdateClass(t, compose.GetWeaviate().URI(), c)
	})

	t.Run("assert paragraphs were scaled out", func(t *testing.T) {
		// shard.ObjectCount is eventually consistent, see Bucket::CountAsync()
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			n := common.GetNodes(t, compose.GetWeaviate().URI())
			var shardsFound int
			for _, node := range n.Nodes {
				for _, shard := range node.Shards {
					if shard.Class == paragraphClass.Class {
						assert.EqualValues(collect, int64(10), shard.ObjectCount)
						shardsFound++
					}
				}
			}
			assert.Equal(collect, 2, shardsFound)
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("scale out articles", func(t *testing.T) {
		c := common.GetClass(t, compose.GetWeaviate().URI(), articleClass.Class)
		c.ReplicationConfig.Factor = 2
		common.UpdateClass(t, compose.GetWeaviate().URI(), c)
	})

	t.Run("assert articles were scaled out", func(t *testing.T) {
		// shard.ObjectCount is eventually consistent, see Bucket::CountAsync()
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			n := common.GetNodes(t, compose.GetWeaviate().URI())
			var shardsFound int
			for _, node := range n.Nodes {
				for _, shard := range node.Shards {
					if shard.Class == articleClass.Class {
						assert.EqualValues(collect, int64(10), shard.ObjectCount)
						shardsFound++
					}
				}
			}
			assert.Equal(collect, 2, shardsFound)
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("kill a node and check contents of remaining node", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
		p := common.GQLGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, types.ConsistencyLevelOne)
		assert.Len(t, p, 10)
		a := common.GQLGet(t, compose.GetWeaviate().URI(), articleClass.Class, types.ConsistencyLevelOne)
		assert.Len(t, a, 10)
	})
}
