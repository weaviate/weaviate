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

func graphqlSearch(t *testing.T) {
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
	paragraphClass.Vectorizer = "text2vec-contextionary"
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

	t.Run("stop node 2", func(t *testing.T) {
		stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		time.Sleep(10 * time.Second)
	})

	t.Run("get consistent search results with ONE (1/2 nodes up)", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, replica.One)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("restart node 2", func(t *testing.T) {
		err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
		require.Nil(t, err)
	})

	t.Run("get consistent search results with ALL (2/2 nodes up)", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, replica.All)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("get consistent search results with QUORUM (2/2 nodes up)", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, replica.Quorum)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("get consistent search results with ONE (2/2 nodes up)", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, replica.One)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("get consistent search results with ONE (2/2 nodes up)", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, replica.One)
		require.GreaterOrEqual(t, len(resp), 1)
		vec := resp[0].(map[string]interface{})["_additional"].(map[string]interface{})["vector"].([]interface{})
		resp = gqlGetNearVec(t, compose.GetWeaviate().URI(), paragraphClass.Class, vec, replica.Quorum)
		checkResultsConsistency(t, resp, true)
	})
}

func checkResultsConsistency(t *testing.T, results []interface{}, expectConsistent bool) {
	for _, res := range results {
		addl := res.(map[string]interface{})["_additional"].(map[string]interface{})
		if expectConsistent {
			assert.True(t, addl["isConsistent"].(bool))
		} else {
			assert.False(t, addl["isConsistent"].(bool))
		}
	}
}
