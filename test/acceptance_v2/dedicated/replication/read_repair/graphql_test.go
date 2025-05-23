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
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestGraphqlSearch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	t.Cleanup(cancel)

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With3NodeCluster().WithText2VecContextionary()
	})

	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.Vectorizer = "text2vec-contextionary"
	articleClass := articles.ArticlesClass()

	t.Run("CreateSchema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		helper.CreateClassWithClient(t, client, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		helper.CreateClassWithClient(t, client, articleClass)
	})

	t.Run("InsertParagraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.ContainerURI(1), batch)
	})

	t.Run("InsertArticles", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.ContainerURI(2), batch)
	})

	t.Run("StopNode-2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	t.Run("get consistent search results with ONE (1/2 nodes up)", func(t *testing.T) {
		resp := common.GQLGet(t, compose.ContainerURI(1), paragraphClass.Class, types.ConsistencyLevelOne)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("RestartNode-2", func(t *testing.T) {
		err := compose.StartAt(ctx, 2)
		require.Nil(t, err)
	})

	t.Run("get consistent search results with ALL (2/2 nodes up)", func(t *testing.T) {
		resp := common.GQLGet(t, compose.ContainerURI(1), paragraphClass.Class, types.ConsistencyLevelAll)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("get consistent search results with QUORUM (2/2 nodes up)", func(t *testing.T) {
		resp := common.GQLGet(t, compose.ContainerURI(1), paragraphClass.Class, types.ConsistencyLevelQuorum)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("get consistent search results with ONE (2/2 nodes up)", func(t *testing.T) {
		resp := common.GQLGet(t, compose.ContainerURI(1), paragraphClass.Class, types.ConsistencyLevelOne)
		checkResultsConsistency(t, resp, true)
	})

	t.Run("get consistent search results with ONE (2/2 nodes up)", func(t *testing.T) {
		resp := common.GQLGet(t, compose.ContainerURI(1), paragraphClass.Class, types.ConsistencyLevelOne)
		require.GreaterOrEqual(t, len(resp), 1)
		vec := resp[0].(map[string]interface{})["_additional"].(map[string]interface{})["vector"].([]interface{})
		resp = common.GQLGetNearVec(t, compose.ContainerURI(1), paragraphClass.Class, vec, types.ConsistencyLevelQuorum)
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
