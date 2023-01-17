//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replication

import (
	"fmt"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/articles"
	"testing"
)

type mockCompose struct {
	uri string
}

func (m mockCompose) GetWeaviate() mockCompose {
	return mockCompose{uri: "localhost:8080"}
}

func (m mockCompose) GetWeaviateNode2() mockCompose {
	return mockCompose{uri: "localhost:8081"}
}

func (m mockCompose) URI() string {
	return m.uri
}

func multiShardScaleOut(t *testing.T) {
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	//defer cancel()
	//
	//compose, err := docker.New().
	//	WithWeaviateCluster().
	//	WithText2VecContextionary().
	//	Start(ctx)
	//require.Nil(t, err)
	//defer func() {
	//	if err := compose.Terminate(ctx); err != nil {
	//		t.Fatalf("failed to terminte test containers: %s", err.Error())
	//	}
	//}()

	compose := mockCompose{}

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

	t.Log()
}
