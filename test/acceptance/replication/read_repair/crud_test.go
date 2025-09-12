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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

var (
	paragraphIDs = []strfmt.UUID{
		strfmt.UUID("3bf331ac-8c86-4f95-b127-2f8f96bbc093"),
		strfmt.UUID("47b26ba1-6bc9-41f8-a655-8b9a5b60e1a3"),
		strfmt.UUID("5fef6289-28d2-4ea2-82a9-48eb501200cd"),
		strfmt.UUID("34a673b4-8859-4cb4-bb30-27f5622b47e9"),
		strfmt.UUID("9fa362f5-c2dc-4fb8-b5b2-11701adc5f75"),
		strfmt.UUID("63735238-6723-4caf-9eaa-113120968ff4"),
		strfmt.UUID("2236744d-b2d2-40e5-95d8-2574f20a7126"),
		strfmt.UUID("1a54e25d-aaf9-48d2-bc3c-bef00b556297"),
		strfmt.UUID("0b8a0e70-a240-44b2-ac6d-26dda97523b9"),
		strfmt.UUID("50566856-5d0a-4fb1-a390-e099bc236f66"),
	}

	articleIDs = []strfmt.UUID{
		strfmt.UUID("aeaf8743-5a8f-4149-b960-444181d3131a"),
		strfmt.UUID("2a1e9834-064e-4ca8-9efc-35707c6bae6d"),
		strfmt.UUID("8d101c0c-4deb-48d0-805c-d9c691042a1a"),
		strfmt.UUID("b9715fec-ef6c-4e8d-a89e-55e2eebee3f6"),
		strfmt.UUID("faf520f2-f6c3-4cdf-9c16-0348ffd0f8ac"),
		strfmt.UUID("d4c695dd-4dc7-4e49-bc73-089ef5f90fc8"),
		strfmt.UUID("c7949324-e07f-4ffc-8be0-194f0470d375"),
		strfmt.UUID("9c112e01-7759-43ed-a6e8-5defb267c8ee"),
		strfmt.UUID("9bf847f3-3a1a-45a5-b656-311163e536b5"),
		strfmt.UUID("c1975388-d67c-404a-ae77-5983fbaea4bb"),
	}
)

type ReplicationTestSuite struct {
	suite.Suite
}

func (suite *ReplicationTestSuite) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationTestSuite))
}

func (suite *ReplicationTestSuite) TestImmediateReplicaCRUD() {
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

	helper.SetupClient(compose.ContainerURI(1))
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("CreateSchema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("InsertParagraphsBatch", func(t *testing.T) {
		t.Run("CreateObjectsOnNode-3", func(t *testing.T) {
			batch := make([]*models.Object, len(paragraphIDs))
			for i, id := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d", i)).
					Object()
			}
			common.CreateObjects(t, compose.ContainerURI(3), batch)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, 3)
		})

		t.Run("ObjectsExistOnNode-1", func(t *testing.T) {
			resp := common.GQLGet(t, compose.ContainerURI(1), "Paragraph", types.ConsistencyLevelOne)
			assert.Len(t, resp, len(paragraphIDs))
		})

		t.Run("ObjectsExistOnNode-2", func(t *testing.T) {
			resp := common.GQLGet(t, compose.ContainerURI(2), "Paragraph", types.ConsistencyLevelOne)
			require.Len(t, resp, len(paragraphIDs))
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			err = compose.StartAt(ctx, 3)
			require.Nil(t, err)
		})
	})

	t.Run("InsertArticlesIndividually", func(t *testing.T) {
		t.Run("CreateObjectsOnNode 3", func(t *testing.T) {
			for i, id := range articleIDs {
				obj := articles.NewArticle().
					WithID(id).
					WithTitle(fmt.Sprintf("Article#%d", i)).
					Object()
				common.CreateObjectCL(t, compose.ContainerURI(3), obj, types.ConsistencyLevelOne)
			}
		})

		t.Run("StopNode-3", func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, 3)
		})

		t.Run("ObjectsExistOnNode-1", func(t *testing.T) {
			resp := common.GQLGet(t, compose.ContainerURI(1), "Article", types.ConsistencyLevelOne)
			require.Len(t, resp, len(articleIDs))
		})

		t.Run("ObjectsExistOnNode-2", func(t *testing.T) {
			resp := common.GQLGet(t, compose.ContainerURI(2), "Article", types.ConsistencyLevelOne)
			require.Len(t, resp, len(articleIDs))
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			err = compose.StartAt(ctx, 3)
			require.Nil(t, err)
		})
	})

	t.Run("AddReferences", func(t *testing.T) {
		refs := make([]*models.BatchReference, len(articleIDs))
		for i := range articleIDs {
			refs[i] = &models.BatchReference{
				From: strfmt.URI(crossref.NewSource("Article", "hasParagraphs", articleIDs[i]).String()),
				To:   strfmt.URI(crossref.NewLocalhost("Paragraph", paragraphIDs[i]).String()),
			}
		}

		t.Run("OnNode-3", func(t *testing.T) {
			common.AddReferences(t, compose.ContainerURI(3), refs)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, 3)
		})

		t.Run("ExistOnNode-2", func(t *testing.T) {
			type additional struct {
				ID strfmt.UUID `json:"id"`
			}

			type article struct {
				Additional    additional `json:"_additional"`
				HasParagraphs []struct {
					Additional additional `json:"_additional"`
				} `json:"hasParagraphs"`
			}

			// maps article id to referenced paragraph id
			refPairs := make(map[strfmt.UUID]strfmt.UUID)
			resp := common.GQLGet(t, compose.ContainerURI(2), "Article", types.ConsistencyLevelOne,
				"_additional{id}", "hasParagraphs {... on Paragraph {_additional{id}}}")
			require.Len(t, resp, len(articleIDs))

			for _, r := range resp {
				b, err := json.Marshal(r)
				require.Nil(t, err)
				var art article
				err = json.Unmarshal(b, &art)
				require.Nil(t, err)
				require.Len(t, art.HasParagraphs, 1)
				refPairs[art.Additional.ID] = art.HasParagraphs[0].Additional.ID
			}

			for i := range articleIDs {
				paragraphID, ok := refPairs[articleIDs[i]]
				require.True(t, ok, "expected %q to be in refPairs: %+v", articleIDs[i], refPairs)
				require.Equal(t, paragraphIDs[i], paragraphID)
			}
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			err = compose.StartAt(ctx, 3)
			require.Nil(t, err)
		})
	})

	t.Run("UpdateObject", func(t *testing.T) {
		before, err := common.GetObject(t, compose.ContainerURI(3), "Article", articleIDs[0], false)
		require.Nil(t, err)
		newTitle := "Article#9000"

		t.Run("OnNode-3", func(t *testing.T) {
			patch := &models.Object{
				ID:         before.ID,
				Class:      "Article",
				Properties: map[string]interface{}{"title": newTitle},
			}
			common.UpdateObjectCL(t, compose.ContainerURI(3), patch, types.ConsistencyLevelQuorum)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, 3)
		})

		t.Run("PatchedOnNode-1", func(t *testing.T) {
			after, err := common.GetObjectFromNode(t, compose.ContainerURI(1), "Article", articleIDs[0], "node1")
			require.Nil(t, err)

			newVal, ok := after.Properties.(map[string]interface{})["title"]
			require.True(t, ok)
			require.Equal(t, newTitle, newVal)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			require.Nil(t, compose.StartAt(ctx, 3))
		})
	})

	t.Run("DeleteObject", func(t *testing.T) {
		t.Run("OnNode-3", func(t *testing.T) {
			common.DeleteObject(t, compose.ContainerURI(3), "Article", articleIDs[0], types.ConsistencyLevelAll)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, 3)
		})

		t.Run("OnNode-1", func(t *testing.T) {
			_, err := common.GetObjectFromNode(t, compose.ContainerURI(1), "Article", articleIDs[0], "node1")
			require.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
		})
		t.Run("OnNode-2", func(t *testing.T) {
			_, err := common.GetObjectFromNode(t, compose.ContainerURI(2), "Article", articleIDs[0], "node2")
			require.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			require.Nil(t, compose.StartAt(ctx, 3))
		})
	})

	t.Run("BatchAllObjects", func(t *testing.T) {
		t.Run("OnNode-3", func(t *testing.T) {
			common.DeleteObjects(t, compose.ContainerURI(3),
				"Article", []string{"title"}, "Article#*", types.ConsistencyLevelAll)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			common.StopNodeAt(ctx, t, compose, 3)
		})

		t.Run("OnNode-1", func(t *testing.T) {
			resp := common.GQLGet(t, compose.ContainerURI(1), "Article", types.ConsistencyLevelOne)
			require.Empty(t, resp)
		})

		t.Run("OnNode-2", func(t *testing.T) {
			resp := common.GQLGet(t, compose.ContainerURI(2), "Article", types.ConsistencyLevelOne)
			require.Empty(t, resp)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			require.Nil(t, compose.StartAt(ctx, 3))
		})
	})
}

func (suite *ReplicationTestSuite) TestEventualReplicaCRUD() {
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

	ctx, cancel := context.WithTimeout(mainCtx, 5*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("create schema on node 1", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		helper.CreateClass(t, paragraphClass)
		articleClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		helper.CreateClass(t, articleClass)
	})

	t.Run("insert paragraphs batch on node 1", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("insert articles batch on node 1", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("configure classes to replicate to node 2 and 3", func(t *testing.T) {
		ac := helper.GetClass(t, "Article")
		ac.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		helper.UpdateClass(t, ac)

		pc := helper.GetClass(t, "Paragraph")
		pc.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		helper.UpdateClass(t, pc)
	})

	t.Run("StopNode-3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	t.Run("assert all previous data replicated to node 2", func(t *testing.T) {
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			resp := common.GQLGet(t, compose.GetWeaviateNode2().URI(), "Article", types.ConsistencyLevelOne)
			require.Len(collect, resp, len(articleIDs))
			resp = common.GQLGet(t, compose.GetWeaviateNode2().URI(), "Paragraph", types.ConsistencyLevelOne)
			require.Len(collect, resp, len(paragraphIDs))
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("RestartNode-3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("assert all previous data replicated to node 3", func(t *testing.T) {
		assert.EventuallyWithT(t, func(collect *assert.CollectT) {
			resp := common.GQLGet(t, compose.GetWeaviateNode3().URI(), "Article", types.ConsistencyLevelAll)
			assert.Len(collect, resp, len(articleIDs))
			resp = common.GQLGet(t, compose.GetWeaviateNode3().URI(), "Paragraph", types.ConsistencyLevelAll)
			assert.Len(collect, resp, len(paragraphIDs))
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("assert any future writes are replicated", func(t *testing.T) {
		t.Run("PatchObject", func(t *testing.T) {
			before, err := common.GetObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], false)
			require.Nil(t, err)
			newTitle := "Article#9000"

			t.Run("OnNode-2", func(t *testing.T) {
				patch := &models.Object{
					ID:         before.ID,
					Class:      "Article",
					Properties: map[string]interface{}{"title": newTitle},
				}
				common.PatchObject(t, compose.GetWeaviateNode(2).URI(), patch)
			})

			t.Run("PatchedOnNode-1", func(t *testing.T) {
				after, err := common.GetObjectFromNode(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], "node1")
				require.Nil(t, err)

				require.Contains(t, after.Properties.(map[string]interface{}), "title")
				require.Equal(t, newTitle, after.Properties.(map[string]interface{})["title"])
			})

			t.Run("PatchedOnNode-2", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					after, err := common.GetObjectFromNode(t, compose.GetWeaviateNode2().URI(), "Article", articleIDs[0], "node2")
					require.Nil(collect, err)

					require.Contains(collect, after.Properties.(map[string]interface{}), "title")
					assert.Equal(collect, newTitle, after.Properties.(map[string]interface{})["title"])
				}, 5*time.Second, 100*time.Millisecond)
			})

			t.Run("PatchedOnNode-3", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					after, err := common.GetObjectFromNode(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], "node3")
					require.Nil(collect, err)

					require.Contains(collect, after.Properties.(map[string]interface{}), "title")
					assert.Equal(collect, newTitle, after.Properties.(map[string]interface{})["title"])
				}, 5*time.Second, 100*time.Millisecond)
			})
		})

		t.Run("DeleteObject", func(t *testing.T) {
			t.Run("OnNode-2", func(t *testing.T) {
				common.DeleteObject(t, compose.GetWeaviateNode2().URI(), "Article", articleIDs[0], types.ConsistencyLevelAll)
			})

			t.Run("OnNode-1", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					_, err := common.GetObjectFromNode(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], "node1")
					require.Equal(collect, &objects.ObjectsClassGetNotFound{}, err)
				}, 5*time.Second, 100*time.Millisecond)
			})
		})

		t.Run("BatchDeleteAllObjects", func(t *testing.T) {
			t.Run("OnNode-2", func(t *testing.T) {
				common.DeleteObjects(t, compose.GetWeaviateNode2().URI(),
					"Article", []string{"title"}, "Article#*", types.ConsistencyLevelAll)
			})

			t.Run("OnNode-1", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					resp := common.GQLGet(t, compose.GetWeaviate().URI(), "Article", types.ConsistencyLevelOne)
					assert.Empty(collect, resp)
				}, 5*time.Second, 100*time.Millisecond)
			})
		})

		t.Run("configure classes to decrease replication factor should fail", func(t *testing.T) {
			ac := helper.GetClass(t, "Article")
			ac.ReplicationConfig = &models.ReplicationConfig{
				Factor: 2,
			}

			params := schema.NewSchemaObjectsUpdateParams().
				WithObjectClass(ac).WithClassName(ac.Class)
			resp, err := helper.Client(t).Schema.SchemaObjectsUpdate(params, nil)
			require.NotNil(t, err)
			helper.AssertRequestFail(t, resp, err, func() {
				var errResponse *schema.SchemaObjectsUpdateUnprocessableEntity
				require.True(t, errors.As(err, &errResponse))
				require.Equal(t, fmt.Sprintf("scale \"%s\" from 3 replicas to 2: scaling in not supported yet", ac.Class), errResponse.Payload.Error[0].Message)
			})
		})
	})
}
