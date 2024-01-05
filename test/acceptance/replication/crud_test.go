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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
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

func immediateReplicaCRUD(t *testing.T) {
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

	t.Run("insert paragraphs batch", func(t *testing.T) {
		t.Run("create objects on node 1", func(t *testing.T) {
			batch := make([]*models.Object, len(paragraphIDs))
			for i, id := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d", i)).
					Object()
			}
			createObjects(t, compose.GetWeaviate().URI(), batch)
		})

		t.Run("stop node 1", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviate().Name())
		})

		t.Run("assert objects exist on node 2", func(t *testing.T) {
			resp := gqlGet(t, compose.GetWeaviateNode2().URI(), "Paragraph", replica.One)
			assert.Len(t, resp, len(paragraphIDs))
		})

		t.Run("restart node 1", func(t *testing.T) {
			restartNode1(ctx, t, compose)
		})
	})

	t.Run("insert articles individually", func(t *testing.T) {
		t.Run("create objects on node 2", func(t *testing.T) {
			for i, id := range articleIDs {
				obj := articles.NewArticle().
					WithID(id).
					WithTitle(fmt.Sprintf("Article#%d", i)).
					Object()
				createObject(t, compose.GetWeaviateNode2().URI(), obj)
			}
		})

		t.Run("stop node 2", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		})

		t.Run("assert objects exist on node 1", func(t *testing.T) {
			resp := gqlGet(t, compose.GetWeaviate().URI(), "Article", replica.One)
			assert.Len(t, resp, len(articleIDs))
		})

		t.Run("restart node 2", func(t *testing.T) {
			err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
			require.Nil(t, err)
		})
	})

	t.Run("add references", func(t *testing.T) {
		refs := make([]*models.BatchReference, len(articleIDs))
		for i := range articleIDs {
			refs[i] = &models.BatchReference{
				From: strfmt.URI(crossref.NewSource("Article", "hasParagraphs", articleIDs[i]).String()),
				To:   strfmt.URI(crossref.NewLocalhost("Paragraph", paragraphIDs[i]).String()),
			}
		}

		t.Run("add references to node 1", func(t *testing.T) {
			addReferences(t, compose.GetWeaviate().URI(), refs)
		})

		t.Run("stop node 1", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviate().Name())
		})

		t.Run("assert references were added successfully to node 2", func(t *testing.T) {
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
			resp := gqlGet(t, compose.GetWeaviateNode2().URI(), "Article", replica.One,
				"_additional{id}", "hasParagraphs {... on Paragraph {_additional{id}}}")
			assert.Len(t, resp, len(articleIDs))

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
				assert.Equal(t, paragraphIDs[i], paragraphID)
			}
		})

		t.Run("restart node 1", func(t *testing.T) {
			restartNode1(ctx, t, compose)
		})
	})

	t.Run("patch an object", func(t *testing.T) {
		before, err := getObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], false)
		require.Nil(t, err)
		newTitle := "Article#9000"

		t.Run("execute object patch on node 2", func(t *testing.T) {
			patch := &models.Object{
				ID:         before.ID,
				Class:      "Article",
				Properties: map[string]interface{}{"title": newTitle},
			}
			patchObject(t, compose.GetWeaviateNode2().URI(), patch)
		})

		t.Run("stop node 2", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		})

		t.Run("assert object is patched on node 1", func(t *testing.T) {
			after, err := getObjectFromNode(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], "node1")
			require.Nil(t, err)

			newVal, ok := after.Properties.(map[string]interface{})["title"]
			require.True(t, ok)
			assert.Equal(t, newTitle, newVal)
		})

		t.Run("restart node 2", func(t *testing.T) {
			err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
			require.Nil(t, err)
		})
	})

	t.Run("delete an object", func(t *testing.T) {
		t.Run("execute delete object on node 1", func(t *testing.T) {
			deleteObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0])
		})

		t.Run("stop node 1", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviate().Name())
		})

		t.Run("assert object removed from node 2", func(t *testing.T) {
			_, err := getObjectFromNode(t, compose.GetWeaviateNode2().URI(), "Article", articleIDs[0], "node2")
			assert.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
		})

		t.Run("restart node 1", func(t *testing.T) {
			restartNode1(ctx, t, compose)
		})
	})

	t.Run("batch delete all objects", func(t *testing.T) {
		t.Run("execute batch delete on node 2", func(t *testing.T) {
			deleteObjects(t, compose.GetWeaviateNode2().URI(),
				"Article", []string{"title"}, "Article#*")
		})

		t.Run("stop node 2", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		})

		t.Run("assert objects are removed from node 1", func(t *testing.T) {
			resp := gqlGet(t, compose.GetWeaviate().URI(), "Article", replica.One)
			assert.Empty(t, resp)
		})

		t.Run("restart node 2", func(t *testing.T) {
			err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
			require.Nil(t, err)
		})
	})
}

func eventualReplicaCRUD(t *testing.T) {
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
		createObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("insert articles batch on node 1", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		createObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("configure classes to replicate to node 2", func(t *testing.T) {
		ac := helper.GetClass(t, "Article")
		ac.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		helper.UpdateClass(t, ac)

		pc := helper.GetClass(t, "Paragraph")
		pc.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		helper.UpdateClass(t, pc)
	})

	t.Run("stop node 1", func(t *testing.T) {
		stopNode(ctx, t, compose, compose.GetWeaviate().Name())
	})

	t.Run("assert all previous data replicated to node 2", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviateNode2().URI(), "Article", replica.One)
		assert.Len(t, resp, len(articleIDs))
		resp = gqlGet(t, compose.GetWeaviateNode2().URI(), "Paragraph", replica.One)
		assert.Len(t, resp, len(paragraphIDs))
	})

	t.Run("restart node 1", func(t *testing.T) {
		restartNode1(ctx, t, compose)
	})

	t.Run("assert any future writes are replicated", func(t *testing.T) {
		t.Run("patch an object", func(t *testing.T) {
			before, err := getObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], false)
			require.Nil(t, err)
			newTitle := "Article#9000"

			t.Run("execute object patch on node 2", func(t *testing.T) {
				patch := &models.Object{
					ID:         before.ID,
					Class:      "Article",
					Properties: map[string]interface{}{"title": newTitle},
				}
				patchObject(t, compose.GetWeaviateNode2().URI(), patch)
			})

			t.Run("stop node 2", func(t *testing.T) {
				stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
			})

			t.Run("assert object is patched on node 1", func(t *testing.T) {
				after, err := getObjectFromNode(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], "node1")
				require.Nil(t, err)

				newVal, ok := after.Properties.(map[string]interface{})["title"]
				require.True(t, ok)
				assert.Equal(t, newTitle, newVal)
			})

			t.Run("restart node 2", func(t *testing.T) {
				err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
				require.Nil(t, err)
			})
		})

		t.Run("delete an object", func(t *testing.T) {
			t.Run("execute delete object on node 1", func(t *testing.T) {
				deleteObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0])
			})

			t.Run("stop node 1", func(t *testing.T) {
				stopNode(ctx, t, compose, compose.GetWeaviate().Name())
			})

			t.Run("assert object removed from node 2", func(t *testing.T) {
				_, err := getObjectFromNode(t, compose.GetWeaviateNode2().URI(), "Article", articleIDs[0], "node2")
				assert.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
			})

			t.Run("restart node 1", func(t *testing.T) {
				restartNode1(ctx, t, compose)
			})
		})

		t.Run("batch delete all objects", func(t *testing.T) {
			t.Run("execute batch delete on node 2", func(t *testing.T) {
				deleteObjects(t, compose.GetWeaviateNode2().URI(),
					"Article", []string{"title"}, "Article#*")
			})

			t.Run("stop node 2", func(t *testing.T) {
				stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
			})

			t.Run("assert objects are removed from node 1", func(t *testing.T) {
				resp := gqlGet(t, compose.GetWeaviate().URI(), "Article", replica.One)
				assert.Empty(t, resp)
			})

			t.Run("restart node 2", func(t *testing.T) {
				err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
				require.Nil(t, err)
			})
		})
	})
}

func restartNode1(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	// since node1 is the gossip "leader", node 2 must be stopped and restarted
	// after node1 to re-facilitate internode communication
	stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
	require.Nil(t, compose.Start(ctx, compose.GetWeaviate().Name()))
	require.Nil(t, compose.Start(ctx, compose.GetWeaviateNode2().Name()))
	<-time.After(1 * time.Second) // wait for initialization
}

func stopNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, container string) {
	require.Nil(t, compose.Stop(ctx, container, nil))
	<-time.After(1 * time.Second) // give time for shutdown
}
