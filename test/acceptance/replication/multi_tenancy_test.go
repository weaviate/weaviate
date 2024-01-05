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

const (
	tenantID = strfmt.UUID("45e9e17e-8102-4011-95f0-3079ca188bbf")
)

func multiTenancyEnabled(t *testing.T) {
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
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 2,
		}
		articleClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("add tenants", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantID.String()}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
		helper.CreateTenants(t, articleClass.Class, tenants)
	})

	t.Run("insert paragraphs batch", func(t *testing.T) {
		t.Run("create objects on node 1", func(t *testing.T) {
			batch := make([]*models.Object, len(paragraphIDs))
			for i, id := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d", i)).
					WithTenant(tenantID.String()).
					Object()
			}
			createTenantObjects(t, compose.GetWeaviate().URI(), batch)
		})

		t.Run("stop node 1", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviate().Name())
		})

		t.Run("assert objects exist on node 2", func(t *testing.T) {
			count := countTenantObjects(t, compose.GetWeaviateNode2().URI(),
				"Paragraph", tenantID.String())
			assert.Equal(t, int64(len(paragraphIDs)), count)
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
					WithTenant(tenantID.String()).
					Object()
				createTenantObject(t, compose.GetWeaviateNode2().URI(), obj)
			}
		})

		t.Run("stop node 2", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		})

		t.Run("assert objects exist on node 1", func(t *testing.T) {
			count := countTenantObjects(t, compose.GetWeaviate().URI(),
				"Article", tenantID.String())
			assert.Equal(t, int64(len(articleIDs)), count)
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
				From:   strfmt.URI(crossref.NewSource("Article", "hasParagraphs", articleIDs[i]).String()),
				To:     strfmt.URI(crossref.NewLocalhost("Paragraph", paragraphIDs[i]).String()),
				Tenant: tenantID.String(),
			}
		}

		t.Run("add references to node 1", func(t *testing.T) {
			addTenantReferences(t, compose.GetWeaviate().URI(), refs)
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
			resp := gqlTenantGet(t, compose.GetWeaviateNode2().URI(), "Article", replica.One,
				tenantID.String(), "_additional{id}", "hasParagraphs {... on Paragraph {_additional{id}}}")
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
		before, err := getTenantObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], tenantID.String())
		require.Nil(t, err)
		newTitle := "Article#9000"

		t.Run("execute object patch on node 2", func(t *testing.T) {
			patch := &models.Object{
				ID:         before.ID,
				Class:      "Article",
				Properties: map[string]interface{}{"title": newTitle},
				Tenant:     tenantID.String(),
			}
			patchTenantObject(t, compose.GetWeaviateNode2().URI(), patch)
		})

		t.Run("stop node 2", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		})

		t.Run("assert object is patched on node 1", func(t *testing.T) {
			after, err := getTenantObjectFromNode(t, compose.GetWeaviate().URI(),
				"Article", articleIDs[0], "node1", tenantID.String())
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
			deleteTenantObject(t, compose.GetWeaviate().URI(), "Article", articleIDs[0], tenantID.String())
		})

		t.Run("stop node 1", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviate().Name())
		})

		t.Run("assert object removed from node 2", func(t *testing.T) {
			_, err := getTenantObjectFromNode(t, compose.GetWeaviateNode2().URI(),
				"Article", articleIDs[0], "node2", tenantID.String())
			assert.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
		})

		t.Run("restart node 1", func(t *testing.T) {
			restartNode1(ctx, t, compose)
		})
	})

	t.Run("batch delete all objects", func(t *testing.T) {
		t.Run("execute batch delete on node 2", func(t *testing.T) {
			deleteTenantObjects(t, compose.GetWeaviateNode2().URI(),
				"Article", []string{"title"}, "Article#*", tenantID.String())
		})

		t.Run("stop node 2", func(t *testing.T) {
			stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		})

		t.Run("assert objects are removed from node 1", func(t *testing.T) {
			count := countTenantObjects(t, compose.GetWeaviate().URI(), "Article", tenantID.String())
			assert.Zero(t, count)
		})

		t.Run("restart node 2", func(t *testing.T) {
			err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
			require.Nil(t, err)
		})
	})
}
