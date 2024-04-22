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
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		articleClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
		helper.CreateClass(t, articleClass)
	})

	time.Sleep(1 * time.Second) // remove once eventual consistency has been addressed

	t.Run("AddTenants", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantID.String()}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
		helper.CreateTenants(t, articleClass.Class, tenants)
	})

	t.Run("InsertParagraphsBatch", func(t *testing.T) {
		t.Run("OnNode-3", func(t *testing.T) {
			batch := make([]*models.Object, len(paragraphIDs))
			for i, id := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d", i)).
					WithTenant(tenantID.String()).
					Object()
			}
			createTenantObjects(t, compose.ContainerURI(3), batch)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			stopNodeAt(ctx, t, compose, 3)
		})

		t.Run("ObjectsExistOnNode-1", func(t *testing.T) {
			count := countTenantObjects(t, compose.ContainerURI(1),
				"Paragraph", tenantID.String())
			assert.Equal(t, int64(len(paragraphIDs)), count)
		})

		t.Run("ObjectsExistOnNode-2", func(t *testing.T) {
			count := countTenantObjects(t, compose.ContainerURI(2),
				"Paragraph", tenantID.String())
			assert.Equal(t, int64(len(paragraphIDs)), count)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 3)
			time.Sleep(time.Second)
		})
	})

	t.Run("InsertArticlesIndividually", func(t *testing.T) {
		t.Run("CreateObjectsOnNode-3", func(t *testing.T) {
			for i, id := range articleIDs {
				obj := articles.NewArticle().
					WithID(id).
					WithTitle(fmt.Sprintf("Article#%d", i)).
					WithTenant(tenantID.String()).
					Object()
				createObjectCL(t, compose.ContainerURI(3), obj, replica.Quorum)
			}
		})

		t.Run("StopNode-3", func(t *testing.T) {
			time.Sleep(time.Second)
			stopNodeAt(ctx, t, compose, 3)
		})

		t.Run("ObjectsExistOnNode-1", func(t *testing.T) {
			count := countTenantObjects(t, compose.ContainerURI(1),
				"Article", tenantID.String())
			assert.Equal(t, int64(len(articleIDs)), count)
		})
		t.Run("ObjectsExistOnNode-2", func(t *testing.T) {
			count := countTenantObjects(t, compose.ContainerURI(2),
				"Article", tenantID.String())
			assert.Equal(t, int64(len(articleIDs)), count)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 3)
			time.Sleep(time.Second)
		})
	})

	t.Run("AddReferences", func(t *testing.T) {
		refs := make([]*models.BatchReference, len(articleIDs))
		for i := range articleIDs {
			refs[i] = &models.BatchReference{
				From:   strfmt.URI(crossref.NewSource("Article", "hasParagraphs", articleIDs[i]).String()),
				To:     strfmt.URI(crossref.NewLocalhost("Paragraph", paragraphIDs[i]).String()),
				Tenant: tenantID.String(),
			}
		}

		t.Run("AddReferencesToNode-3", func(t *testing.T) {
			addTenantReferences(t, compose.ContainerURI(3), refs)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			stopNodeAt(ctx, t, compose, 3)
		})

		t.Run("ReferencesExistsONNode-2", func(t *testing.T) {
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
			resp := gqlTenantGet(t, compose.ContainerURI(2), "Article", replica.One,
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

		t.Run("RestartNode-3", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 3)
			time.Sleep(time.Second)
		})
	})

	t.Run("UpdateObject", func(t *testing.T) {
		before, err := getTenantObject(t, compose.ContainerURI(1), "Article", articleIDs[0], tenantID.String())
		require.Nil(t, err)
		newTitle := "Article#9000"

		t.Run("OnNode-3", func(t *testing.T) {
			patch := &models.Object{
				ID:         before.ID,
				Class:      "Article",
				Properties: map[string]interface{}{"title": newTitle},
				Tenant:     tenantID.String(),
			}
			updateObjectCL(t, compose.ContainerURI(3), patch, replica.Quorum)
		})

		t.Run("StopNode-3", func(t *testing.T) {
			stopNodeAt(ctx, t, compose, 3)
		})

		t.Run("PatchedOnNode-1", func(t *testing.T) {
			after, err := getTenantObjectFromNode(t, compose.ContainerURI(1),
				"Article", articleIDs[0], "node1", tenantID.String())
			require.Nil(t, err)

			newVal, ok := after.Properties.(map[string]interface{})["title"]
			require.True(t, ok)
			assert.Equal(t, newTitle, newVal)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 3)
			time.Sleep(time.Second)
		})
	})

	t.Run("DeleteObject", func(t *testing.T) {
		t.Run("OnNode-1", func(t *testing.T) {
			deleteTenantObject(t, compose.ContainerURI(1), "Article", articleIDs[0], tenantID.String())
		})

		t.Run("StopNode-3", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 3)
		})

		t.Run("OnNode-2", func(t *testing.T) {
			_, err := getTenantObjectFromNode(t, compose.ContainerURI(2),
				"Article", articleIDs[0], "node2", tenantID.String())
			assert.Equal(t, &objects.ObjectsClassGetNotFound{}, err)
		})

		t.Run("RestartNode-3", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 3)
		})
	})

	t.Run("BatchAllObjects", func(t *testing.T) {
		t.Run("OnNode-2", func(t *testing.T) {
			deleteTenantObjects(t, compose.ContainerURI(2),
				"Article", []string{"title"}, "Article#*", tenantID.String())
		})

		t.Run("StopNode-2", func(t *testing.T) {
			stopNodeAt(ctx, t, compose, 2)
		})

		t.Run("OnNode-1", func(t *testing.T) {
			count := countTenantObjects(t, compose.ContainerURI(1), "Article", tenantID.String())
			assert.Zero(t, count)
		})

		t.Run("RestartNode-2", func(t *testing.T) {
			startNodeAt(ctx, t, compose, 2)
		})
	})
}
