//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package module_base_urls

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	adminUser  = "admin"
	adminKey   = "admin-key"
	editorUser = "editor"
	editorKey  = "editor-key"

	listedBaseURL   = "https://gateway.example"
	attackerBaseURL = "https://attacker.example"
)

// A user who may only update one collection must not be able to point its generative module at a host of their
// choosing, which would then receive the server's OpenAI key and every caller's prompts. No request leaves Weaviate:
// the settings are rejected when the collection is written.
func TestCollectionBaseURLNeedsOperatorAllowList(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithRBAC().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(editorUser, editorKey).
		WithRbacRoots(adminUser).
		WithGenerativeOpenAI("sk-server-key", "", "").
		WithWeaviateEnv("MODULES_ALLOWED_BASE_URLS", listedBaseURL).
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Logf("failed to terminate test containers: %v", err)
		}
	})
	helper.SetupClient(compose.GetWeaviate().URI())
	t.Cleanup(helper.ResetClient)

	article := func(generative map[string]any) *models.Class {
		return &models.Class{
			Class:        "Article",
			Vectorizer:   "none",
			ModuleConfig: map[string]any{"generative-openai": generative},
			Properties:   []*models.Property{{Name: "body", DataType: []string{"text"}}},
		}
	}
	requireRejected := func(t *testing.T, err error) {
		t.Helper()
		var createErr *schema.SchemaObjectsCreateUnprocessableEntity
		var updateErr *schema.SchemaObjectsUpdateUnprocessableEntity
		switch {
		case errors.As(err, &createErr):
			require.Contains(t, createErr.Payload.Error[0].Message, "is not allowed")
		case errors.As(err, &updateErr):
			require.Contains(t, updateErr.Payload.Error[0].Message, "is not allowed")
		default:
			require.Failf(t, "expected 422", "%T: %v", err, err)
		}
	}

	helper.CreateClassAuth(t, article(map[string]any{}), adminKey)

	updateCollections := authorization.UpdateCollections
	readCollections := authorization.ReadCollections
	collection := "Article"
	helper.CreateRoleAndAssign(t, adminKey, editorUser, "article-schema",
		&models.Permission{Action: &updateCollections, Collections: &models.PermissionCollections{Collection: &collection}},
		&models.Permission{Action: &readCollections, Collections: &models.PermissionCollections{Collection: &collection}},
	)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := helper.GetClassAuthWithReturn(t, "Article", editorKey)
		assert.NoError(c, err)
	}, 15*time.Second, 100*time.Millisecond)

	t.Run("update to a foreign host is rejected", func(t *testing.T) {
		class := helper.GetClassAuth(t, "Article", editorKey)
		class.ModuleConfig = map[string]any{"generative-openai": map[string]any{"baseURL": attackerBaseURL}}
		_, err := helper.UpdateClassAuthWithReturn(t, "Article", class, editorKey)
		requireRejected(t, err)

		stored := helper.GetClassAuth(t, "Article", adminKey).ModuleConfig.(map[string]any)["generative-openai"]
		require.NotContains(t, stored, "baseURL")
	})

	t.Run("update hiding a foreign host under the empty module name is rejected", func(t *testing.T) {
		class := helper.GetClassAuth(t, "Article", editorKey)
		class.ModuleConfig = map[string]any{
			"generative-openai": map[string]any{},
			"":                  map[string]any{"baseURL": attackerBaseURL},
		}
		_, err := helper.UpdateClassAuthWithReturn(t, "Article", class, editorKey)
		var updateErr *schema.SchemaObjectsUpdateUnprocessableEntity
		require.ErrorAs(t, err, &updateErr)
		require.Contains(t, updateErr.Payload.Error[0].Message, "empty module name")
	})

	t.Run("create with a foreign host is rejected, even for an admin", func(t *testing.T) {
		class := article(map[string]any{"baseURL": attackerBaseURL})
		class.Class = "Attacker"
		_, err := helper.CreateClassAuthWithReturn(t, class, adminKey)
		requireRejected(t, err)
	})

	t.Run("update to a listed host is accepted", func(t *testing.T) {
		class := helper.GetClassAuth(t, "Article", editorKey)
		class.ModuleConfig = map[string]any{"generative-openai": map[string]any{"baseURL": listedBaseURL + "/v1"}}
		_, err := helper.UpdateClassAuthWithReturn(t, "Article", class, editorKey)
		require.NoError(t, err)
	})

	// The host is listed, so this does not show that an unlisted stored host is kept: the container's environment
	// cannot change between writes. TestUpdateClass_BaseURLs and TestUpdateClassInternal_KeepsAnyStoredBaseURL in
	// usecases/schema cover that.
	t.Run("unrelated update keeps a stored listed host", func(t *testing.T) {
		class := helper.GetClassAuth(t, "Article", editorKey)
		class.Description = "articles"
		_, err := helper.UpdateClassAuthWithReturn(t, "Article", class, editorKey)
		require.NoError(t, err)
		stored := helper.GetClassAuth(t, "Article", adminKey).ModuleConfig.(map[string]any)["generative-openai"]
		require.Equal(t, listedBaseURL+"/v1", stored.(map[string]any)["baseURL"])
	})
}
