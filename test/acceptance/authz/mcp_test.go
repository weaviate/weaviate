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

package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func callToolOnceWithAuth[I any, O any](ctx context.Context, t *testing.T, mcpURL, tool, key string, in I, out *O) error {
	c, err := client.NewStreamableHttpClient(
		mcpURL,
		transport.WithHTTPHeaders(map[string]string{"Authorization": fmt.Sprintf("Bearer %s", key)}),
	)
	if err != nil {
		return err
	}

	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	if err != nil {
		return err
	}

	res, err := c.CallTool(ctx, mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      tool,
			Arguments: in,
		},
	})
	if err != nil {
		return err
	}
	require.NotNil(t, res)
	if res.IsError {
		return fmt.Errorf("error calling tool %s: %s", tool, res.Content[0])
	}
	require.NotNil(t, res.StructuredContent)

	bytes, err := json.Marshal(res.StructuredContent)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, out)
	if err != nil {
		return err
	}
	return nil
}

func TestMCPServerAuthZ(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	customUser := "custom-user"
	customKey := "custom-key"

	compose, down := composeUpWithMCP(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil, true)
	defer down()

	mcpURL := fmt.Sprintf("http://%s", compose.GetWeaviate().McpURI())

	cls := articles.ParagraphsClass()
	helper.DeleteClassWithAuthz(t, cls.Class, helper.CreateAuth(adminKey))
	helper.CreateClassAuth(t, cls, adminKey)
	defer helper.DeleteClassWithAuthz(t, cls.Class, helper.CreateAuth(adminKey))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	roleName := "test-role"
	helper.DeleteRole(t, adminKey, roleName)
	helper.CreateRole(t, adminKey, &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{
			{
				Action: &authorization.ManageMcp,
				Mcp:    make(map[string]any),
			},
			{
				Action: &authorization.ReadCollections,
				Collections: &models.PermissionCollections{
					Collection: &cls.Class,
				},
			},
		},
	})
	defer helper.DeleteRole(t, adminKey, roleName)

	t.Run("fail to call tool without MCP permission", func(t *testing.T) {
		var resp *read.GetCollectionConfigResp
		err := callToolOnceWithAuth[any](ctx, t, mcpURL, "weaviate-collections-get-config", customKey, nil, &resp)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "user 'custom-user' has insufficient permissions to read_mcp []")
	})

	helper.AssignRoleToUser(t, adminKey, roleName, customUser)

	t.Run("succeed to call tool with MCP permission", func(t *testing.T) {
		var resp *read.GetCollectionConfigResp
		err := callToolOnceWithAuth[any](ctx, t, mcpURL, "weaviate-collections-get-config", customKey, nil, &resp)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Collections)
		require.Len(t, resp.Collections, 1)
	})
}

// TestMCPServerCollectionLevelAuthZ verifies that a user with MCP permission
// but only collection-scoped data permissions cannot access collections they
// are not authorized for via MCP tools (search, upsert, get-config).
func TestMCPServerCollectionLevelAuthZ(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	customUser := "custom-user"
	customKey := "custom-key"

	compose, down := composeUpWithMCP(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil, true)
	defer down()

	mcpURL := fmt.Sprintf("http://%s", compose.GetWeaviate().McpURI())
	adminAuth := helper.CreateAuth(adminKey)

	// Create two collections: the user will only have access to AllowedCollection
	allowedClass := "AllowedCollection"
	forbiddenClass := "ForbiddenCollection"

	classProps := []*models.Property{
		{
			Name:     "content",
			DataType: schema.DataTypeText.PropString(),
		},
	}
	for _, className := range []string{allowedClass, forbiddenClass} {
		helper.DeleteClassWithAuthz(t, className, adminAuth)
		helper.CreateClassAuth(t, &models.Class{
			Class:      className,
			Properties: classProps,
			Vectorizer: "none",
		}, adminKey)
	}
	defer helper.DeleteClassWithAuthz(t, allowedClass, adminAuth)
	defer helper.DeleteClassWithAuthz(t, forbiddenClass, adminAuth)

	// Insert an object into each collection as admin
	for _, className := range []string{allowedClass, forbiddenClass} {
		require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"content": "test content for " + className,
			},
		}, adminKey))
	}

	// Create a role with MCP permission + data permissions scoped to AllowedCollection only
	roleName := "collection-scoped-mcp-role"
	helper.DeleteRole(t, adminKey, roleName)
	helper.CreateRole(t, adminKey, &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{
			{
				Action: &authorization.ManageMcp,
				Mcp:    make(map[string]any),
			},
			{
				Action: &authorization.ReadCollections,
				Collections: &models.PermissionCollections{
					Collection: &allowedClass,
				},
			},
			{
				Action: &authorization.ReadData,
				Data: &models.PermissionData{
					Collection: &allowedClass,
				},
			},
			{
				Action: &authorization.CreateData,
				Data: &models.PermissionData{
					Collection: &allowedClass,
				},
			},
			{
				Action: &authorization.UpdateData,
				Data: &models.PermissionData{
					Collection: &allowedClass,
				},
			},
		},
	})
	defer helper.DeleteRole(t, adminKey, roleName)
	helper.AssignRoleToUser(t, adminKey, roleName, customUser)
	defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use alpha=0 (pure keyword/BM25) to avoid vectorizer requirement since classes use vectorizer: none
	pureKeyword := float64(0)

	t.Run("hybrid search on allowed collection succeeds", func(t *testing.T) {
		var resp search.QueryHybridResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-query-hybrid", customKey,
			search.QueryHybridArgs{
				Query:          "test",
				CollectionName: allowedClass,
				Alpha:          &pureKeyword,
			}, &resp)
		require.NoError(t, err)
	})

	t.Run("hybrid search on forbidden collection fails with auth error", func(t *testing.T) {
		var resp search.QueryHybridResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-query-hybrid", customKey,
			search.QueryHybridArgs{
				Query:          "test",
				CollectionName: forbiddenClass,
				Alpha:          &pureKeyword,
			}, &resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "forbidden")
	})

	t.Run("upsert into allowed collection succeeds", func(t *testing.T) {
		var resp create.UpsertObjectResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-objects-upsert", customKey,
			create.UpsertObjectArgs{
				CollectionName: allowedClass,
				Objects: []create.ObjectToUpsert{
					{Properties: map[string]any{"content": "new object"}},
				},
			}, &resp)
		require.NoError(t, err)
		require.Len(t, resp.Results, 1)
		require.Empty(t, resp.Results[0].Error)
		require.NotEmpty(t, resp.Results[0].ID)
	})

	t.Run("upsert into forbidden collection fails", func(t *testing.T) {
		var resp create.UpsertObjectResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-objects-upsert", customKey,
			create.UpsertObjectArgs{
				CollectionName: forbiddenClass,
				Objects: []create.ObjectToUpsert{
					{Properties: map[string]any{"content": "should not be allowed"}},
				},
			}, &resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "forbidden")
	})

	t.Run("get-config for allowed collection succeeds", func(t *testing.T) {
		var resp read.GetCollectionConfigResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-collections-get-config", customKey,
			read.GetCollectionConfigArgs{CollectionName: allowedClass}, &resp)
		require.NoError(t, err)
		require.Len(t, resp.Collections, 1)
		require.Equal(t, allowedClass, resp.Collections[0].Class)
	})

	t.Run("get-config for forbidden collection is not visible", func(t *testing.T) {
		var resp read.GetCollectionConfigResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-collections-get-config", customKey,
			read.GetCollectionConfigArgs{CollectionName: forbiddenClass}, &resp)
		// The schema reader filters by auth, so the collection is simply not found
		// (information hiding — user doesn't learn the collection exists)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("get-config for all collections only returns allowed", func(t *testing.T) {
		var resp read.GetCollectionConfigResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-collections-get-config", customKey,
			read.GetCollectionConfigArgs{}, &resp)
		require.NoError(t, err)
		// Should only see AllowedCollection, not ForbiddenCollection
		for _, c := range resp.Collections {
			require.NotEqual(t, forbiddenClass, c.Class,
				"user should not see ForbiddenCollection in unfiltered get-config response")
		}
	})
}
