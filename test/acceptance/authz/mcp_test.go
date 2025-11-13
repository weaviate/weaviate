//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func callToolOnceWithAuth[I any, O any](ctx context.Context, t *testing.T, tool, key string, in I, out *O) error {
	c, err := client.NewStreamableHttpClient(
		"http://localhost:9001/mcp",
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
	helper.SetupClient("localhost:8081")
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

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
		var resp *read.GetSchemaResp
		err := callToolOnceWithAuth[any](ctx, t, "get-schema", customKey, nil, &resp)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "user 'custom-user' has insufficient permissions to read_mcp []")
	})

	helper.AssignRoleToUser(t, adminKey, roleName, customUser)

	t.Run("succeed to call tool with MCP permission", func(t *testing.T) {
		var resp *read.GetSchemaResp
		err := callToolOnceWithAuth[any](ctx, t, "get-schema", customKey, nil, &resp)
		t.Log(err)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Schema)
		require.Len(t, resp.Schema.Classes, 1)
	})
}
