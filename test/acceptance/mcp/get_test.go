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

package mcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func callToolOnce[I any, O any](ctx context.Context, t *testing.T, tool string, in I, out *O) error {
	client, err := client.NewStreamableHttpClient("http://localhost:9000/mcp")
	if err != nil {
		return err
	}

	_, err = client.Initialize(ctx, mcp.InitializeRequest{})
	if err != nil {
		return err
	}

	res, err := client.CallTool(ctx, mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      tool,
			Arguments: in,
		},
	})
	if err != nil {
		return err
	}
	require.NotNil(t, res)
	require.False(t, res.IsError)
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

func TestGetSchemaTool(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)
	defer helper.DeleteClass(t, cls.Class)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var schema *read.GetSchemaResp
	err := callToolOnce[any](ctx, t, "get-schema", nil, &schema)
	require.Nil(t, err)

	require.NotNil(t, schema)
	require.NotNil(t, schema.Schema)
	require.Len(t, schema.Schema.Classes, 1)
}

func TestGetTenantsTool(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)
	defer helper.DeleteClass(t, cls.Class)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	helper.CreateTenants(t, cls.Class, []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}})

	var tenants *read.GetTenantsResp
	err := callToolOnce(ctx, t, "get-tenants", &read.GetTenantsArgs{Collection: cls.Class}, &tenants)
	require.Nil(t, err)

	require.NotNil(t, tenants)
	require.Len(t, tenants.Tenants, 2)
}
