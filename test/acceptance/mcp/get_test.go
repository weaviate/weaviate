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

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestGetSchemaTool(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var schema *read.GetSchemaResp
	err := helper.CallToolOnce[any](ctx, t, "get-schema", nil, &schema, apiKey)
	require.Nil(t, err)

	require.NotNil(t, schema)
	require.NotNil(t, schema.Schema)
	require.Len(t, schema.Schema.Classes, 1)
}

func TestGetTenantsTool(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}}, apiKey)

	var tenants *read.GetTenantsResp
	err := helper.CallToolOnce(ctx, t, "get-tenants", &read.GetTenantsArgs{Collection: cls.Class}, &tenants, apiKey)
	require.Nil(t, err)

	require.NotNil(t, tenants)
	require.Len(t, tenants.Tenants, 2)
}
