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

func TestCollectionsGetConfigTool(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := articles.ParagraphsClass()
	helper.DeleteClassAuth(t, cls.Class, apiKey)
	helper.CreateClassAuth(t, cls, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var schema *read.GetCollectionConfigResp
	err := helper.CallToolOnce[any](ctx, t, "weaviate-collections-get-config", nil, &schema)
	require.Nil(t, err)

	require.NotNil(t, schema)
	require.NotNil(t, schema.Collections)
	require.Len(t, schema.Collections, 1)
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
	err := helper.CallToolOnce(ctx, t, "weaviate-tenants-list", &read.GetTenantsArgs{CollectionName: cls.Class}, &tenants)
	require.Nil(t, err)

	require.NotNil(t, tenants)
	require.Len(t, tenants.Tenants, 2)
}
