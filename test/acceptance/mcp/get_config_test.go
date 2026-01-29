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

const (
	toolNameGetConfig  = "weaviate-collections-get-config"
	toolNameGetTenants = "weaviate-tenants-list"
)

// setupGetConfigTest handles the boilerplate setup: client init, class creation, and context generation.
// It returns the class schema, the context, and a cleanup function.
func setupGetConfigTest(t *testing.T) (*models.Class, context.Context, func()) {
	return setupGetConfigTestWithTenants(t, nil)
}

// setupMultiTenantTest creates a multi-tenant class and returns it with context and cleanup.
func setupMultiTenantTest(t *testing.T, tenantNames []string) (*models.Class, context.Context, func()) {
	return setupGetConfigTestWithTenants(t, tenantNames)
}

// setupGetConfigTestWithTenants is the common setup function for both single and multi-tenant tests.
func setupGetConfigTestWithTenants(t *testing.T, tenantNames []string) (*models.Class, context.Context, func()) {
	helper.SetupClient(testServerAddr)
	cls := articles.ParagraphsClass()

	if tenantNames != nil {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
	}

	helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	helper.CreateClassAuth(t, cls, testAPIKey)

	if len(tenantNames) > 0 {
		tenants := make([]*models.Tenant, len(tenantNames))
		for i, name := range tenantNames {
			tenants[i] = &models.Tenant{Name: name}
		}
		helper.CreateTenantsAuth(t, cls.Class, tenants, testAPIKey)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, cls.Class, testAPIKey)
	}

	return cls, ctx, cleanup
}

func TestCollectionsGetConfigTool(t *testing.T) {
	_, ctx, cleanup := setupGetConfigTest(t)
	defer cleanup()

	var schema *read.GetCollectionConfigResp
	err := helper.CallToolOnce[any](ctx, t, toolNameGetConfig, nil, &schema, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, schema)
	require.NotNil(t, schema.Collections)
	require.Len(t, schema.Collections, 1)
}

func TestCollectionsGetSpecificConfigTool(t *testing.T) {
	cls, ctx, cleanup := setupGetConfigTest(t)
	defer cleanup()

	var schema *read.GetCollectionConfigResp
	err := helper.CallToolOnce(ctx, t, toolNameGetConfig, &read.GetCollectionConfigArgs{
		CollectionName: cls.Class,
	}, &schema, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, schema)
	require.NotNil(t, schema.Collections)
	require.Len(t, schema.Collections, 1)
	require.Equal(t, cls.Class, schema.Collections[0].Class)
}

func TestGetTenantsTool(t *testing.T) {
	cls, ctx, cleanup := setupMultiTenantTest(t, []string{"tenant1", "tenant2"})
	defer cleanup()

	var tenants *read.GetTenantsResp
	err := helper.CallToolOnce(ctx, t, toolNameGetTenants, &read.GetTenantsArgs{CollectionName: cls.Class}, &tenants, testAPIKey)
	require.Nil(t, err)

	require.NotNil(t, tenants)
	require.Len(t, tenants.Tenants, 2)
}
