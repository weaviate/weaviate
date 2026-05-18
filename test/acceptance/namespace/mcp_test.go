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

package namespace

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	mcpToolHybrid     = "weaviate-query-hybrid"
	mcpToolUpsert     = "weaviate-objects-upsert"
	mcpToolGetConfig  = "weaviate-collections-get-config"
	mcpToolGetTenants = "weaviate-tenants-list"
)

// TestNamespaces_MCP exercises the MCP backend on a namespace-enabled cluster.
// Per the namespace-prefix validator, a namespaced principal that submits a
// qualified name is rejected up front with "is not a valid class name".
func TestNamespaces_MCP(t *testing.T) {
	user1Key, _ := twoNamespaces(t)

	alpha0 := 0.0
	const (
		short   = "Movies"
		qualNs1 = "customer1:Movies"
		qualNs2 = "customer2:Movies"
	)

	setupClassInNs1(t, short, user1Key)

	t.Run("namespaced principal, short input works on single-tenant tools", func(t *testing.T) {
		// upsert
		var upsertResp *create.UpsertObjectResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolUpsert, &create.UpsertObjectArgs{
			CollectionName: short,
			Objects: []create.ObjectToUpsert{
				{Properties: map[string]any{"title": "Inception"}},
			},
		}, &upsertResp, user1Key)
		require.NoError(t, err)
		require.Len(t, upsertResp.Results, 1)
		require.Empty(t, upsertResp.Results[0].Error)

		// Verify the object landed under the qualified class name.
		got, err := helper.GetObjectAuth(t, qualNs1, strfmt.UUID(upsertResp.Results[0].ID), adminKey)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, qualNs1, got.Class)

		// get-config (specific)
		var cfg *read.GetCollectionConfigResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: short}, &cfg, user1Key)
		require.NoError(t, err)
		require.Len(t, cfg.Collections, 1)
		assert.Equal(t, qualNs1, cfg.Collections[0].Class)

		// hybrid (BM25-only to avoid needing a vectorizer)
		var hybridResp *search.QueryHybridResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: short,
			Query:          "Inception",
			Alpha:          &alpha0,
		}, &hybridResp, user1Key)
		require.NoError(t, err)
		require.NotEmpty(t, hybridResp.Results)
	})

	// tenants-list needs an MT class. Kept separate to avoid forcing TenantName
	// onto every other tool call above.
	t.Run("namespaced principal, tenants-list with short input", func(t *testing.T) {
		const mtShort = "Theaters"
		setupMTClassInNs1(t, mtShort, user1Key)
		require.NoError(t, addTenantsAuth(t, "customer1:"+mtShort,
			[]*models.Tenant{{Name: "t1", ActivityStatus: models.TenantActivityStatusHOT}}, user1Key))

		var tenantsResp *read.GetTenantsResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolGetTenants,
			&read.GetTenantsArgs{CollectionName: mtShort}, &tenantsResp, user1Key)
		require.NoError(t, err)
		require.Len(t, tenantsResp.Tenants, 1)
		assert.Equal(t, "t1", tenantsResp.Tenants[0].Name)
	})

	// On NS-enabled clusters the namespace-prefix validator rejects qualified
	// input from a namespaced principal with "is not a valid class name".
	// Only the tools that resolve at the MCP handler (hybrid, get-config) see
	// this validator. Upsert and tenants-list resolve in deeper layers and are
	// covered by the namespace REST/gRPC tests.
	t.Run("namespaced principal, own-namespace qualified input is rejected", func(t *testing.T) {
		var hybridResp *search.QueryHybridResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: qualNs1,
			Query:          "x",
			Alpha:          &alpha0,
		}, &hybridResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")

		var cfg *read.GetCollectionConfigResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: qualNs1}, &cfg, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("namespaced principal, foreign-namespace qualified input is rejected", func(t *testing.T) {
		var hybridResp *search.QueryHybridResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: qualNs2,
			Query:          "x",
			Alpha:          &alpha0,
		}, &hybridResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")

		var cfg *read.GetCollectionConfigResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: qualNs2}, &cfg, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("global admin, qualified input succeeds", func(t *testing.T) {
		var cfg *read.GetCollectionConfigResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: qualNs1}, &cfg, adminKey)
		require.NoError(t, err)
		require.Len(t, cfg.Collections, 1)
		assert.Equal(t, qualNs1, cfg.Collections[0].Class)
	})

	// Global principals carry no namespace, so the resolver doesn't qualify
	// "Movies" and the equality filter against the schema (which stores
	// "customer1:Movies") returns the existing "not found" path.
	t.Run("global admin, short input does not resolve to namespaced class", func(t *testing.T) {
		var cfg *read.GetCollectionConfigResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: short}, &cfg, adminKey)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("alias resolves through hybrid search", func(t *testing.T) {
		const alias = "Films"
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: short}, user1Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, "customer1:"+alias, helper.CreateAuth(adminKey))
		})

		retryOnAliasLag(t, func() error {
			var hybridResp *search.QueryHybridResp
			return helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
				CollectionName: alias,
				Query:          "Inception",
				Alpha:          &alpha0,
			}, &hybridResp, user1Key)
		})
	})

	// filterext.Parse rejects reference-path filters (path length > 1) on
	// namespace-enabled clusters. Confirms the hardcoded `false` removed in
	// hybrid.go is now wired to s.namespacesEnabled.
	t.Run("hybrid reference-path filter is rejected on namespace-enabled cluster", func(t *testing.T) {
		var hybridResp *search.QueryHybridResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: short,
			Query:          "Inception",
			Alpha:          &alpha0,
			Filters: map[string]any{
				"path":      []any{"hasAuthor", "Author", "name"},
				"operator":  "Equal",
				"valueText": "Anyone",
			},
		}, &hybridResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reference-path filters")
	})
}
