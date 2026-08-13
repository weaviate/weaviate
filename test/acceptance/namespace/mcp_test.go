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
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
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
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	alpha0 := 0.0
	const short = "Movies"
	qualNs1 := ns1 + ":" + short
	qualNs2 := ns2 + ":" + short

	setupClassInNs1(t, ns1, short, user1Key)

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

		// get-config (specific) — namespaced principals see the short name in the
		// response; the qualified prefix is stripped before serialization.
		var cfg *read.GetCollectionConfigResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: short}, &cfg, user1Key)
		require.NoError(t, err)
		require.Len(t, cfg.Collections, 1)
		assert.Equal(t, short, cfg.Collections[0].Class)

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
		setupMTClassInNs1(t, ns1, mtShort, user1Key)
		require.NoError(t, addTenantsAuth(t, mtShort,
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
	// Hybrid and get-config see the validator at the MCP handler; upsert and
	// tenants-list see it through BatchManager.AddObjects / GetConsistentTenants
	// respectively. All four are covered here to lock the MCP-wrapped error.
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

		var upsertResp *create.UpsertObjectResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolUpsert, &create.UpsertObjectArgs{
			CollectionName: qualNs1,
			Objects: []create.ObjectToUpsert{
				{Properties: map[string]any{"title": "Tenet"}},
			},
		}, &upsertResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")

		var tenantsResp *read.GetTenantsResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolGetTenants,
			&read.GetTenantsArgs{CollectionName: qualNs1}, &tenantsResp, user1Key)
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

		var upsertResp *create.UpsertObjectResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolUpsert, &create.UpsertObjectArgs{
			CollectionName: qualNs2,
			Objects: []create.ObjectToUpsert{
				{Properties: map[string]any{"title": "Tenet"}},
			},
		}, &upsertResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")

		var tenantsResp *read.GetTenantsResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolGetTenants,
			&read.GetTenantsArgs{CollectionName: qualNs2}, &tenantsResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("global admin, qualified input succeeds", func(t *testing.T) {
		// Global principals carry no namespace, so stripping is a no-op and the
		// qualified name flows through unchanged.
		var cfg *read.GetCollectionConfigResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: qualNs1}, &cfg, adminKey)
		require.NoError(t, err)
		require.Len(t, cfg.Collections, 1)
		assert.Equal(t, qualNs1, cfg.Collections[0].Class)

		var hybridResp *search.QueryHybridResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: qualNs1,
			Query:          "Inception",
			Alpha:          &alpha0,
		}, &hybridResp, adminKey)
		require.NoError(t, err)
		require.NotEmpty(t, hybridResp.Results)
	})

	// Global principals carry no namespace, so the resolver doesn't qualify
	// "Movies" and the equality filter against the schema (which stores the
	// namespaced "Movies" class) returns the existing "not found" path. Hybrid hits
	// the traverser, which surfaces a class-missing error.
	t.Run("global admin, short input does not resolve to namespaced class", func(t *testing.T) {
		var cfg *read.GetCollectionConfigResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: short}, &cfg, adminKey)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		var hybridResp *search.QueryHybridResp
		err = helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: short,
			Query:          "Inception",
			Alpha:          &alpha0,
		}, &hybridResp, adminKey)
		require.Error(t, err)
	})

	t.Run("alias resolves through hybrid search", func(t *testing.T) {
		const alias = "Films"
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: short}, user1Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, ns1+":"+alias, helper.CreateAuth(adminKey))
		})

		var hybridResp *search.QueryHybridResp
		retryOnAliasLag(t, func() error {
			return helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
				CollectionName: alias,
				Query:          "Inception",
				Alpha:          &alpha0,
			}, &hybridResp, user1Key)
		})
		require.NotNil(t, hybridResp)
		require.NotEmpty(t, hybridResp.Results)
	})

	// filterext.Parse qualifies reference-path inner class segments against
	// the source's namespace on NS-enabled clusters (mirroring the gRPC
	// parser). user1Key is a namespaced caller, so QualifyRefTarget rejects
	// ANY prefix it types on an inner class (the resolver adds the prefix
	// automatically) — here a foreign one. The rejection happens BEFORE the
	// schema lookup, so the call fails with the "is not a valid class name"
	// wording regardless of whether the source class has the named ref property.
	t.Run("hybrid reference-path filter with prefixed inner class is rejected", func(t *testing.T) {
		var hybridResp *search.QueryHybridResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: short,
			Query:          "Inception",
			Alpha:          &alpha0,
			Filters: map[string]any{
				"path":      []any{"hasAuthor", ns2 + ":Author", "name"},
				"operator":  "Equal",
				"valueText": "Anyone",
			},
		}, &hybridResp, user1Key)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	// Cross-namespace isolation backstop. The RBAC matcher does not confine the
	// mcp domain (it is neither namespace-qualified on write nor operator-only),
	// so confinement rests entirely on each tool self-scoping its collection
	// argument to the caller's namespace. Create the same short name in both
	// namespaces with distinct content and assert a caller only ever reaches its
	// own namespace's data through every collection-scoped tool. A future tool
	// that forgets to scope would leak across namespaces and fail here.
	t.Run("cross-namespace isolation on colliding short name", func(t *testing.T) {
		const collide = "IsoFilms"
		setupCollidingClassInBothNamespaces(t, ns1, ns2, collide, "ns1prop", "ns2prop", user1Key, user2Key)
		id := strfmt.UUID("cccccccc-1111-1111-1111-111111111111")
		seedTwo(t, collide, id, "collide onlyns1", "collide onlyns2", user1Key, user2Key)

		// hybrid: each caller sees only its own object.
		var r1 *search.QueryHybridResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: collide, Query: "collide", Alpha: &alpha0,
		}, &r1, user1Key))
		assert.Equal(t, []string{"collide onlyns1"}, hybridTitles(t, r1))

		var r2 *search.QueryHybridResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: collide, Query: "collide", Alpha: &alpha0,
		}, &r2, user2Key))
		assert.Equal(t, []string{"collide onlyns2"}, hybridTitles(t, r2))

		// get-config: each caller sees only its own namespace's schema.
		var c1 *read.GetCollectionConfigResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolGetConfig,
			&read.GetCollectionConfigArgs{CollectionName: collide}, &c1, user1Key))
		require.Len(t, c1.Collections, 1)
		assert.Contains(t, propNames(c1.Collections[0]), "ns1prop")
		assert.NotContains(t, propNames(c1.Collections[0]), "ns2prop")

		// upsert: the write lands only in the caller's namespace.
		var up *create.UpsertObjectResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolUpsert, &create.UpsertObjectArgs{
			CollectionName: collide,
			Objects:        []create.ObjectToUpsert{{Properties: map[string]any{"title": "written by uniqueterm"}}},
		}, &up, user1Key))
		require.Len(t, up.Results, 1)
		require.Empty(t, up.Results[0].Error)

		var seen *search.QueryHybridResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: collide, Query: "uniqueterm", Alpha: &alpha0,
		}, &seen, user1Key))
		assert.Equal(t, []string{"written by uniqueterm"}, hybridTitles(t, seen))

		var absent *search.QueryHybridResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: collide, Query: "uniqueterm", Alpha: &alpha0,
		}, &absent, user2Key))
		assert.Empty(t, hybridTitles(t, absent))

		// tenants-list: each caller sees only its own namespace's tenants.
		const collideMT = "IsoTheaters"
		setupMTClassInBothNamespaces(t, ns1, ns2, collideMT, user1Key, user2Key)
		require.NoError(t, addTenantsAuth(t, collideMT,
			[]*models.Tenant{{Name: "tns1", ActivityStatus: models.TenantActivityStatusHOT}}, user1Key))
		require.NoError(t, addTenantsAuth(t, collideMT,
			[]*models.Tenant{{Name: "tns2", ActivityStatus: models.TenantActivityStatusHOT}}, user2Key))

		var tr1 *read.GetTenantsResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolGetTenants,
			&read.GetTenantsArgs{CollectionName: collideMT}, &tr1, user1Key))
		assert.Equal(t, []string{"tns1"}, tenantNames(tr1.Tenants))

		var tr2 *read.GetTenantsResp
		require.NoError(t, helper.CallToolOnce(t.Context(), t, mcpToolGetTenants,
			&read.GetTenantsArgs{CollectionName: collideMT}, &tr2, user2Key))
		assert.Equal(t, []string{"tns2"}, tenantNames(tr2.Tenants))
	})
}

// mcpCollectionScopedTools are the MCP tools exercised by the cross-namespace
// isolation subtest in TestNamespaces_MCP. Each accepts a collection_name and
// must confine that argument to the caller's namespace. Kept in sync with the
// registry by TestNamespaces_MCP_IsolationCoversAllCollectionTools.
var mcpCollectionScopedTools = map[string]bool{
	mcpToolHybrid:     true,
	mcpToolGetConfig:  true,
	mcpToolUpsert:     true,
	mcpToolGetTenants: true,
}

// setupCollidingClassInBothNamespaces creates a class with the same short name
// under each namespaced user, each carrying a distinct extra property so a
// get-config response reveals which namespace's schema was returned.
func setupCollidingClassInBothNamespaces(t *testing.T, ns1, ns2, name, extra1, extra2, k1, k2 string) {
	t.Helper()
	mk := func(extra string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: extra, DataType: []string{"text"}},
			},
		}
	}
	helper.CreateClassAuth(t, mk(extra1), k1)
	helper.CreateClassAuth(t, mk(extra2), k2)
	t.Cleanup(func() {
		helper.DeleteClassAuth(t, ns1+":"+name, adminKey)
		helper.DeleteClassAuth(t, ns2+":"+name, adminKey)
	})
}

func hybridTitles(t *testing.T, resp *search.QueryHybridResp) []string {
	t.Helper()
	titles := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		m, ok := r.(map[string]any)
		require.True(t, ok, "hybrid result should be a JSON object")
		if title, ok := m["title"].(string); ok {
			titles = append(titles, title)
		}
	}
	return titles
}

func propNames(class *models.Class) []string {
	names := make([]string, 0, len(class.Properties))
	for _, p := range class.Properties {
		names = append(names, p.Name)
	}
	return names
}

// TestNamespaces_MCP_IsolationCoversAllCollectionTools guards the isolation
// subtest above. Every registered MCP tool that accepts a collection_name must
// be listed in mcpCollectionScopedTools (and thus exercised for cross-namespace
// isolation). Because the mcp authz domain provides no namespace confinement,
// adding a collection-scoped tool without proving its isolation is a latent
// cross-namespace escape; this test fails until the new tool is covered, turning
// a review checklist item into a CI gate.
func TestNamespaces_MCP_IsolationCoversAllCollectionTools(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	c, err := client.NewStreamableHttpClient(helper.GetWeaviateURL()+"/v1/mcp",
		transport.WithHTTPHeaders(map[string]string{"Authorization": "Bearer " + adminKey}))
	require.NoError(t, err)
	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	require.NoError(t, err)
	list, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	require.NoError(t, err)
	require.NotNil(t, list)

	registered := make(map[string]bool)
	for _, tool := range list.Tools {
		if _, ok := tool.InputSchema.Properties["collection_name"]; !ok {
			continue
		}
		registered[tool.Name] = true
		assert.Truef(t, mcpCollectionScopedTools[tool.Name],
			"MCP tool %q accepts a collection_name but is not covered by the "+
				"cross-namespace isolation subtest; the mcp authz domain has no namespace "+
				"backstop, so exercise it there and list it in mcpCollectionScopedTools", tool.Name)
	}

	// Guard against a stale coverage entry that would let the check above pass
	// vacuously for a tool that was renamed or dropped.
	for name := range mcpCollectionScopedTools {
		assert.Truef(t, registered[name],
			"MCP tool %q is listed in mcpCollectionScopedTools but is not a registered "+
				"collection-scoped tool", name)
	}
}
