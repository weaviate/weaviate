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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

func mtClass(name string) *models.Class {
	return &models.Class{
		Class:              name,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}
}

// setupMTClassInBothNamespaces creates an MT-enabled class with the same short
// name under each namespaced user and registers cleanup of the qualified names.
func setupMTClassInBothNamespaces(t *testing.T, ns1, ns2, name, k1, k2 string) {
	t.Helper()
	for _, key := range []string{k1, k2} {
		helper.CreateClassAuth(t, mtClass(name), key)
	}
	t.Cleanup(func() {
		helper.DeleteClassAuth(t, ns1+":"+name, adminKey)
		helper.DeleteClassAuth(t, ns2+":"+name, adminKey)
	})
}

func setupMTClassInNs1(t *testing.T, ns1, name, key string) {
	t.Helper()
	helper.CreateClassAuth(t, mtClass(name), key)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns1+":"+name, adminKey) })
}

func addTenantsAuth(t *testing.T, class string, tenants []*models.Tenant, key string) error {
	t.Helper()
	params := schemaCli.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	_, err := helper.Client(t).Schema.TenantsCreate(params, helper.CreateAuth(key))
	return err
}

func updateTenantsAuth(t *testing.T, class string, tenants []*models.Tenant, key string) error {
	t.Helper()
	params := schemaCli.NewTenantsUpdateParams().WithClassName(class).WithBody(tenants)
	_, err := helper.Client(t).Schema.TenantsUpdate(params, helper.CreateAuth(key))
	return err
}

func deleteTenantsAuth(t *testing.T, class string, tenantNames []string, key string) error {
	t.Helper()
	params := schemaCli.NewTenantsDeleteParams().WithClassName(class).WithTenants(tenantNames)
	_, err := helper.Client(t).Schema.TenantsDelete(params, helper.CreateAuth(key))
	return err
}

func getTenantsAuth(t *testing.T, class, key string) ([]*models.Tenant, error) {
	t.Helper()
	params := schemaCli.NewTenantsGetParams().WithClassName(class)
	resp, err := helper.Client(t).Schema.TenantsGet(params, helper.CreateAuth(key))
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func getOneTenantAuth(t *testing.T, class, tenant, key string) (*models.Tenant, error) {
	t.Helper()
	params := schemaCli.NewTenantsGetOneParams().WithClassName(class).WithTenantName(tenant)
	resp, err := helper.Client(t).Schema.TenantsGetOne(params, helper.CreateAuth(key))
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func tenantExistsAuth(t *testing.T, class, tenant, key string) error {
	t.Helper()
	params := schemaCli.NewTenantExistsParams().WithClassName(class).WithTenantName(tenant)
	_, err := helper.Client(t).Schema.TenantExists(params, helper.CreateAuth(key))
	return err
}

func tenantNames(tenants []*models.Tenant) []string {
	out := make([]string, len(tenants))
	for i, tt := range tenants {
		out[i] = tt.Name
	}
	return out
}

// TestNamespaces_TenantOps exercises namespace qualification and alias
// resolution across the six REST tenant handlers and gRPC TenantsGet. Each
// namespaced caller submits the short collection name; the handler must
// qualify (mutations) or resolve (reads) it before any storage hit, so that
// shards land under the qualified class name and tenant lookups never bleed
// across namespaces.
func TestNamespaces_TenantOps(t *testing.T) {
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	t.Run("REST mutations under namespaced principal land on qualified class", func(t *testing.T) {
		const class = "TenantsCRUD"
		setupMTClassInBothNamespaces(t, ns1, ns2, class, user1Key, user2Key)

		// user1 adds two tenants by short name; user2 adds the same short
		// "alpha" tenant under its own copy of the class.
		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "alpha", ActivityStatus: models.TenantActivityStatusHOT},
			{Name: "beta", ActivityStatus: models.TenantActivityStatusHOT},
		}, user1Key))
		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "alpha", ActivityStatus: models.TenantActivityStatusHOT},
		}, user2Key))

		// Admin's raw view of the qualified classes shows the per-namespace
		// tenant sets exactly as written.
		ns1Tenants, err := getTenantsAuth(t, ns1+":"+class, adminKey)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"alpha", "beta"}, tenantNames(ns1Tenants))

		ns2Tenants, err := getTenantsAuth(t, ns2+":"+class, adminKey)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"alpha"}, tenantNames(ns2Tenants))

		// user1 deactivates beta via the short class name.
		require.NoError(t, updateTenantsAuth(t, class, []*models.Tenant{
			{Name: "beta", ActivityStatus: models.TenantActivityStatusCOLD},
		}, user1Key))
		listed, err := getTenantsAuth(t, ns1+":"+class, adminKey)
		require.NoError(t, err)
		var betaActivity string
		for _, tt := range listed {
			if tt.Name == "beta" {
				betaActivity = tt.ActivityStatus
			}
		}
		assert.Equal(t, models.TenantActivityStatusCOLD, betaActivity)

		// user1 deletes alpha by short name; ns2's alpha is untouched.
		require.NoError(t, deleteTenantsAuth(t, class, []string{"alpha"}, user1Key))
		ns1after, err := getTenantsAuth(t, ns1+":"+class, adminKey)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"beta"}, tenantNames(ns1after))
		ns2after, err := getTenantsAuth(t, ns2+":"+class, adminKey)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"alpha"}, tenantNames(ns2after))
	})

	t.Run("REST reads are namespace-scoped", func(t *testing.T) {
		const class = "TenantsRead"
		setupMTClassInBothNamespaces(t, ns1, ns2, class, user1Key, user2Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "shared"},
		}, user1Key))
		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "shared"},
			{Name: "ns2only"},
		}, user2Key))

		ns1Tenants, err := getTenantsAuth(t, class, user1Key)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"shared"}, tenantNames(ns1Tenants))

		ns2Tenants, err := getTenantsAuth(t, class, user2Key)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"shared", "ns2only"}, tenantNames(ns2Tenants))

		got, err := getOneTenantAuth(t, class, "shared", user1Key)
		require.NoError(t, err)
		assert.Equal(t, "shared", got.Name)

		// ns2only is invisible to user1 — qualifies to user1's own namespace
		// where no such tenant exists.
		_, err = getOneTenantAuth(t, class, "ns2only", user1Key)
		require.Error(t, err)

		require.NoError(t, tenantExistsAuth(t, class, "shared", user1Key))
		require.Error(t, tenantExistsAuth(t, class, "ns2only", user1Key))
	})

	t.Run("global principal uses qualified names; short names do not resolve", func(t *testing.T) {
		const class = "TenantsAdmin"
		setupMTClassInNs1(t, ns1, class, user1Key)

		require.NoError(t, addTenantsAuth(t, ns1+":"+class, []*models.Tenant{
			{Name: "byAdmin"},
		}, adminKey))

		listed, err := getTenantsAuth(t, ns1+":"+class, adminKey)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"byAdmin"}, tenantNames(listed))

		// Short name from a global principal does not resolve — admin has no
		// namespace, so the handler leaves the name as-is and the storage
		// lookup misses.
		_, err = getTenantsAuth(t, class, adminKey)
		require.Error(t, err)
	})

	t.Run("gRPC TenantsGet under namespaced principal returns own tenants", func(t *testing.T) {
		const class = "TenantsGRPC"
		setupMTClassInBothNamespaces(t, ns1, ns2, class, user1Key, user2Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{{Name: "g1"}}, user1Key))
		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{{Name: "g2"}}, user2Key))

		resp1, err := grpcClient.TenantsGet(authCtx(user1Key), &pb.TenantsGetRequest{Collection: class})
		require.NoError(t, err)
		require.Len(t, resp1.Tenants, 1)
		assert.Equal(t, "g1", resp1.Tenants[0].Name)

		resp2, err := grpcClient.TenantsGet(authCtx(user2Key), &pb.TenantsGetRequest{Collection: class})
		require.NoError(t, err)
		require.Len(t, resp2.Tenants, 1)
		assert.Equal(t, "g2", resp2.Tenants[0].Name)
	})

	t.Run("gRPC TenantsGet with Names param filters to the requested subset", func(t *testing.T) {
		const class = "TenantsGRPCNames"
		setupMTClassInNs1(t, ns1, class, user1Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "keep1"},
			{Name: "keep2"},
			{Name: "skip"},
		}, user1Key))

		resp, err := grpcClient.TenantsGet(authCtx(user1Key), &pb.TenantsGetRequest{
			Collection: class,
			Params: &pb.TenantsGetRequest_Names{
				Names: &pb.TenantNames{Values: []string{"keep1", "keep2"}},
			},
		})
		require.NoError(t, err)
		got := make([]string, len(resp.Tenants))
		for i, tt := range resp.Tenants {
			got[i] = tt.Name
		}
		assert.ElementsMatch(t, []string{"keep1", "keep2"}, got)
	})

	t.Run("gRPC TenantsGet under global principal uses qualified collection name", func(t *testing.T) {
		const class = "TenantsGRPCAdmin"
		setupMTClassInNs1(t, ns1, class, user1Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{{Name: "ga1"}}, user1Key))

		resp, err := grpcClient.TenantsGet(authCtx(adminKey), &pb.TenantsGetRequest{Collection: ns1 + ":" + class})
		require.NoError(t, err)
		require.Len(t, resp.Tenants, 1)
		assert.Equal(t, "ga1", resp.Tenants[0].Name)

		// Short name from a global principal does not resolve.
		_, err = grpcClient.TenantsGet(authCtx(adminKey), &pb.TenantsGetRequest{Collection: class})
		require.Error(t, err)
	})

	t.Run("alias is not a backdoor: tenant mutations via alias name fail", func(t *testing.T) {
		const (
			class = "AliasBackdoorTarget"
			alias = "AliasBackdoor"
		)
		setupMTClassInNs1(t, ns1, class, user1Key)

		// Seed the underlying class with a known tenant so we can detect any
		// state change after the rejected calls.
		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "seed", ActivityStatus: models.TenantActivityStatusHOT},
		}, user1Key))

		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: class}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, ns1+":"+alias, helper.CreateAuth(adminKey))

		// Mutations qualify via QualifyClass, which does not resolve aliases.
		// All three calls must fail.
		require.Error(t, addTenantsAuth(t, alias, []*models.Tenant{
			{Name: "viaAlias", ActivityStatus: models.TenantActivityStatusHOT},
		}, user1Key))
		require.Error(t, updateTenantsAuth(t, alias, []*models.Tenant{
			{Name: "seed", ActivityStatus: models.TenantActivityStatusCOLD},
		}, user1Key))
		require.Error(t, deleteTenantsAuth(t, alias, []string{"seed"}, user1Key))

		// Underlying class state must be unchanged: same single tenant, still
		// HOT, no leaked "viaAlias" tenant.
		listed, err := getTenantsAuth(t, ns1+":"+class, adminKey)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, "seed", listed[0].Name)
		assert.Equal(t, models.TenantActivityStatusHOT, listed[0].ActivityStatus)
	})

	t.Run("namespaced caller submitting namespace-qualified class double-prefixes", func(t *testing.T) {
		const class = "TenantsDoublePrefix"
		setupMTClassInNs1(t, ns1, class, user1Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{{Name: "x"}}, user1Key))

		// user1 supplying the already-qualified "<ns>:TenantsDoublePrefix"
		// qualifies again to "<ns>:<ns>:TenantsDoublePrefix" — no such class.
		_, err := getTenantsAuth(t, ns1+":"+class, user1Key)
		require.Error(t, err)

		err = addTenantsAuth(t, ns1+":"+class, []*models.Tenant{{Name: "y"}}, user1Key)
		require.Error(t, err)
	})

	t.Run("single-tenant read endpoints resolve aliases", func(t *testing.T) {
		const (
			class = "AliasSingleTarget"
			alias = "AliasSingle"
		)
		setupMTClassInNs1(t, ns1, class, user1Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{
			{Name: "present", ActivityStatus: models.TenantActivityStatusHOT},
		}, user1Key))

		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: class}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, ns1+":"+alias, helper.CreateAuth(adminKey))

		// GetConsistentTenant via alias resolves to the underlying class.
		// retryOnAliasLag absorbs the brief window where the alias entry
		// has been applied on the leader but the follower has not yet
		// replicated it; once the first read succeeds the rest are safe.
		var got *models.Tenant
		retryOnAliasLag(t, func() error {
			var err error
			got, err = getOneTenantAuth(t, alias, "present", user1Key)
			return err
		})
		assert.Equal(t, "present", got.Name)
		assert.Equal(t, models.TenantActivityStatusHOT, got.ActivityStatus)

		// Missing tenant on the same alias still 404s — alias resolution
		// must not paper over a real not-found.
		_, err := getOneTenantAuth(t, alias, "missing", user1Key)
		require.Error(t, err)

		// ConsistentTenantExists via alias resolves to the underlying class.
		require.NoError(t, tenantExistsAuth(t, alias, "present", user1Key))
		require.Error(t, tenantExistsAuth(t, alias, "missing", user1Key))
	})

	t.Run("alias-resolved read returns tenants of underlying class", func(t *testing.T) {
		const (
			class = "TenantsAliasTarget"
			alias = "TenantsAlias"
		)
		setupMTClassInNs1(t, ns1, class, user1Key)

		require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{{Name: "t1"}, {Name: "t2"}}, user1Key))

		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: class}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, ns1+":"+alias, helper.CreateAuth(adminKey))

		// retryOnAliasLag absorbs the brief window where the alias entry
		// has been applied on the leader but the follower has not yet
		// replicated it; once the first read succeeds the rest are safe.
		var listed []*models.Tenant
		retryOnAliasLag(t, func() error {
			var err error
			listed, err = getTenantsAuth(t, alias, user1Key)
			return err
		})
		assert.ElementsMatch(t, []string{"t1", "t2"}, tenantNames(listed))

		got, err := getOneTenantAuth(t, alias, "t1", user1Key)
		require.NoError(t, err)
		assert.Equal(t, "t1", got.Name)

		require.NoError(t, tenantExistsAuth(t, alias, "t2", user1Key))

		resp, err := grpcClient.TenantsGet(authCtx(user1Key), &pb.TenantsGetRequest{Collection: alias})
		require.NoError(t, err)
		grpcNames := make([]string, len(resp.Tenants))
		for i, tt := range resp.Tenants {
			grpcNames[i] = tt.Name
		}
		assert.ElementsMatch(t, []string{"t1", "t2"}, grpcNames)
	})
}
