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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzNamespaces(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	manageUser := "manage-user"
	manageKey := "manage-key"
	scopedManageUser := "scoped-manage-user"
	scopedManageKey := "scoped-manage-key"
	noPermsUser := "no-perms-user"
	noPermsKey := "no-perms-key"

	_, down := composeUpWithSettings(t,
		map[string]string{adminUser: adminKey},
		map[string]string{
			manageUser:       manageKey,
			scopedManageUser: scopedManageKey,
			noPermsUser:      noPermsKey,
		},
		nil,
		false,
		nil,
		true, // withDbUsers — needed for the namespace-scoping subtests below.
		true, // withNamespaces
	)
	defer down()

	t.Run("admin can CRUD namespaces", func(t *testing.T) {
		const name = "authzadmin"
		helper.CreateNamespace(t, name, adminKey)

		assert.Equal(t, name, helper.GetNamespace(t, name, adminKey).Name)
		assert.Contains(t, authzNamespaceNames(helper.ListNamespaces(t, adminKey)), name)

		helper.DeleteNamespace(t, name, adminKey)
	})

	t.Run("manage_namespaces on wildcard grants all CRUD verbs", func(t *testing.T) {
		const (
			roleName = "manage-all-role"
			nsName   = "manageall"
		)
		helper.CreateRoleAndAssign(t, adminKey, manageUser, roleName,
			helper.NewNamespacesPermission().
				WithAction(authorization.ManageNamespaces).
				WithNamespace("*").
				Permission(),
		)

		helper.CreateNamespace(t, nsName, manageKey)
		assert.Equal(t, nsName, helper.GetNamespace(t, nsName, manageKey).Name)
		assert.Contains(t, authzNamespaceNames(helper.ListNamespaces(t, manageKey)), nsName)
		helper.DeleteNamespace(t, nsName, manageKey)

		// After delete, the handler pre-check returns 404.
		_, err := helper.Client(t).Namespaces.DeleteNamespace(
			namespaces.NewDeleteNamespaceParams().WithNamespaceID(nsName),
			helper.CreateAuth(manageKey),
		)
		require.Error(t, err)
		var notFound *namespaces.DeleteNamespaceNotFound
		require.True(t, errors.As(err, &notFound), "expected DeleteNamespaceNotFound, got %T: %v", err, err)
	})

	t.Run("scoped manage_namespaces filters access to one namespace", func(t *testing.T) {
		const (
			roleName     = "scoped-manage-role"
			scopedName   = "scopedns"
			otherName    = "otherns"
			unscopedName = "differentns"
		)

		helper.CreateNamespace(t, scopedName, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, scopedName, adminKey) })
		helper.CreateNamespace(t, otherName, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, otherName, adminKey) })

		helper.CreateRoleAndAssign(t, adminKey, scopedManageUser, roleName,
			helper.NewNamespacesPermission().
				WithAction(authorization.ManageNamespaces).
				WithNamespace(scopedName).
				Permission(),
		)

		// In-scope GET succeeds.
		assert.Equal(t, scopedName, helper.GetNamespace(t, scopedName, scopedManageKey).Name)

		// Out-of-scope GET is 403.
		_, err := helper.Client(t).Namespaces.GetNamespace(
			namespaces.NewGetNamespaceParams().WithNamespaceID(otherName),
			helper.CreateAuth(scopedManageKey),
		)
		require.Error(t, err)
		var getForbidden *namespaces.GetNamespaceForbidden
		require.True(t, errors.As(err, &getForbidden), "expected GetNamespaceForbidden, got %T: %v", err, err)

		// CREATE outside scope is 403.
		_, err = helper.Client(t).Namespaces.CreateNamespace(
			namespaces.NewCreateNamespaceParams().WithNamespaceID(unscopedName),
			helper.CreateAuth(scopedManageKey),
		)
		require.Error(t, err)
		var createForbidden *namespaces.CreateNamespaceForbidden
		require.True(t, errors.As(err, &createForbidden), "expected CreateNamespaceForbidden, got %T: %v", err, err)

		// DELETE outside scope is 403.
		_, err = helper.Client(t).Namespaces.DeleteNamespace(
			namespaces.NewDeleteNamespaceParams().WithNamespaceID(otherName),
			helper.CreateAuth(scopedManageKey),
		)
		require.Error(t, err)
		var delForbidden *namespaces.DeleteNamespaceForbidden
		require.True(t, errors.As(err, &delForbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)

		// LIST filters via FilterAuthorizedResources.
		names := authzNamespaceNames(helper.ListNamespaces(t, scopedManageKey))
		assert.Contains(t, names, scopedName)
		assert.NotContains(t, names, otherName)
	})

	t.Run("user with no roles is denied on CRUD and sees empty list", func(t *testing.T) {
		const name = "nopermsattempt"

		_, err := helper.Client(t).Namespaces.CreateNamespace(
			namespaces.NewCreateNamespaceParams().WithNamespaceID(name),
			helper.CreateAuth(noPermsKey),
		)
		require.Error(t, err)
		var createForbidden *namespaces.CreateNamespaceForbidden
		require.True(t, errors.As(err, &createForbidden), "expected CreateNamespaceForbidden, got %T: %v", err, err)

		_, err = helper.Client(t).Namespaces.GetNamespace(
			namespaces.NewGetNamespaceParams().WithNamespaceID(name),
			helper.CreateAuth(noPermsKey),
		)
		require.Error(t, err)
		var getForbidden *namespaces.GetNamespaceForbidden
		require.True(t, errors.As(err, &getForbidden), "expected GetNamespaceForbidden, got %T: %v", err, err)

		_, err = helper.Client(t).Namespaces.DeleteNamespace(
			namespaces.NewDeleteNamespaceParams().WithNamespaceID(name),
			helper.CreateAuth(noPermsKey),
		)
		require.Error(t, err)
		var delForbidden *namespaces.DeleteNamespaceForbidden
		require.True(t, errors.As(err, &delForbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)

		assert.Empty(t, helper.ListNamespaces(t, noPermsKey))
	})

	// Namespace-scoping subtests (matcher + role-safety guards). Reuses the
	// same compose to avoid a second cluster boot.
	const (
		ns1 = "customer1"
		ns2 = "customer2"
	)
	helper.CreateNamespace(t, ns1, adminKey)
	defer helper.DeleteNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)
	defer helper.DeleteNamespace(t, ns2, adminKey)

	user1Key := helper.CreateUserWithNamespace(t, "u1", ns1, adminKey)
	defer helper.DeleteUser(t, ns1+":u1", adminKey)
	user2Key := helper.CreateUserWithNamespace(t, "u2", ns2, adminKey)
	defer helper.DeleteUser(t, ns2+":u2", adminKey)

	// Namespaced DB users start with no permissions. Grant both a single
	// namespace-relative role covering create_collections and create_aliases;
	// the matcher specializes the unqualified `*` template per caller, so each
	// user can only create within their own namespace.
	const bootstrapRole = "ns-bootstrap-create"
	helper.CreateRole(t, adminKey, &models.Role{
		Name: String(bootstrapRole),
		Permissions: []*models.Permission{
			helper.NewCollectionsPermission().WithAction(authorization.CreateCollections).WithCollection("*").Permission(),
			helper.NewAliasesPermission().WithAction(authorization.CreateAliases).WithCollection("*").WithAlias("*").Permission(),
		},
	})
	defer helper.DeleteRole(t, adminKey, bootstrapRole)
	helper.AssignRoleToUser(t, adminKey, bootstrapRole, ns1+":u1")
	defer helper.RevokeRoleFromUser(t, adminKey, bootstrapRole, ns1+":u1")
	helper.AssignRoleToUser(t, adminKey, bootstrapRole, ns2+":u2")
	defer helper.RevokeRoleFromUser(t, adminKey, bootstrapRole, ns2+":u2")

	helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user1Key)
	defer helper.DeleteClassAuth(t, ns1+":Movies", adminKey)
	helper.CreateClassAuth(t, &models.Class{Class: "MoviesArchive"}, user1Key)
	defer helper.DeleteClassAuth(t, ns1+":MoviesArchive", adminKey)
	helper.CreateClassAuth(t, &models.Class{Class: "Music"}, user1Key)
	defer helper.DeleteClassAuth(t, ns1+":Music", adminKey)

	helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user2Key)
	defer helper.DeleteClassAuth(t, ns2+":Movies", adminKey)
	helper.CreateClassAuth(t, &models.Class{Class: "Music"}, user2Key)
	defer helper.DeleteClassAuth(t, ns2+":Music", adminKey)

	t.Run("read_collections wildcard scope: namespaced caller can read own-NS collection by unqualified name", func(t *testing.T) {
		const role = "read-all"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		assert.Equal(t, "Movies", helper.GetClassAuth(t, "Movies", user1Key).Class)
	})

	t.Run("read_collections regex scope (Movies*): matches in own namespace only", func(t *testing.T) {
		const role = "read-movies-prefix"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("Movies*").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		assert.Equal(t, "Movies", helper.GetClassAuth(t, "Movies", user1Key).Class)
		assert.Equal(t, "MoviesArchive", helper.GetClassAuth(t, "MoviesArchive", user1Key).Class)

		_, err := helper.Client(t).Schema.SchemaObjectsGet(
			schema.NewSchemaObjectsGetParams().WithClassName("Music"),
			helper.CreateAuth(user1Key),
		)
		require.Error(t, err, "Music should not match Movies.*")
	})

	t.Run("read_collections exact scope (Movies): allows only Movies in own namespace", func(t *testing.T) {
		const role = "read-movies-exact"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("Movies").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		assert.Equal(t, "Movies", helper.GetClassAuth(t, "Movies", user1Key).Class)

		_, err := helper.Client(t).Schema.SchemaObjectsGet(
			schema.NewSchemaObjectsGetParams().WithClassName("MoviesArchive"),
			helper.CreateAuth(user1Key),
		)
		require.Error(t, err, "exact-scope role must not match MoviesArchive")
	})

	t.Run("schema dump filters to own namespace for namespaced caller", func(t *testing.T) {
		const role = "read-all-for-dump"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		resp, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams(), helper.CreateAuth(user1Key))
		require.NoError(t, err)
		got := classNames(resp.Payload.Classes)
		assert.ElementsMatch(t, []string{"Movies", "MoviesArchive", "Music"}, got,
			"namespaced caller must only see own-namespace classes in schema dump, stripped to short names")
	})

	t.Run("global operator sees both namespaces in schema dump by qualified name", func(t *testing.T) {
		resp, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams(), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		got := classNames(resp.Payload.Classes)
		assert.ElementsMatch(t, []string{
			ns1 + ":Movies", ns1 + ":MoviesArchive", ns1 + ":Music",
			ns2 + ":Movies", ns2 + ":Music",
		}, got)
	})

	t.Run("alias list filters to own namespace for namespaced caller", func(t *testing.T) {
		// Each namespace gets the same short alias name pointing at its own
		// Movies class. RBAC scopes the list to entries the caller can READ;
		// stripping returns short forms to namespaced callers.
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Films", Class: "Movies"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, ns1+":Films", helper.CreateAuth(adminKey))
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Films", Class: "Movies"}, user2Key)
		defer helper.DeleteAliasWithAuthz(t, ns2+":Films", helper.CreateAuth(adminKey))

		const role = "read-aliases-all"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithCollection("*").WithAlias("*").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		resp, err := helper.GetAliasesAuthWithReturn(t, nil, user1Key)
		require.NoError(t, err)
		seenAlias := map[string]string{}
		for _, a := range resp.Payload.Aliases {
			seenAlias[a.Alias] = a.Class
		}
		assert.Equal(t, "Movies", seenAlias["Films"], "namespaced caller sees own alias in short form")
		assert.NotContains(t, seenAlias, ns2+":Films", "namespaced caller must not see foreign-namespace alias")
	})

	t.Run("global operator sees both namespaces in alias list by qualified name", func(t *testing.T) {
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Songs", Class: "Music"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, ns1+":Songs", helper.CreateAuth(adminKey))
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Songs", Class: "Music"}, user2Key)
		defer helper.DeleteAliasWithAuthz(t, ns2+":Songs", helper.CreateAuth(adminKey))

		resp, err := helper.GetAliasesAuthWithReturn(t, nil, adminKey)
		require.NoError(t, err)
		seenAlias := map[string]string{}
		for _, a := range resp.Payload.Aliases {
			seenAlias[a.Alias] = a.Class
		}
		assert.Equal(t, ns1+":Music", seenAlias[ns1+":Songs"], "admin sees ns1 alias raw")
		assert.Equal(t, ns2+":Music", seenAlias[ns2+":Songs"], "admin sees ns2 alias raw")
	})

	t.Run("own-info strips username and lets wildcard permission resources pass through", func(t *testing.T) {
		// Role creation rejects qualified resource paths for now
		const role = "own-info-strip"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(),
				helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithCollection("*").WithAlias("*").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		info := helper.GetInfoForOwnUser(t, user1Key)
		require.NotNil(t, info.Username)
		assert.Equal(t, "u1", *info.Username, "namespaced caller's username must be stripped")

		// Find the assigned role on the principal and assert wildcard
		// permissions pass through unchanged (StripOwnNamespace is a no-op on `*`).
		var found *models.Role
		for _, r := range info.Roles {
			if r.Name != nil && *r.Name == role {
				found = r
				break
			}
		}
		require.NotNil(t, found, "role must be present on own-info response")
		require.NotEmpty(t, found.Permissions)
		sawCollectionsStar := false
		sawAliasesStar := false
		for _, p := range found.Permissions {
			if p.Collections != nil && p.Collections.Collection != nil && *p.Collections.Collection == "*" {
				sawCollectionsStar = true
			}
			if p.Aliases != nil && p.Aliases.Collection != nil && *p.Aliases.Collection == "*" &&
				p.Aliases.Alias != nil && *p.Aliases.Alias == "*" {
				sawAliasesStar = true
			}
		}
		assert.True(t, sawCollectionsStar, "wildcard collection must pass through unchanged")
		assert.True(t, sawAliasesStar, "wildcard alias must pass through unchanged")
	})

	t.Run("own-info: global admin sees raw username", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, adminKey)
		require.NotNil(t, info.Username)
		assert.Equal(t, adminUser, *info.Username)
	})

	t.Run("role with namespace-qualified resource path is rejected at create time", func(t *testing.T) {
		const role = "qualified-path-role"
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: String(role),
				Permissions: []*models.Permission{
					helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(ns1 + ":Movies").Permission(),
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err, "create role must reject namespace-qualified collection name")
	})
}

func classNames(classes []*models.Class) []string {
	out := make([]string, len(classes))
	for i, c := range classes {
		out[i] = c.Class
	}
	return out
}

func authzNamespaceNames(list []*models.Namespace) []string {
	out := make([]string, len(list))
	for i, ns := range list {
		out[i] = ns.Name
	}
	return out
}
