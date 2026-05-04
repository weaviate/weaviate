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
	viewerUser := "viewer-user"
	viewerKey := "viewer-key"
	noPermsUser := "no-perms-user"
	noPermsKey := "no-perms-key"

	_, down := composeUpWithSettings(t,
		map[string]string{adminUser: adminKey},
		map[string]string{
			manageUser:       manageKey,
			scopedManageUser: scopedManageKey,
			noPermsUser:      noPermsKey,
		},
		map[string]string{viewerUser: viewerKey},
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

	t.Run("viewer role can read but not write namespaces", func(t *testing.T) {
		const nsName = "viewervisible"
		helper.CreateNamespace(t, nsName, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, nsName, adminKey) })

		helper.AssignRoleToUser(t, adminKey, "viewer", viewerUser)
		t.Cleanup(func() { helper.RevokeRoleFromUser(t, adminKey, "viewer", viewerUser) })

		// Reads succeed.
		assert.Equal(t, nsName, helper.GetNamespace(t, nsName, viewerKey).Name)
		assert.Contains(t, authzNamespaceNames(helper.ListNamespaces(t, viewerKey)), nsName)

		// Writes are forbidden.
		_, err := helper.Client(t).Namespaces.CreateNamespace(
			namespaces.NewCreateNamespaceParams().WithNamespaceID("viewerattempt"),
			helper.CreateAuth(viewerKey),
		)
		require.Error(t, err)
		var createForbidden *namespaces.CreateNamespaceForbidden
		require.True(t, errors.As(err, &createForbidden), "expected CreateNamespaceForbidden, got %T: %v", err, err)

		_, err = helper.Client(t).Namespaces.DeleteNamespace(
			namespaces.NewDeleteNamespaceParams().WithNamespaceID(nsName),
			helper.CreateAuth(viewerKey),
		)
		require.Error(t, err)
		var delForbidden *namespaces.DeleteNamespaceForbidden
		require.True(t, errors.As(err, &delForbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)
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

	// Namespace-scoping subtests (WS5 matcher + role-safety guards). Reuses
	// the same compose to avoid a second cluster boot.
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
	// namespace-relative create_collections role; the matcher specializes the
	// unqualified `*` template per caller, so each user can only create within
	// their own namespace.
	const bootstrapRole = "ns-bootstrap-create"
	helper.CreateRole(t, adminKey, &models.Role{
		Name: String(bootstrapRole),
		Permissions: []*models.Permission{
			helper.NewCollectionsPermission().WithAction(authorization.CreateCollections).WithCollection("*").Permission(),
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

		assert.Equal(t, ns1+":Movies", helper.GetClassAuth(t, "Movies", user1Key).Class)
	})

	t.Run("read_collections regex scope (Movies*): matches in own namespace only", func(t *testing.T) {
		const role = "read-movies-prefix"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(role),
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("Movies.*").Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, role, ns1+":u1")
		defer helper.RevokeRoleFromUser(t, adminKey, role, ns1+":u1")

		assert.Equal(t, ns1+":Movies", helper.GetClassAuth(t, "Movies", user1Key).Class)
		assert.Equal(t, ns1+":MoviesArchive", helper.GetClassAuth(t, "MoviesArchive", user1Key).Class)

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

		assert.Equal(t, ns1+":Movies", helper.GetClassAuth(t, "Movies", user1Key).Class)

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
		assert.ElementsMatch(t, []string{ns1 + ":Movies", ns1 + ":MoviesArchive", ns1 + ":Music"}, got,
			"namespaced caller must only see own-namespace classes in schema dump")
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

	t.Run("built-in admin role cannot be assigned to namespaced DB user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(ns1+":u1").WithBody(authz.AssignRoleToUserBody{
				Roles:    []string{authorization.Admin},
				UserType: models.UserTypeInputDb,
			}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToUserForbidden, got %T: %v", err, err)
	})

	t.Run("built-in viewer role cannot be assigned to namespaced DB user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(ns1+":u1").WithBody(authz.AssignRoleToUserBody{
				Roles:    []string{authorization.Viewer},
				UserType: models.UserTypeInputDb,
			}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToUserForbidden, got %T: %v", err, err)
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
