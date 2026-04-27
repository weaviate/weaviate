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

	"github.com/weaviate/weaviate/client/namespaces"
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
		map[string]string{
			"NAMESPACES_ENABLED": "true",
			"DISABLE_GRAPHQL":    "true",
		},
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
}

func authzNamespaceNames(list []*models.Namespace) []string {
	out := make([]string, len(list))
	for i, ns := range list {
		out[i] = ns.Name
	}
	return out
}
