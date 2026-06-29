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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// readCollectionsRole is a role with a single collection-read permission on the
// given collection (defaults to the own-namespace wildcard).
func readCollectionsRole(name, collection string) *models.Role {
	return &models.Role{
		Name: authorization.String(name),
		Permissions: []*models.Permission{
			helper.NewCollectionsPermission().
				WithAction(authorization.ReadCollections).
				WithCollection(collection).
				Permission(),
		},
	}
}

// findDataCollection returns the collection of the role's data permission, or
// "" when the role has none.
func findDataCollection(role *models.Role) string {
	for _, p := range role.Permissions {
		if p.Data != nil && p.Data.Collection != nil {
			return *p.Data.Collection
		}
	}
	return ""
}

// collectionReadPerm builds a collection-read permission for add/remove/has-permission bodies.
func collectionReadPerm(collection string) []*models.Permission {
	return []*models.Permission{
		helper.NewCollectionsPermission().
			WithAction(authorization.ReadCollections).
			WithCollection(collection).
			Permission(),
	}
}

// roleNames extracts the names from a role list.
func roleNames(roles []*models.Role) []string {
	out := make([]string, 0, len(roles))
	for _, r := range roles {
		out = append(out, *r.Name)
	}
	return out
}

// usersForRole returns the user ids the role lists for the given caller.
func usersForRole(t *testing.T, key, roleID string) []string {
	t.Helper()
	resp, err := helper.Client(t).Authz.GetUsersForRole(
		authz.NewGetUsersForRoleParams().WithID(roleID), helper.CreateAuth(key))
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.Payload))
	for _, u := range resp.Payload {
		ids = append(ids, u.UserID)
	}
	return ids
}

// TestNamespaceLocalRoles exercises the user-facing contract for
// namespace-local roles against a real multi-namespace cluster: names and
// permissions auto-prefix on write and strip on read, short names are unique
// per namespace with a global reservation, cross-namespace references are
// rejected, and content-scope visibility hides roles beyond the caller's permissions.
func TestNamespaceLocalRoles(t *testing.T) {
	t.Parallel()
	ns1, ns2, u1Key, u2Key := twoNamespaces(t)

	t.Run("local role auto-prefixes; caller sees short form, operator sees qualified", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("editor", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":editor") })

		// The namespaced caller sees the short name and an unqualified wildcard.
		own := helper.GetRoleByName(t, u1Key, "editor")
		require.Equal(t, "editor", *own.Name)
		require.Equal(t, "*", *own.Permissions[0].Collections.Collection)

		// The operator sees the stored, namespace-qualified form.
		stored := helper.GetRoleByName(t, adminKey, ns1+":editor")
		require.Equal(t, ns1+":editor", *stored.Name)
		require.Equal(t, ns1+":*", *stored.Permissions[0].Collections.Collection)
	})

	t.Run("a namespaced admin deletes its own local role", func(t *testing.T) {
		// The caller's own matcher-scoped delete is the assertion, so no operator
		// cleanup is registered.
		helper.CreateRole(t, u1Key, readCollectionsRole("disposable", "*"))
		_, err := helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID("disposable"), helper.CreateAuth(u1Key))
		require.NoError(t, err)

		// It is gone in the operator's qualified view too.
		_, getErr := helper.Client(t).Authz.GetRole(
			authz.NewGetRoleParams().WithID(ns1+":disposable"), helper.CreateAuth(adminKey))
		var notFound *authz.GetRoleNotFound
		require.True(t, errors.As(getErr, &notFound), "expected GetRoleNotFound, got %T: %v", getErr, getErr)
	})

	t.Run("same short name in two namespaces stores separate permissions", func(t *testing.T) {
		// ns1's "dup" is a schema-read role; ns2's "dup" is a data-read role on a
		// specific collection. Same short name, different content per namespace.
		helper.CreateRole(t, u1Key, readCollectionsRole("dup", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":dup") })
		helper.CreateRole(t, u2Key, &models.Role{
			Name: authorization.String("dup"),
			Permissions: []*models.Permission{
				helper.NewDataPermission().
					WithAction(authorization.ReadData).
					WithCollection("Widgets").
					Permission(),
			},
		})
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns2+":dup") })

		// Each caller reads back its own "dup" with its own permission, stripped.
		own1 := helper.GetRoleByName(t, u1Key, "dup")
		require.Equal(t, authorization.ReadCollections, *own1.Permissions[0].Action)
		require.NotNil(t, own1.Permissions[0].Collections)
		require.Equal(t, "*", *own1.Permissions[0].Collections.Collection)
		require.Nil(t, own1.Permissions[0].Data)

		own2 := helper.GetRoleByName(t, u2Key, "dup")
		require.Equal(t, authorization.ReadData, *own2.Permissions[0].Action)
		require.NotNil(t, own2.Permissions[0].Data)
		require.Equal(t, "Widgets", *own2.Permissions[0].Data.Collection)
		require.Nil(t, own2.Permissions[0].Collections)
	})

	t.Run("a global short name is reserved by an existing local role", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("reserved", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":reserved") })

		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(readCollectionsRole("reserved", "*")),
			helper.CreateAuth(adminKey))
		var conflict *authz.CreateRoleConflict
		require.True(t, errors.As(err, &conflict), "expected CreateRoleConflict, got %T: %v", err, err)
	})

	t.Run("a cross-namespace reference is rejected", func(t *testing.T) {
		// A namespaced caller may not name another namespace; its own segments
		// are auto-prefixed, so an explicit prefix is invalid.
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(readCollectionsRole("crossref", ns2+":Foo")),
			helper.CreateAuth(u1Key))
		var unproc *authz.CreateRoleUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateRoleUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("content-scope visibility hides root, read-only, and other namespaces", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("visible", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":visible") })
		helper.CreateRole(t, u2Key, readCollectionsRole("hidden", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns2+":hidden") })

		names := map[string]bool{}
		for _, r := range helper.GetRoles(t, u1Key) {
			names[*r.Name] = true
		}
		assert.True(t, names[authorization.Admin], "namespaced admin should see admin")
		assert.True(t, names[authorization.Viewer], "namespaced admin should see viewer")
		assert.True(t, names["visible"], "namespaced admin should see its own role (stripped)")
		assert.False(t, names[authorization.Root], "root must stay hidden")
		assert.False(t, names[authorization.ReadOnly], "read-only must stay hidden")
		assert.False(t, names["hidden"], "another namespace's role must stay hidden")
		assert.False(t, names[ns2+":hidden"], "another namespace's role must stay hidden (qualified)")
	})

	t.Run("a foreign-namespace role cannot be assigned across namespaces; own and global roles stay visible", func(t *testing.T) {
		// A namespace-local role belongs to exactly one namespace and may not be
		// assigned to a user in another namespace — not even by an operator.
		// Own-namespace and global roles assigned legitimately stay visible.
		helper.CreateRole(t, u2Key, readCollectionsRole("probe", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns2+":probe") })
		helper.CreateRole(t, u1Key, readCollectionsRole("mine", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":mine") })

		// A fresh ns1 target user (not the caller, so the read path authorizes).
		createNamespacedUser(t, "bob", ns1, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns1+":bob", adminKey) })

		helper.AssignRoleToUser(t, u1Key, "mine", "bob")                       // own-namespace role
		helper.AssignRoleToUser(t, adminKey, authorization.Viewer, ns1+":bob") // global role

		// The operator cannot assign ns2's local role to a ns1 user.
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(ns1+":bob").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{ns2 + ":probe"}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToUserForbidden, got %T: %v", err, err)

		// The ns1 admin lists bob's roles: own + global visible, the foreign role
		// never made it in.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, includeFull := range []bool{false, true} {
				full := includeFull
				resp, err := helper.Client(t).Authz.GetRolesForUser(
					authz.NewGetRolesForUserParams().WithID("bob").
						WithUserType(string(models.UserTypeInputDb)).WithIncludeFullRoles(&full),
					helper.CreateAuth(u1Key))
				if !assert.NoError(c, err) {
					return
				}
				names := map[string]bool{}
				for _, r := range resp.Payload {
					names[*r.Name] = true
				}
				assert.False(c, names[ns2+":probe"], "foreign-namespace role present (includeFull=%v)", full)
				assert.False(c, names["probe"], "foreign-namespace role present under its short name (includeFull=%v)", full)
				assert.True(c, names["mine"], "own-namespace role missing (includeFull=%v)", full)
				assert.True(c, names[authorization.Viewer], "global role missing (includeFull=%v)", full)
			}
		}, 15*time.Second, 100*time.Millisecond, "roles-for-user visibility did not converge")
	})

	t.Run("a namespaced caller cannot read or assign a foreign-namespace role", func(t *testing.T) {
		// ns2 owns the role; ns1's caller must not reach it under any name form.
		helper.CreateRole(t, u2Key, readCollectionsRole("foreignprobe", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns2+":foreignprobe") })

		// By short name the caller's own namespace is searched, so ns2's role 404s.
		_, getShortErr := helper.Client(t).Authz.GetRole(
			authz.NewGetRoleParams().WithID("foreignprobe"), helper.CreateAuth(u1Key))
		var getNotFound *authz.GetRoleNotFound
		require.True(t, errors.As(getShortErr, &getNotFound), "expected GetRoleNotFound, got %T: %v", getShortErr, getShortErr)

		// Naming the foreign namespace explicitly is an invalid name for a confined caller.
		_, getQualErr := helper.Client(t).Authz.GetRole(
			authz.NewGetRoleParams().WithID(ns2+":foreignprobe"), helper.CreateAuth(u1Key))
		var getBadReq *authz.GetRoleBadRequest
		require.True(t, errors.As(getQualErr, &getBadReq), "expected GetRoleBadRequest, got %T: %v", getQualErr, getQualErr)

		// Assigning it to the caller's own user fails identically: the short name
		// resolves to a missing own-namespace role (404), the qualified name is a
		// bad request.
		_, asgShortErr := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID("u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{"foreignprobe"}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(u1Key))
		var asgNotFound *authz.AssignRoleToUserNotFound
		require.True(t, errors.As(asgShortErr, &asgNotFound), "expected AssignRoleToUserNotFound, got %T: %v", asgShortErr, asgShortErr)

		_, asgQualErr := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID("u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{ns2 + ":foreignprobe"}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(u1Key))
		var asgBadReq *authz.AssignRoleToUserBadRequest
		require.True(t, errors.As(asgQualErr, &asgBadReq), "expected AssignRoleToUserBadRequest, got %T: %v", asgQualErr, asgQualErr)
	})

	t.Run("assign requires an explicit userType", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("asg", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":asg") })

		// No userType in the body is rejected before the {db,oidc} fan-out.
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID("u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{"asg"}}),
			helper.CreateAuth(u1Key))
		var badReq *authz.AssignRoleToUserBadRequest
		require.True(t, errors.As(err, &badReq), "expected AssignRoleToUserBadRequest, got %T: %v", err, err)

		// With userType it succeeds.
		helper.AssignRoleToUser(t, u1Key, "asg", "u1")
	})

	t.Run("assigning a role beyond the caller's permissions is denied", func(t *testing.T) {
		// A global role the narrowed namespaced admin does not hold.
		broad := uniqueRole()
		helper.CreateRole(t, adminKey, &models.Role{
			Name: authorization.String(broad),
			Permissions: []*models.Permission{
				helper.NewBackupPermission().
					WithAction(authorization.ManageBackups).
					WithCollection("*").
					Permission(),
			},
		})
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, broad) })

		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID("u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{broad}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(u1Key))
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToUserForbidden, got %T: %v", err, err)
	})

	t.Run("a namespaced viewer cannot see the admin role", func(t *testing.T) {
		viewerKey := helper.CreateUserWithNamespace(t, "vw", ns1, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns1+":vw", adminKey) })
		helper.AssignRoleToUser(t, adminKey, authorization.Viewer, ns1+":vw")
		helper.WaitForOwnRole(t, viewerKey, authorization.Viewer)

		// admin's write permissions exceed viewer's, so it is not visible to it.
		_, err := helper.Client(t).Authz.GetRole(
			authz.NewGetRoleParams().WithID(authorization.Admin), helper.CreateAuth(viewerKey))
		var forbidden *authz.GetRoleForbidden
		require.True(t, errors.As(err, &forbidden), "expected GetRoleForbidden, got %T: %v", err, err)
	})

	t.Run("creating an ALL-scoped roles permission is denied", func(t *testing.T) {
		scope := models.PermissionRolesScopeAll
		role := &models.Role{
			Name: authorization.String("superroles"),
			Permissions: []*models.Permission{{
				Action: authorization.String(authorization.ReadRoles),
				Roles:  &models.PermissionRoles{Role: authorization.All, Scope: &scope},
			}},
		}
		// Cluster-wide role management can't be stored in a role on an NS
		// cluster, not even by the operator.
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(role), helper.CreateAuth(adminKey))
		var forbidden *authz.CreateRoleForbidden
		require.True(t, errors.As(err, &forbidden), "expected CreateRoleForbidden, got %T: %v", err, err)
	})

	t.Run("a namespaced caller is also denied an ALL-scoped roles permission", func(t *testing.T) {
		scope := models.PermissionRolesScopeAll
		role := &models.Role{
			Name: authorization.String("nssuperroles"),
			Permissions: []*models.Permission{{
				Action: authorization.String(authorization.ReadRoles),
				Roles:  &models.PermissionRoles{Role: authorization.All, Scope: &scope},
			}},
		}
		// The ALL-scope gate fires before the content-scope check, so a confined
		// caller is denied the same way the operator is.
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(role), helper.CreateAuth(u1Key))
		var forbidden *authz.CreateRoleForbidden
		require.True(t, errors.As(err, &forbidden), "expected CreateRoleForbidden, got %T: %v", err, err)
	})

	t.Run("the root role cannot be assigned to a namespaced target", func(t *testing.T) {
		// root is the only role carrying ALL-scoped role management; assigning it
		// to a namespaced user would escalate them to cluster-wide root. The
		// operator itself is denied — the closed end of the create-side guard.
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(ns1+":u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{authorization.Root}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToUserForbidden, got %T: %v", err, err)
	})

	t.Run("a namespaced role's permission is enforced", func(t *testing.T) {
		setupClassInNs1(t, ns1, "Docs", u1Key)
		helper.CreateRole(t, u1Key, &models.Role{
			Name: authorization.String("docreader"),
			Permissions: []*models.Permission{
				helper.NewDataPermission().
					WithAction(authorization.ReadData).
					WithCollection("Docs").
					Permission(),
			},
		})
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":docreader") })

		// A fresh user holding only the read-data role.
		readerKey := helper.CreateUserWithNamespace(t, "reader1", ns1, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns1+":reader1", adminKey) })
		helper.AssignRoleToUser(t, u1Key, "docreader", "reader1")

		// Poll the positive capability: once reads succeed the role is applied
		// and the new key recognized (past apikey + binding replication lag).
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err := helper.ListObjectsAuth(t, "Docs", readerKey)
			assert.NoError(c, err)
		}, 15*time.Second, 100*time.Millisecond, "read_data role never became effective for the new user")

		// read_data grants reads, so a write (create object) is denied.
		err := helper.CreateObjectAuth(t, &models.Object{
			Class:      "Docs",
			Properties: map[string]any{"title": "nope"},
		}, readerKey)
		require.Error(t, err)
		var forbidden *objects.ObjectsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected ObjectsCreateForbidden, got %T: %v", err, err)
	})

	t.Run("revoke requires an explicit userType and removes the binding", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("rev", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":rev") })
		helper.AssignRoleToUser(t, u1Key, "rev", "u1")

		// No userType in the body is rejected before the {db,oidc} fan-out.
		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID("u1").
				WithBody(authz.RevokeRoleFromUserBody{Roles: []string{"rev"}}),
			helper.CreateAuth(u1Key))
		var badReq *authz.RevokeRoleFromUserBadRequest
		require.True(t, errors.As(err, &badReq), "expected RevokeRoleFromUserBadRequest, got %T: %v", err, err)

		// With userType the binding is removed.
		helper.RevokeRoleFromUser(t, u1Key, "rev", "u1")
		includeFull := false
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := helper.Client(t).Authz.GetRolesForUser(
				authz.NewGetRolesForUserParams().WithID("u1").
					WithUserType(string(models.UserTypeInputDb)).WithIncludeFullRoles(&includeFull),
				helper.CreateAuth(u1Key))
			if !assert.NoError(c, err) {
				return
			}
			for _, r := range resp.Payload {
				assert.NotEqual(c, "rev", *r.Name, "revoked role still bound")
			}
		}, 10*time.Second, 100*time.Millisecond, "revoke did not remove the binding")
	})

	t.Run("revoking a namespaced role removes the effective capability", func(t *testing.T) {
		// Binding removal in the role list is not the same as effective denial:
		// prove the previously granted read actually stops working after revoke.
		setupClassInNs1(t, ns1, "Revdocs", u1Key)
		helper.CreateRole(t, u1Key, &models.Role{
			Name: authorization.String("revreader"),
			Permissions: []*models.Permission{
				helper.NewDataPermission().
					WithAction(authorization.ReadData).
					WithCollection("Revdocs").
					Permission(),
			},
		})
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":revreader") })

		// A fresh user holding only the read-data role.
		readerKey := helper.CreateUserWithNamespace(t, "revreader1", ns1, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns1+":revreader1", adminKey) })
		helper.AssignRoleToUser(t, u1Key, "revreader", "revreader1")

		// The capability becomes effective (past apikey + binding replication lag).
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err := helper.ListObjectsAuth(t, "Revdocs", readerKey)
			assert.NoError(c, err)
		}, 15*time.Second, 100*time.Millisecond, "read_data role never became effective for the new user")

		// After revoke the read must start failing with a forbidden.
		helper.RevokeRoleFromUser(t, u1Key, "revreader", "revreader1")
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err := helper.ListObjectsAuth(t, "Revdocs", readerKey)
			var forbidden *objects.ObjectsListForbidden
			assert.True(c, errors.As(err, &forbidden), "expected ObjectsListForbidden, got %T: %v", err, err)
		}, 15*time.Second, 100*time.Millisecond, "revoke did not remove the effective read capability")
	})

	t.Run("an operator cannot revoke a namespace-local role", func(t *testing.T) {
		// A local role is managed entirely within its namespace, so an operator
		// cannot revoke it.
		helper.CreateRole(t, u1Key, readCollectionsRole("revmine", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":revmine") })

		createNamespacedUser(t, "carol", ns1, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns1+":carol", adminKey) })

		helper.AssignRoleToUser(t, u1Key, "revmine", "carol")

		// The operator addresses the local role by its qualified name and is denied.
		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID(ns1+":carol").
				WithBody(authz.RevokeRoleFromUserBody{Roles: []string{ns1 + ":revmine"}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.RevokeRoleFromUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected RevokeRoleFromUserForbidden, got %T: %v", err, err)

		// The binding survived the denied revoke; the ns1 admin can still revoke it.
		helper.RevokeRoleFromUser(t, u1Key, "revmine", "carol")
	})

	t.Run("a namespaced admin cannot modify a role outside its namespace", func(t *testing.T) {
		// An operator-created global custom role; the matcher confines the
		// namespaced admin to its own namespace's roles.
		globalRole := uniqueRole()
		helper.CreateRole(t, adminKey, readCollectionsRole(globalRole, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, globalRole) })

		_, delErr := helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID(globalRole), helper.CreateAuth(u1Key))
		var delForbidden *authz.DeleteRoleForbidden
		require.True(t, errors.As(delErr, &delForbidden), "expected DeleteRoleForbidden, got %T: %v", delErr, delErr)

		extra := []*models.Permission{
			helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(),
		}
		_, addErr := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(globalRole).WithBody(authz.AddPermissionsBody{Permissions: extra}),
			helper.CreateAuth(u1Key))
		var addForbidden *authz.AddPermissionsForbidden
		require.True(t, errors.As(addErr, &addForbidden), "expected AddPermissionsForbidden, got %T: %v", addErr, addErr)

		_, remErr := helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID(globalRole).WithBody(authz.RemovePermissionsBody{Permissions: extra}),
			helper.CreateAuth(u1Key))
		var remForbidden *authz.RemovePermissionsForbidden
		require.True(t, errors.As(remErr, &remForbidden), "expected RemovePermissionsForbidden, got %T: %v", remErr, remErr)
	})

	t.Run("an operator cannot modify a namespace-local role's permissions", func(t *testing.T) {
		// A local role is managed entirely within its namespace; even the operator
		// is denied, so its bare resources can't be stored unqualified in the role.
		helper.CreateRole(t, u1Key, readCollectionsRole("opedit", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":opedit") })

		extra := []*models.Permission{
			helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("Movies").Permission(),
		}
		_, addErr := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(ns1+":opedit").WithBody(authz.AddPermissionsBody{Permissions: extra}),
			helper.CreateAuth(adminKey))
		var addForbidden *authz.AddPermissionsForbidden
		require.True(t, errors.As(addErr, &addForbidden), "expected AddPermissionsForbidden, got %T: %v", addErr, addErr)

		_, remErr := helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID(ns1+":opedit").WithBody(authz.RemovePermissionsBody{Permissions: extra}),
			helper.CreateAuth(adminKey))
		var remForbidden *authz.RemovePermissionsForbidden
		require.True(t, errors.As(remErr, &remForbidden), "expected RemovePermissionsForbidden, got %T: %v", remErr, remErr)
	})

	t.Run("a namespaced admin cannot assign roles to groups", func(t *testing.T) {
		// Group assignment is hard-denied for namespaced principals.
		_, err := helper.Client(t).Authz.AssignRoleToGroup(
			authz.NewAssignRoleToGroupParams().WithID("engineers").
				WithBody(authz.AssignRoleToGroupBody{Roles: []string{authorization.Viewer}, GroupType: models.GroupTypeOidc}),
			helper.CreateAuth(u1Key))
		var forbidden *authz.AssignRoleToGroupForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToGroupForbidden, got %T: %v", err, err)
	})

	t.Run("a namespaced admin cannot revoke roles from groups", func(t *testing.T) {
		// Group revocation is hard-denied for namespaced principals.
		_, err := helper.Client(t).Authz.RevokeRoleFromGroup(
			authz.NewRevokeRoleFromGroupParams().WithID("engineers").
				WithBody(authz.RevokeRoleFromGroupBody{Roles: []string{authorization.Viewer}, GroupType: models.GroupTypeOidc}),
			helper.CreateAuth(u1Key))
		var forbidden *authz.RevokeRoleFromGroupForbidden
		require.True(t, errors.As(err, &forbidden), "expected RevokeRoleFromGroupForbidden, got %T: %v", err, err)
	})

	t.Run("an operator cannot revoke a namespace-local role from a group", func(t *testing.T) {
		// Groups are global; a namespace-local role can never be on one, so an
		// operator naming one by its qualified form is denied.
		helper.CreateRole(t, u1Key, readCollectionsRole("grouplocal", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":grouplocal") })

		_, err := helper.Client(t).Authz.RevokeRoleFromGroup(
			authz.NewRevokeRoleFromGroupParams().WithID("engineers").
				WithBody(authz.RevokeRoleFromGroupBody{Roles: []string{ns1 + ":grouplocal"}, GroupType: models.GroupTypeOidc}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.RevokeRoleFromGroupForbidden
		require.True(t, errors.As(err, &forbidden), "expected RevokeRoleFromGroupForbidden, got %T: %v", err, err)
	})

	t.Run("hasPermission rejects a namespace-qualified permission body", func(t *testing.T) {
		// A namespaced caller must submit bare resource paths; an explicit prefix
		// (even its own) is rejected, matching create/add/remove.
		helper.CreateRole(t, u1Key, readCollectionsRole("permcheck", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":permcheck") })

		perm := helper.NewCollectionsPermission().
			WithAction(authorization.ReadCollections).
			WithCollection(ns1 + ":Foo").
			Permission()
		_, err := helper.Client(t).Authz.HasPermission(
			authz.NewHasPermissionParams().WithID("permcheck").WithBody(perm),
			helper.CreateAuth(u1Key))
		var badReq *authz.HasPermissionBadRequest
		require.True(t, errors.As(err, &badReq), "expected HasPermissionBadRequest, got %T: %v", err, err)
	})

	t.Run("creating a role beyond the caller's permissions is denied", func(t *testing.T) {
		// A narrowed namespaced admin lacks manage_backups, so it cannot author
		// it into a role (the caller must already hold each permission on create).
		role := &models.Role{
			Name: authorization.String("toobroad"),
			Permissions: []*models.Permission{
				helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection("*").Permission(),
			},
		}
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(role), helper.CreateAuth(u1Key))
		var forbidden *authz.CreateRoleForbidden
		require.True(t, errors.As(err, &forbidden), "expected CreateRoleForbidden, got %T: %v", err, err)
	})

	t.Run("addPermissions on a local role auto-prefixes and strips on read", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("upd", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":upd") })

		dataPerm := []*models.Permission{
			helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection("Notes").Permission(),
		}
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID("upd").WithBody(authz.AddPermissionsBody{Permissions: dataPerm}),
			helper.CreateAuth(u1Key))
		require.NoError(t, err)

		// Caller sees the added permission stripped; operator sees it qualified.
		require.Equal(t, "Notes", findDataCollection(helper.GetRoleByName(t, u1Key, "upd")))
		require.Equal(t, ns1+":Notes", findDataCollection(helper.GetRoleByName(t, adminKey, ns1+":upd")))

		// removePermissions drops it again.
		_, err = helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID("upd").WithBody(authz.RemovePermissionsBody{Permissions: dataPerm}),
			helper.CreateAuth(u1Key))
		require.NoError(t, err)
		require.Equal(t, "", findDataCollection(helper.GetRoleByName(t, u1Key, "upd")))
	})

	t.Run("a global short name blocks a local create (reverse reservation)", func(t *testing.T) {
		globalOnly := uniqueRole()
		helper.CreateRole(t, adminKey, readCollectionsRole(globalOnly, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, globalOnly) })

		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(readCollectionsRole(globalOnly, "*")),
			helper.CreateAuth(u1Key))
		var conflict *authz.CreateRoleConflict
		require.True(t, errors.As(err, &conflict), "expected CreateRoleConflict, got %T: %v", err, err)
	})

	t.Run("a namespaced caller's error strips its prefix while the operator stays raw", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("duperr", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":duperr") })

		// The namespaced caller's conflict names the short role, not ns1:duperr.
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(readCollectionsRole("duperr", "*")),
			helper.CreateAuth(u1Key))
		var conflict *authz.CreateRoleConflict
		require.True(t, errors.As(err, &conflict), "got %T: %v", err, err)
		require.NotEmpty(t, conflict.Payload.Error)
		msg := conflict.Payload.Error[0].Message
		assert.NotContains(t, msg, ns1+":", "namespaced error must not leak the caller's prefix")
		assert.Contains(t, msg, "duperr")

		// The operator sees the same role's qualified name raw (never stripped).
		require.Equal(t, ns1+":duperr", *helper.GetRoleByName(t, adminKey, ns1+":duperr").Name)
	})

	t.Run("GET /users/me strips the namespace prefix from a bound local role", func(t *testing.T) {
		// Bind a local role (stored as ns1:selfrole) so the strip is actually
		// exercised; the built-in admin u1 already holds is unprefixed regardless.
		helper.CreateRole(t, u1Key, readCollectionsRole("selfrole", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":selfrole") })
		helper.AssignRoleToUser(t, u1Key, "selfrole", "u1")
		helper.WaitForOwnRole(t, u1Key, "selfrole")

		info := helper.GetInfoForOwnUser(t, u1Key)
		require.NotEmpty(t, info.Roles)
		var found bool
		for _, r := range info.Roles {
			assert.NotContains(t, *r.Name, ":", "own-info role names must not carry a namespace prefix")
			if *r.Name == "selfrole" {
				found = true
			}
		}
		assert.True(t, found, "me must list the bound local role under its stripped short name")
	})
}

// TestNamespaceHasPermissionGlobalFallbackRole checks hasPermission against an
// inherited global role unqualified, end-to-end through real casbin matching.
func TestNamespaceHasPermissionGlobalFallbackRole(t *testing.T) {
	t.Parallel()
	ns1, _, u1Key, _ := twoNamespaces(t)

	// A global role with a concrete unqualified permission, inherited by the ns1 user.
	gRole := uniqueRole()
	helper.CreateRole(t, adminKey, readCollectionsRole(gRole, "Movies"))
	t.Cleanup(func() { helper.DeleteRole(t, adminKey, gRole) })
	helper.AssignRoleToUser(t, adminKey, gRole, ns1+":u1")
	helper.WaitForOwnRole(t, u1Key, gRole)

	// The bare "Movies" permission must match the global role's unqualified row,
	// not be qualified to "ns1:Movies" (which could never match it).
	perm := helper.NewCollectionsPermission().
		WithAction(authorization.ReadCollections).
		WithCollection("Movies").
		Permission()
	resp, err := helper.Client(t).Authz.HasPermission(
		authz.NewHasPermissionParams().WithID(gRole).WithBody(perm),
		helper.CreateAuth(u1Key))
	require.NoError(t, err)
	require.True(t, resp.Payload, "namespaced caller must see the global role's unqualified permission")
}

// TestNamespaceHasPermissionOperatorOnLocalRole checks a global operator's
// hasPermission verdict against a namespace-local role. The operator names the
// role's namespace explicitly (ns1:Movies); that qualified resource is compared
// as submitted against the role's qualified stored rows.
func TestNamespaceHasPermissionOperatorOnLocalRole(t *testing.T) {
	t.Parallel()
	ns1, _, u1Key, _ := twoNamespaces(t)

	// A namespace-local role whose collection permission stores ns1:Movies.
	helper.CreateRole(t, u1Key, readCollectionsRole("opcheck", "Movies"))
	t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":opcheck") })

	// The operator submits the qualified collection it sees on the stored role.
	granted := helper.NewCollectionsPermission().
		WithAction(authorization.ReadCollections).
		WithCollection(ns1 + ":Movies").
		Permission()
	resp, err := helper.Client(t).Authz.HasPermission(
		authz.NewHasPermissionParams().WithID(ns1+":opcheck").WithBody(granted),
		helper.CreateAuth(adminKey))
	require.NoError(t, err)
	require.True(t, resp.Payload, "operator's qualified permission must match the role's stored row")

	// A different namespace's collection is not what the role grants.
	notGranted := helper.NewCollectionsPermission().
		WithAction(authorization.ReadCollections).
		WithCollection(ns1 + ":Other").
		Permission()
	respNeg, err := helper.Client(t).Authz.HasPermission(
		authz.NewHasPermissionParams().WithID(ns1+":opcheck").WithBody(notGranted),
		helper.CreateAuth(adminKey))
	require.NoError(t, err)
	require.False(t, respNeg.Payload, "a permission the role lacks must report false")
}

// TestNamespaceDeleteCascadesRBAC proves the namespace-delete cascade and the
// apply-time emptiness gate work together: a namespace holding a local role and
// a user with both a local and a global role assignment can still be deleted —
// the removal succeeds only because every RBAC row was cascaded away first (the
// gate would otherwise block it forever) — and the local role is gone after.
func TestNamespaceDeleteCascadesRBAC(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)
	uKey := createNamespacedUser(t, "casc", ns, adminKey)

	helper.CreateRole(t, uKey, readCollectionsRole("temp", "*"))
	helper.AssignRoleToUser(t, uKey, "temp", "casc")                       // local-role assignment
	helper.AssignRoleToUser(t, adminKey, authorization.Viewer, ns+":casc") // global-role assignment

	// Deleting waits for the namespace to 404; it can only reach that state if
	// the cascade removed the local role and every assignment first.
	helper.DeleteNamespace(t, ns, adminKey)

	for _, r := range helper.GetRoles(t, adminKey) {
		require.NotEqual(t, ns+":temp", *r.Name, "local role must be cascaded away on namespace delete")
	}

	// The global viewer assignment — whose subject is the deleted namespace's
	// user — must be cascaded too; assert the grouping row directly rather than
	// inferring it from the delete succeeding.
	resp, err := helper.Client(t).Authz.GetUsersForRole(
		authz.NewGetUsersForRoleParams().WithID(authorization.Viewer), helper.CreateAuth(adminKey))
	require.NoError(t, err)
	for _, u := range resp.Payload {
		require.NotEqual(t, ns+":casc", u.UserID, "global-role assignment row must be cascaded on namespace delete")
	}
}

// TestNamespaceOperatorReservedRoles exercises the operator-reserved role-name
// prefix end to end on a multi-namespace cluster: a namespaced admin can neither
// create, see, nor be assigned an operator_*/global_* role, while the operator
// can create, see, and assign one to a global user. A non-reserved global role
// stays delegatable to the namespaced admin (regression guard).
func TestNamespaceOperatorReservedRoles(t *testing.T) {
	t.Parallel()
	ns1, _, u1Key, _ := twoNamespaces(t)

	t.Run("namespaced admin cannot create a reserved-prefix role", func(t *testing.T) {
		_, err := helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(readCollectionsRole("operator_"+uniqueRole(), "*")),
			helper.CreateAuth(u1Key))
		var unproc *authz.CreateRoleUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateRoleUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("operator creates a reserved-prefix role hidden from the namespaced admin", func(t *testing.T) {
		name := "operator_" + uniqueRole()
		// Its single permission, held by the caller, would pass the content gate, so only
		// the prefix keeps it hidden.
		helper.CreateRole(t, adminKey, readCollectionsRole(name, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, name) })

		// The operator sees it.
		require.Equal(t, name, *helper.GetRoleByName(t, adminKey, name).Name)

		// The namespaced admin gets a 404 and never sees it listed.
		_, getErr := helper.Client(t).Authz.GetRole(
			authz.NewGetRoleParams().WithID(name), helper.CreateAuth(u1Key))
		var notFound *authz.GetRoleNotFound
		require.True(t, errors.As(getErr, &notFound), "expected GetRoleNotFound, got %T: %v", getErr, getErr)
		for _, r := range helper.GetRoles(t, u1Key) {
			require.NotEqual(t, name, *r.Name, "reserved role leaked into the namespaced admin's role list")
		}
	})

	t.Run("a reserved-prefix role cannot be assigned to a namespaced user", func(t *testing.T) {
		name := "operator_" + uniqueRole()
		helper.CreateRole(t, adminKey, readCollectionsRole(name, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, name) })

		// Target-based block: even the operator cannot pull it into a namespace.
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(ns1+":u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{name}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected AssignRoleToUserForbidden, got %T: %v", err, err)
	})

	t.Run("operator assigns a reserved-prefix role to a global user", func(t *testing.T) {
		name := "operator_" + uniqueRole()
		helper.CreateRole(t, adminKey, readCollectionsRole(name, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, name) })

		// gTarget is a global (namespace-less) user, so the target-based block
		// does not apply.
		helper.AssignRoleToUser(t, adminKey, name, gTarget)
		t.Cleanup(func() { helper.RevokeRoleFromUser(t, adminKey, name, gTarget) })
	})

	t.Run("a non-reserved global role stays delegatable to the namespaced admin", func(t *testing.T) {
		name := uniqueRole() // no reserved prefix
		helper.CreateRole(t, adminKey, readCollectionsRole(name, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, name) })

		// Visible to the namespaced admin and assignable to its own-namespace user.
		require.Equal(t, name, *helper.GetRoleByName(t, u1Key, name).Name)
		helper.AssignRoleToUser(t, adminKey, name, ns1+":u1")
		t.Cleanup(func() { helper.RevokeRoleFromUser(t, adminKey, name, ns1+":u1") })
	})
}

// TestNamespaceCrossNamespaceLocalRoleWrites pins that a namespaced caller cannot
// reach another namespace's local role through any write/delete: a bare short
// name resolves into the caller's own namespace (idempotent / not found), and a
// qualified foreign name is rejected outright. The modify-outside subtests above
// use a global role as the target; this exercises a foreign local role.
func TestNamespaceCrossNamespaceLocalRoleWrites(t *testing.T) {
	t.Parallel()
	_, ns2, u1Key, u2Key := twoNamespaces(t)

	helper.CreateRole(t, u2Key, readCollectionsRole("probe", "*"))
	t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns2+":probe") })

	t.Run("delete by short name is a no-op and leaves the foreign role intact", func(t *testing.T) {
		// "probe" resolves into ns1, which has no such role, so delete is idempotent.
		_, err := helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID("probe"), helper.CreateAuth(u1Key))
		require.NoError(t, err)
		require.Equal(t, ns2+":probe", *helper.GetRoleByName(t, adminKey, ns2+":probe").Name)
	})

	t.Run("delete by qualified foreign name is rejected", func(t *testing.T) {
		_, err := helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID(ns2+":probe"), helper.CreateAuth(u1Key))
		var badReq *authz.DeleteRoleBadRequest
		require.True(t, errors.As(err, &badReq), "got %T: %v", err, err)
	})

	t.Run("addPermissions by short name is not found", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID("probe").WithBody(authz.AddPermissionsBody{Permissions: collectionReadPerm("Movies")}),
			helper.CreateAuth(u1Key))
		var notFound *authz.AddPermissionsNotFound
		require.True(t, errors.As(err, &notFound), "got %T: %v", err, err)
	})

	t.Run("addPermissions by qualified foreign name is rejected", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(ns2+":probe").WithBody(authz.AddPermissionsBody{Permissions: collectionReadPerm("Movies")}),
			helper.CreateAuth(u1Key))
		var badReq *authz.AddPermissionsBadRequest
		require.True(t, errors.As(err, &badReq), "got %T: %v", err, err)
	})

	t.Run("removePermissions by short name is not found", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID("probe").WithBody(authz.RemovePermissionsBody{Permissions: collectionReadPerm("Movies")}),
			helper.CreateAuth(u1Key))
		var notFound *authz.RemovePermissionsNotFound
		require.True(t, errors.As(err, &notFound), "got %T: %v", err, err)
	})

	t.Run("removePermissions by qualified foreign name is rejected", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID(ns2+":probe").WithBody(authz.RemovePermissionsBody{Permissions: collectionReadPerm("Movies")}),
			helper.CreateAuth(u1Key))
		var badReq *authz.RemovePermissionsBadRequest
		require.True(t, errors.As(err, &badReq), "got %T: %v", err, err)
	})

	t.Run("revoke of a qualified foreign role is rejected", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID("u1").
				WithBody(authz.RevokeRoleFromUserBody{Roles: []string{ns2 + ":probe"}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(u1Key))
		var badReq *authz.RevokeRoleFromUserBadRequest
		require.True(t, errors.As(err, &badReq), "got %T: %v", err, err)
	})
}

// TestNamespaceHasPermissionLocalRolePositive proves the local-caller happy path:
// a bare resource is qualified into the caller's namespace and matches its own
// local role's stored qualified row.
func TestNamespaceHasPermissionLocalRolePositive(t *testing.T) {
	t.Parallel()
	ns1, _, u1Key, _ := twoNamespaces(t)

	helper.CreateRole(t, u1Key, readCollectionsRole("checker", "Docs"))
	t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":checker") })

	t.Run("granted bare permission matches the local role's qualified row", func(t *testing.T) {
		resp, err := helper.Client(t).Authz.HasPermission(
			authz.NewHasPermissionParams().WithID("checker").WithBody(collectionReadPerm("Docs")[0]),
			helper.CreateAuth(u1Key))
		require.NoError(t, err)
		require.True(t, resp.Payload, "own bare permission must qualify into ns1 and match")
	})

	t.Run("ungranted permission reports false", func(t *testing.T) {
		resp, err := helper.Client(t).Authz.HasPermission(
			authz.NewHasPermissionParams().WithID("checker").WithBody(collectionReadPerm("Other")[0]),
			helper.CreateAuth(u1Key))
		require.NoError(t, err)
		require.False(t, resp.Payload)
	})
}

// TestNamespaceOperatorLocalRoleDeleteVsEdit pins the deliberate asymmetry: an
// operator may delete a namespace-local role (needed for teardown) but may not
// edit its permissions or assign it — those stay namespace-owned.
func TestNamespaceOperatorLocalRoleDeleteVsEdit(t *testing.T) {
	t.Parallel()
	ns1, _, u1Key, _ := twoNamespaces(t)

	t.Run("operator may delete a namespace-local role", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("deletable", "*"))
		_, err := helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID(ns1+":deletable"), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		_, getErr := helper.Client(t).Authz.GetRole(
			authz.NewGetRoleParams().WithID(ns1+":deletable"), helper.CreateAuth(adminKey))
		var notFound *authz.GetRoleNotFound
		require.True(t, errors.As(getErr, &notFound), "got %T: %v", getErr, getErr)
	})

	t.Run("operator may not edit a namespace-local role's permissions", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("uneditable", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":uneditable") })
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(ns1+":uneditable").WithBody(authz.AddPermissionsBody{Permissions: collectionReadPerm("Movies")}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.AddPermissionsForbidden
		require.True(t, errors.As(err, &forbidden), "got %T: %v", err, err)
	})

	t.Run("operator may not assign a namespace-local role even within its namespace", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("assignable", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":assignable") })
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(ns1+":u1").
				WithBody(authz.AssignRoleToUserBody{Roles: []string{ns1 + ":assignable"}, UserType: models.UserTypeInputDb}),
			helper.CreateAuth(adminKey))
		var forbidden *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &forbidden), "got %T: %v", err, err)
	})
}

// TestNamespaceRoleReadEndpoints covers the role read endpoints that had no
// namespace coverage: getUsersForRole (owner and operator views of a local
// role), the deprecated users-for-role endpoint (gone on NS clusters), and the
// group role listings (operator allowed, namespaced caller denied).
func TestNamespaceRoleReadEndpoints(t *testing.T) {
	t.Parallel()
	ns1, _, u1Key, _ := twoNamespaces(t)

	t.Run("getUsersForRole lists a local role's members for owner and operator", func(t *testing.T) {
		helper.CreateRole(t, u1Key, readCollectionsRole("members", "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, ns1+":members") })
		helper.AssignRoleToUser(t, u1Key, "members", "u1")

		// The operator addresses the qualified role and sees the qualified user id.
		require.Contains(t, usersForRole(t, adminKey, ns1+":members"), ns1+":u1")
		// The namespaced owner addresses the short name and sees the stripped id.
		require.Contains(t, usersForRole(t, u1Key, "members"), "u1")
	})

	t.Run("deprecated users-for-role endpoint is gone on namespace clusters", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetUsersForRoleDeprecated(
			authz.NewGetUsersForRoleDeprecatedParams().WithID(authorization.Viewer), helper.CreateAuth(adminKey))
		var gone *authz.GetUsersForRoleDeprecatedGone
		require.True(t, errors.As(err, &gone), "got %T: %v", err, err)
	})

	t.Run("group role listings: operator allowed, namespaced caller denied", func(t *testing.T) {
		gRole := uniqueRole()
		helper.CreateRole(t, adminKey, readCollectionsRole(gRole, "*"))
		t.Cleanup(func() { helper.DeleteRole(t, adminKey, gRole) })
		group := "grp-" + gRole
		helper.AssignRoleToGroup(t, adminKey, gRole, group)

		// The operator sees the binding from both directions.
		require.Contains(t, roleNames(helper.GetRolesForGroup(t, adminKey, group, false)), gRole)
		require.Contains(t, helper.GetGroupsForRole(t, adminKey, gRole), group)

		// A namespaced caller may not read a (global) group's roles.
		_, err := helper.Client(t).Authz.GetRolesForGroup(
			authz.NewGetRolesForGroupParams().WithID(group).WithGroupType(string(models.GroupTypeOidc)),
			helper.CreateAuth(u1Key))
		var forbidden *authz.GetRolesForGroupForbidden
		require.True(t, errors.As(err, &forbidden), "got %T: %v", err, err)
	})
}
