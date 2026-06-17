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
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// createUserExpectErr issues a CreateUser request and returns the resulting
// error so the caller can errors.As it for a specific 4xx type.
func createUserExpectErr(t *testing.T, userID, key string) error {
	t.Helper()
	_, err := helper.Client(t).Users.CreateUser(
		users.NewCreateUserParams().WithUserID(userID).WithBody(users.CreateUserBody{}),
		helper.CreateAuth(key),
	)
	return err
}

// createNamespacedViewerUser mirrors createNamespacedUser (collection_alias_test.go)
// but grants the built-in viewer role instead of admin, for the deny-without-grant
// scenario.
func createNamespacedViewerUser(t *testing.T, userID, ns, adminKey string) string {
	t.Helper()

	var apikey string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(ns+":"+userID).WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		if !assert.NoError(c, err) {
			return
		}
		if !assert.NotNil(c, resp.Payload.Apikey) {
			return
		}
		apikey = *resp.Payload.Apikey
	}, 10*time.Second, 50*time.Millisecond, "user %q could not be created", userID)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := helper.Client(t).Users.GetOwnInfo(
			users.NewGetOwnInfoParams(), helper.CreateAuth(apikey))
		assert.NoError(c, err)
	}, 10*time.Second, 50*time.Millisecond, "user %q apikey not recognized after create", userID)

	helper.AssignRoleToUser(t, adminKey, authorization.Viewer, ns+":"+userID)
	helper.WaitForOwnRole(t, apikey, authorization.Viewer)
	return apikey
}

// TestNamespacedAdminLifecycle — a namespaced admin runs the full
// create / get / list / deactivate / activate / rotate / delete lifecycle
// on a user in its own namespace, with responses stripped to the short form.
func TestNamespacedAdminLifecycle(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)

	nsAdminKey := createNamespacedUser(t, "alice", ns, adminKey)

	// The admin creates short-name "bob" in her own namespace.
	bobKey := helper.CreateUser(t, "bob", nsAdminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":bob", adminKey) })

	// Operator-side: the user was qualified into the caller's namespace.
	bobFromOperator := helper.GetUser(t, ns+":bob", adminKey)
	require.Equal(t, ns, bobFromOperator.Namespace)

	// Admin GETs "bob": short id, no `:`, namespace hidden.
	bobFromAdmin := helper.GetUser(t, "bob", nsAdminKey)
	require.Equal(t, "bob", *bobFromAdmin.UserID)
	require.Empty(t, bobFromAdmin.Namespace)

	// Admin can read bob's roles via the short id — proves the authz-handler
	// resolver + matcher specialization end to end (bob has no roles yet).
	require.Empty(t, helper.GetRolesForUser(t, "bob", nsAdminKey, false))

	// Admin LIST sees both herself (alice) and bob, both as short ids.
	list := helper.ListAllUsers(t, nsAdminKey)
	require.Len(t, list, 2)
	for _, u := range list {
		require.NotNil(t, u.UserID)
		require.NotContains(t, *u.UserID, ":", "list response leaked a qualified id")
		require.Empty(t, u.Namespace, "list response leaked the namespace field")
	}

	// Deactivate → Active=false.
	helper.DeactivateUser(t, nsAdminKey, "bob", false)
	require.False(t, *helper.GetUser(t, "bob", nsAdminKey).Active)

	// Activate → Active=true.
	helper.ActivateUser(t, nsAdminKey, "bob")
	require.True(t, *helper.GetUser(t, "bob", nsAdminKey).Active)

	// Rotate → new key works, old key 401s once the follower catches up.
	newBobKey := helper.RotateKey(t, "bob", nsAdminKey)
	require.NotEqual(t, bobKey, newBobKey)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(bobKey))
		assert.Error(c, err)
	}, 10*time.Second, 50*time.Millisecond, "old apikey must be invalidated after rotate")
	require.NotNil(t, helper.GetInfoForOwnUser(t, newBobKey))

	// Delete → admin GET 404s.
	helper.DeleteUser(t, "bob", nsAdminKey)
	_, err := helper.Client(t).Users.GetUserInfo(
		users.NewGetUserInfoParams().WithUserID("bob"), helper.CreateAuth(nsAdminKey),
	)
	require.Error(t, err)
	var notFound *users.GetUserInfoNotFound
	require.True(t, errors.As(err, &notFound))
}

// TestNamespacedUserCrossNamespaceIsolation — two namespaces each host a "bob";
// each namespace's admin sees only its own users, deletes only its own, and
// every response (success or 404) is free of the namespace separator.
func TestNamespacedUserCrossNamespaceIsolation(t *testing.T) {
	t.Parallel()
	ns1, ns2 := uniqueNS(), uniqueNS()
	helper.CreateNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)

	ns1AdminKey := createNamespacedUser(t, "alice", ns1, adminKey)
	ns2AdminKey := createNamespacedUser(t, "carol", ns2, adminKey)

	// Operator pre-creates the same short name in both namespaces.
	helper.CreateUserWithNamespace(t, "bob", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":bob", adminKey) })
	helper.CreateUserWithNamespace(t, "bob", ns2, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns2+":bob", adminKey) })

	// ns1 admin sees only ns1's users; ns2 admin sees only ns2's.
	ns1List := helper.ListAllUsers(t, ns1AdminKey)
	require.Len(t, ns1List, 2, "ns1 admin must see alice + bob in ns1 only")
	for _, u := range ns1List {
		require.NotContains(t, *u.UserID, ":")
		require.Empty(t, u.Namespace)
	}
	ns2List := helper.ListAllUsers(t, ns2AdminKey)
	require.Len(t, ns2List, 2, "ns2 admin must see carol + bob in ns2 only")
	for _, u := range ns2List {
		require.NotContains(t, *u.UserID, ":")
	}

	// ns1 admin GET "bob" → 200 from ns1.
	require.Equal(t, "bob", *helper.GetUser(t, "bob", ns1AdminKey).UserID)

	// ns1 admin deletes its own ns1:bob — ns2:bob is untouched.
	helper.DeleteUser(t, "bob", ns1AdminKey)
	_, err := helper.Client(t).Users.GetUserInfo(
		users.NewGetUserInfoParams().WithUserID(ns1+":bob"), helper.CreateAuth(adminKey),
	)
	require.Error(t, err, "ns1:bob should be gone after the ns1 admin's delete")
	require.Equal(t, ns2, helper.GetUser(t, ns2+":bob", adminKey).Namespace)

	// ns1 admin asks for "carol" — exists in ns2 only, must 404 from ns1's view.
	_, err = helper.Client(t).Users.GetUserInfo(
		users.NewGetUserInfoParams().WithUserID("carol"), helper.CreateAuth(ns1AdminKey),
	)
	require.Error(t, err)
	var notFound *users.GetUserInfoNotFound
	require.True(t, errors.As(err, &notFound))
}

// TestNamespacedViewerDeniedUserMutations — viewer in a namespace can read
// users, cannot mutate.
func TestNamespacedViewerDeniedUserMutations(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)

	helper.CreateUserWithNamespace(t, "bob", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":bob", adminKey) })

	viewerKey := createNamespacedViewerUser(t, "dan", ns, adminKey)

	// Reads: allowed.
	require.Equal(t, "bob", *helper.GetUser(t, "bob", viewerKey).UserID)
	require.Len(t, helper.ListAllUsers(t, viewerKey), 2)

	// Mutations: 403.
	err := createUserExpectErr(t, "eve", viewerKey)
	require.Error(t, err)
	var createForbidden *users.CreateUserForbidden
	require.True(t, errors.As(err, &createForbidden), "expected CreateUserForbidden, got %T", err)

	_, err = helper.Client(t).Users.DeleteUser(
		users.NewDeleteUserParams().WithUserID("bob"), helper.CreateAuth(viewerKey),
	)
	require.Error(t, err)
	var deleteForbidden *users.DeleteUserForbidden
	require.True(t, errors.As(err, &deleteForbidden))

	_, err = helper.Client(t).Users.DeactivateUser(
		users.NewDeactivateUserParams().WithUserID("bob").WithBody(users.DeactivateUserBody{}),
		helper.CreateAuth(viewerKey),
	)
	require.Error(t, err)
	var deactivateForbidden *users.DeactivateUserForbidden
	require.True(t, errors.As(err, &deactivateForbidden))

	_, err = helper.Client(t).Users.RotateUserAPIKey(
		users.NewRotateUserAPIKeyParams().WithUserID("dan"), helper.CreateAuth(viewerKey),
	)
	require.Error(t, err)
	var rotateForbidden *users.RotateUserAPIKeyForbidden
	require.True(t, errors.As(err, &rotateForbidden), "rotate of own user must 403 without UpdateUsers")
}

// TestGlobalOperatorReach — the operator manages users in multiple namespaces
// via the qualified id, reads roles via the current endpoint, and assigns /
// revokes roles on a namespaced user (matcher blast-radius guard). The
// deprecated role-read endpoint is gated off on NS clusters — 410.
func TestGlobalOperatorReach(t *testing.T) {
	t.Parallel()
	ns1, ns2 := uniqueNS(), uniqueNS()
	helper.CreateNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)

	helper.CreateUserWithNamespace(t, "bob", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":bob", adminKey) })
	helper.CreateUserWithNamespace(t, "bob", ns2, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns2+":bob", adminKey) })

	// Cross-namespace gets — operator sees the qualified ids + namespace field.
	bobInNs1 := helper.GetUser(t, ns1+":bob", adminKey)
	require.Equal(t, ns1+":bob", *bobInNs1.UserID)
	require.Equal(t, ns1, bobInNs1.Namespace)
	bobInNs2 := helper.GetUser(t, ns2+":bob", adminKey)
	require.Equal(t, ns2, bobInNs2.Namespace)

	// Operator LIST — both bobs visible, qualified, namespace populated.
	all := helper.ListAllUsers(t, adminKey)
	var ns1Bob, ns2Bob bool
	for _, u := range all {
		switch *u.UserID {
		case ns1 + ":bob":
			ns1Bob = true
			require.Equal(t, ns1, u.Namespace)
		case ns2 + ":bob":
			ns2Bob = true
			require.Equal(t, ns2, u.Namespace)
		}
	}
	require.True(t, ns1Bob && ns2Bob, "operator must see both namespaces' bobs")

	// Read roles via the current endpoint.
	require.NotNil(t, helper.GetRolesForUser(t, ns1+":bob", adminKey, false))

	// Deprecated endpoint is gated off on namespace-enabled clusters even
	// for the operator — 410 Gone.
	_, err := helper.Client(t).Authz.GetRolesForUserDeprecated(
		authz.NewGetRolesForUserDeprecatedParams().WithID(ns1+":bob"),
		helper.CreateAuth(adminKey),
	)
	require.Error(t, err)
	var deprecatedGone *authz.GetRolesForUserDeprecatedGone
	require.True(t, errors.As(err, &deprecatedGone), "expected GetRolesForUserDeprecatedGone, got %T", err)

	// Assign + revoke a built-in role on a namespaced user — the matcher's
	// widen branch keeps users/<ns>:<user> reachable for global operators.
	helper.AssignRoleToUser(t, adminKey, authorization.Viewer, ns1+":bob")
	bobRoles := helper.GetRolesForUser(t, ns1+":bob", adminKey, false)
	require.True(t, containsRoleName(bobRoles, authorization.Viewer))
	helper.RevokeRoleFromUser(t, adminKey, authorization.Viewer, ns1+":bob")
	bobRoles = helper.GetRolesForUser(t, ns1+":bob", adminKey, false)
	require.False(t, containsRoleName(bobRoles, authorization.Viewer), "role must be revoked")
}

// TestNamespacedAdminConflictsAndCollisions — re-create → 409 with no
// `:` in the message; double-(de)activate → 409; a name colliding with
// the configured root user succeeds inside a namespace because the
// resolver qualifies the storage key before the isRootUser check.
func TestNamespacedAdminConflictsAndCollisions(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)
	nsAdminKey := createNamespacedUser(t, "alice", ns, adminKey)

	// 1. Create bob, then re-create → 409 with no `:` in the message.
	helper.CreateUser(t, "bob", nsAdminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":bob", adminKey) })

	err := createUserExpectErr(t, "bob", nsAdminKey)
	require.Error(t, err)
	var conflict *users.CreateUserConflict
	require.True(t, errors.As(err, &conflict), "expected CreateUserConflict, got %T", err)
	require.NotNil(t, conflict.Payload)
	for _, e := range conflict.Payload.Error {
		require.NotContains(t, e.Message, ":", "409 message leaked the namespace separator")
	}

	// 2. Already-(in)active → 409.
	helper.DeactivateUser(t, nsAdminKey, "bob", false)

	_, err = helper.Client(t).Users.DeactivateUser(
		users.NewDeactivateUserParams().WithUserID("bob").WithBody(users.DeactivateUserBody{}),
		helper.CreateAuth(nsAdminKey),
	)
	require.Error(t, err)
	var deactivateConflict *users.DeactivateUserConflict
	require.True(t, errors.As(err, &deactivateConflict), "expected DeactivateUserConflict, got %T", err)

	helper.ActivateUser(t, nsAdminKey, "bob")
	_, err = helper.Client(t).Users.ActivateUser(
		users.NewActivateUserParams().WithUserID("bob"), helper.CreateAuth(nsAdminKey),
	)
	require.Error(t, err)
	var activateConflict *users.ActivateUserConflict
	require.True(t, errors.As(err, &activateConflict), "expected ActivateUserConflict, got %T", err)

	// 3. Namespaced caller creates "admin-user" (the configured root) →
	//    succeeds as ns:admin-user; the qualified key never matches the
	//    bare configured root, so isRootUser / staticUserExists don't fire.
	helper.CreateUser(t, "admin-user", nsAdminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":admin-user", adminKey) })

	// Operator confirms it landed in the namespace as a fresh user.
	adminUserInNs := helper.GetUser(t, ns+":admin-user", adminKey)
	require.Equal(t, ns, adminUserInNs.Namespace)
}

// TestNamespacedAdminAuthzSurface — failure-mode pins for the authz user-role
// surface from a namespaced caller's view: deprecated endpoint fails closed
// (matcher cannot specialize the unqualified key), and assign/revoke fail at
// authz on the qualified key (no AssignAndRevokeUsers in the widened admin).
func TestNamespacedAdminAuthzSurface(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)
	nsAdminKey := createNamespacedUser(t, "alice", ns, adminKey)

	helper.CreateUserWithNamespace(t, "bob", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":bob", adminKey) })

	// 1. Deprecated endpoint is gated off at the handler — 410 Gone.
	_, err := helper.Client(t).Authz.GetRolesForUserDeprecated(
		authz.NewGetRolesForUserDeprecatedParams().WithID("bob"),
		helper.CreateAuth(nsAdminKey),
	)
	require.Error(t, err)
	var deprecatedGone *authz.GetRolesForUserDeprecatedGone
	require.True(t, errors.As(err, &deprecatedGone), "expected GetRolesForUserDeprecatedGone, got %T", err)

	// 2. assign → 403-at-authz (no AssignAndRevokeUsers grant for namespaced admin).
	_, err = helper.Client(t).Authz.AssignRoleToUser(
		authz.NewAssignRoleToUserParams().WithID("bob").WithBody(authz.AssignRoleToUserBody{
			Roles:    []string{authorization.Viewer},
			UserType: models.UserTypeInputDb,
		}),
		helper.CreateAuth(nsAdminKey),
	)
	require.Error(t, err)
	var assignForbidden *authz.AssignRoleToUserForbidden
	require.True(t, errors.As(err, &assignForbidden), "expected AssignRoleToUserForbidden, got %T", err)

	// 3. revoke → same failure mode.
	_, err = helper.Client(t).Authz.RevokeRoleFromUser(
		authz.NewRevokeRoleFromUserParams().WithID("bob").WithBody(authz.RevokeRoleFromUserBody{
			Roles:    []string{authorization.Viewer},
			UserType: models.UserTypeInputDb,
		}),
		helper.CreateAuth(nsAdminKey),
	)
	require.Error(t, err)
	var revokeForbidden *authz.RevokeRoleFromUserForbidden
	require.True(t, errors.As(err, &revokeForbidden), "expected RevokeRoleFromUserForbidden, got %T", err)
}

// TestCreateUserAgainstDeletingNamespace — createUser into a namespace
// mid-delete is 422. A class makes cleanup non-instant so the race lands
// somewhere between deleting and gone; both surface 422.
func TestCreateUserAgainstDeletingNamespace(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)

	nsAdminKey := createNamespacedUser(t, "alice", ns, adminKey)
	helper.CreateClassAuth(t, &models.Class{
		Class:      "Movies",
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, nsAdminKey)
	defer helper.DeleteClassWithoutAssert(t, ns+":Movies", adminKey)

	helper.DeleteNamespace(t, ns, adminKey, helper.WithoutWaitForCleanup())
	t.Cleanup(func() { helper.WaitForNamespaceGone(t, ns, adminKey, 30*time.Second) })

	_, err := helper.Client(t).Users.CreateUser(
		users.NewCreateUserParams().WithUserID(ns+":new-user").WithBody(users.CreateUserBody{}),
		helper.CreateAuth(adminKey),
	)
	require.Error(t, err)
	var unproc *users.CreateUserUnprocessableEntity
	require.True(t, errors.As(err, &unproc),
		"expected CreateUserUnprocessableEntity (deleting or gone), got %T: %v", err, err)
}

// TestNamespacedAdminSelfTargetIs422 — self-delete/deactivate via the
// short name hits the self-target guard on the resolved key; the 422
// message must not leak the ':' separator.
func TestNamespacedAdminSelfTargetIs422(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)
	nsAdminKey := createNamespacedUser(t, "alice", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":alice", adminKey) })

	t.Run("self-deactivate", func(t *testing.T) {
		_, err := helper.Client(t).Users.DeactivateUser(
			users.NewDeactivateUserParams().WithUserID("alice").WithBody(users.DeactivateUserBody{}),
			helper.CreateAuth(nsAdminKey),
		)
		require.Error(t, err)
		var unproc *users.DeactivateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc),
			"expected DeactivateUserUnprocessableEntity, got %T: %v", err, err)
		require.NotNil(t, unproc.Payload)
		for _, e := range unproc.Payload.Error {
			require.NotContains(t, e.Message, ":", "self-deactivate 422 leaked the namespace separator")
		}
	})

	t.Run("self-delete", func(t *testing.T) {
		// Runs last: the 422 keeps alice alive for the outer t.Cleanup.
		_, err := helper.Client(t).Users.DeleteUser(
			users.NewDeleteUserParams().WithUserID("alice"),
			helper.CreateAuth(nsAdminKey),
		)
		require.Error(t, err)
		var unproc *users.DeleteUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc),
			"expected DeleteUserUnprocessableEntity, got %T: %v", err, err)
		require.NotNil(t, unproc.Payload)
		for _, e := range unproc.Payload.Error {
			require.NotContains(t, e.Message, ":", "self-delete 422 leaked the namespace separator")
		}
	})
}

// containsRoleName reports whether any role in roles has the given name.
func containsRoleName(roles []*models.Role, name string) bool {
	for _, r := range roles {
		if r != nil && r.Name != nil && *r.Name == name {
			return true
		}
	}
	return false
}
