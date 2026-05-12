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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clauthz "github.com/weaviate/weaviate/client/authz"
	clnamespaces "github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestNamespacesOIDC exercises NS-enabled OIDC integration points that
// need live cluster wiring:
//   - Classification: principal username carries the namespace prefix
//     (or stays bare for global operators); unknown namespaces rejected.
//   - Bare-form OIDC user IDs are rejected at the assign-role API.
//   - Narrowed admin can CRUD their namespace's collections but is
//     denied on cluster-only resources (manage_namespaces).
func TestNamespacesOIDC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		adminUser = "admin-user"
		adminKey  = "admin-key"
	)

	compose, err := docker.New().
		WithWeaviate().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithDbUsers().
		WithRBAC().
		WithRbacRoots(adminUser, "oidc-global").
		WithMockOIDC().
		WithMockOIDCNamespacedUsers().
		WithNamespaces().
		WithWeaviateEnv("AUTHENTICATION_OIDC_NAMESPACE_CLAIM", "weaviate_namespace").
		WithWeaviateEnv("AUTHENTICATION_OIDC_GLOBAL_PRINCIPAL_CLAIM", "weaviate_global_principal").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	helperURI := compose.GetMockOIDCHelper().URI()

	// Pre-create customer1 (and only customer1, so customer2 exercises
	// the namespace-existence rejection path).
	helper.CreateNamespace(t, "customer1", adminKey)
	defer helper.DeleteNamespace(t, "customer1", adminKey)

	t.Run("classification: namespaced token gets prefixed username", func(t *testing.T) {
		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-namespaced-customer1")
		info := helper.GetInfoForOwnUser(t, token)
		require.NotNil(t, info.Username)
		assert.Equal(t, "customer1:oidc-namespaced-customer1", *info.Username)
	})

	t.Run("classification: global-principal token gets bare username", func(t *testing.T) {
		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-global")
		info := helper.GetInfoForOwnUser(t, token)
		require.NotNil(t, info.Username)
		assert.Equal(t, "oidc-global", *info.Username)
	})

	t.Run("classification: unknown namespace claim → 401", func(t *testing.T) {
		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-namespaced-customer2")
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(token))
		require.Error(t, err)
		var unauth *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &unauth), "expected GetOwnInfoUnauthorized, got %T: %v", err, err)
	})

	t.Run("deleting namespace claim → 401", func(t *testing.T) {
		// Create customer2 so the OIDC token authenticates first, then
		// mark it for deletion. Whether the cleanup tick has finished or
		// not, the principal builder rejects with 401: deleting → 401
		// via IsActive; gone → 401 via the same error message.
		const ns = "customer2"
		helper.CreateNamespace(t, ns, adminKey)
		defer helper.WaitForNamespaceGone(t, ns, adminKey, 30*time.Second)

		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-namespaced-"+ns)
		// Sanity: pre-delete, the OIDC token authenticates.
		info := helper.GetInfoForOwnUser(t, token)
		require.NotNil(t, info.Username)

		helper.DeleteNamespace(t, ns, adminKey, helper.WithoutWaitForCleanup())

		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(token))
		require.Error(t, err)
		var unauth *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &unauth), "expected GetOwnInfoUnauthorized, got %T: %v", err, err)
	})

	t.Run("bare-form OIDC user ID: assignment rejected at the API", func(t *testing.T) {
		// validateUserIDForNamespaces rejects bare-form IDs on NS-enabled.
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			clauthz.NewAssignRoleToUserParams().
				WithID("alice"). // bare; no namespace prefix
				WithBody(clauthz.AssignRoleToUserBody{
					Roles:    []string{authorization.Admin},
					UserType: models.UserTypeInputOidc,
				}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var bad *clauthz.AssignRoleToUserBadRequest
		require.True(t, errors.As(err, &bad), "expected BadRequest, got %T: %v", err, err)
		assert.Contains(t, bad.Payload.Error[0].Message, "namespace-prefixed")
	})

	t.Run("narrowed admin: namespaced OIDC user has the right shape", func(t *testing.T) {
		// Narrowed admin is API-assignable to namespaced OIDC users on
		// NS-enabled clusters.
		oidcUserID := "customer1:oidc-namespaced-customer1"
		helper.AssignRoleToUserOIDC(t, adminKey, authorization.Admin, oidcUserID)
		defer helper.RevokeRoleFromUserOIDC(t, adminKey, authorization.Admin, oidcUserID)

		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-namespaced-customer1")

		// Narrowed admin → can CRUD collections inside their namespace.
		// "Movies" becomes "customer1:Movies" cluster-side via the matcher.
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, token)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

		// Verify the class landed under the namespace-qualified name —
		// the global operator sees raw stored names.
		stored := helper.GetClassAuth(t, "customer1:Movies", adminKey)
		assert.Equal(t, "customer1:Movies", stored.Class)

		// Narrowed admin → DENIED on namespace management (cluster-only,
		// not in tenantSafeAdminPermissions).
		_, err := helper.Client(t).Namespaces.DeleteNamespace(
			clnamespaces.NewDeleteNamespaceParams().WithNamespaceID("customer1"),
			helper.CreateAuth(token),
		)
		require.Error(t, err, "narrowed admin must not be able to delete namespaces")
		var nsForbidden *clnamespaces.DeleteNamespaceForbidden
		assert.True(t, errors.As(err, &nsForbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)
	})

	// Global operator via OIDC: oidc-global is bootstrapped as Root via
	// WithRbacRoots and must wield full operator privileges — cluster-only
	// operations (manage_namespaces) and cross-namespace visibility.
	t.Run("global operator via OIDC has cluster-only privileges", func(t *testing.T) {
		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-global")

		// Create a namespace as the global OIDC operator —
		// manage_namespaces is gated on Root and not in the narrowed
		// admin shape.
		const ns = "globalops"
		helper.CreateNamespace(t, ns, token)
		defer helper.DeleteNamespace(t, ns, token)

		// A namespaced DB user populates a collection; the operator
		// reads it back under the qualified name.
		const tenantSubject = "tenant-user"
		tenantID := ns + ":" + tenantSubject
		tenantKey := helper.CreateUserWithNamespace(t, tenantSubject, ns, token)
		defer helper.DeleteUser(t, tenantID, token)
		helper.AssignRoleToUser(t, token, authorization.Admin, tenantID)
		defer helper.RevokeRoleFromUser(t, token, authorization.Admin, tenantID)

		helper.CreateClassAuth(t, &models.Class{Class: "Reports"}, tenantKey)
		defer helper.DeleteClassAuth(t, ns+":Reports", token)

		stored := helper.GetClassAuth(t, ns+":Reports", token)
		assert.Equal(t, ns+":Reports", stored.Class)
	})

	// End-to-end DB-user path: namespaced user creation, dual-auth
	// convergence (OIDC and API key produce the same principal username),
	// tenant-safe admin, and narrowed-admin deny on namespace management.
	t.Run("end-to-end: namespaced DB user, dual auth path, narrowed admin", func(t *testing.T) {
		// customer2 makes the namespace-deny test realistic by giving
		// the cluster more than one namespace at probe time.
		helper.CreateNamespace(t, "customer2", adminKey)
		defer helper.DeleteNamespace(t, "customer2", adminKey)

		// One short subject reused across both namespaces — qualified storage
		// paths customer1:user and customer2:user keep the principals distinct.
		// Pairing the OIDC preseed subject with a DB user probes both auth paths.
		const sharedSubject = "user"
		const customer1ID = "customer1:" + sharedSubject

		customer1Key := helper.CreateUserWithNamespace(t, sharedSubject, "customer1", adminKey)
		defer helper.DeleteUser(t, customer1ID, adminKey)

		// Assign admin on both userTypes — the DB and OIDC paths each
		// resolve to a distinct Casbin user key that needs its own row.
		helper.AssignRoleToUser(t, adminKey, authorization.Admin, customer1ID)
		defer helper.RevokeRoleFromUser(t, adminKey, authorization.Admin, customer1ID)
		helper.AssignRoleToUserOIDC(t, adminKey, authorization.Admin, customer1ID)
		defer helper.RevokeRoleFromUserOIDC(t, adminKey, authorization.Admin, customer1ID)

		customer1OIDCToken, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, sharedSubject)

		// Both auth paths produce the same principal username — the OIDC
		// namespace prefix matches what the user-creation API stores.
		dbInfo := helper.GetInfoForOwnUser(t, customer1Key)
		require.NotNil(t, dbInfo.Username)
		assert.Equal(t, customer1ID, *dbInfo.Username)

		oidcInfo := helper.GetInfoForOwnUser(t, customer1OIDCToken)
		require.NotNil(t, oidcInfo.Username)
		assert.Equal(t, customer1ID, *oidcInfo.Username)

		// customer1's user creates a collection via the DB API key —
		// matcher-specialized to customer1:Books. Title is defined
		// upfront so later object inserts are typed.
		helper.CreateClassAuth(t, &models.Class{
			Class: "Books",
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
			},
		}, customer1Key)
		defer helper.DeleteClassAuth(t, "customer1:Books", adminKey)

		// Operator (admin static API key, global) sees the qualified name.
		stored := helper.GetClassAuth(t, "customer1:Books", adminKey)
		assert.Equal(t, "customer1:Books", stored.Class)

		// customer1's user via OIDC reads the same class: requests "Books",
		// resolver prefixes to customer1:Books. Confirms OIDC and DB paths
		// share the matcher specialization.
		viaOIDC := helper.GetClassAuth(t, "Books", customer1OIDCToken)
		assert.Equal(t, "customer1:Books", viaOIDC.Class)

		// Narrowed admin → DENIED on namespace management (cluster-only).
		_, err = helper.Client(t).Namespaces.DeleteNamespace(
			clnamespaces.NewDeleteNamespaceParams().WithNamespaceID("customer2"),
			helper.CreateAuth(customer1Key),
		)
		require.Error(t, err, "narrowed admin must not be able to delete namespaces via DB key")
		var nsForbidden *clnamespaces.DeleteNamespaceForbidden
		assert.True(t, errors.As(err, &nsForbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)

		// Cross-namespace isolation: a parallel user in customer2 with the
		// *same* short subject. Different prefixes (customer1:user vs
		// customer2:user) keep storage keys distinct.
		customer2ID := "customer2:" + sharedSubject
		customer2Key := helper.CreateUserWithNamespace(t, sharedSubject, "customer2", adminKey)
		defer helper.DeleteUser(t, customer2ID, adminKey)
		helper.AssignRoleToUser(t, adminKey, authorization.Admin, customer2ID)
		defer helper.RevokeRoleFromUser(t, adminKey, authorization.Admin, customer2ID)

		// API keys differ even though the short subjects match — proves
		// credentials are keyed by qualified id, not short id.
		assert.NotEqual(t, customer1Key, customer2Key, "shared short subject must not produce the same API key across namespaces")

		// customer2's user creates a collection in customer2.
		helper.CreateClassAuth(t, &models.Class{
			Class:      "Books",
			Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
		}, customer2Key)
		defer helper.DeleteClassAuth(t, "customer2:Books", adminKey)

		// Both tenants insert an object with the *same* UUID. The
		// qualified storage paths customer1:Books vs customer2:Books
		// keep them distinct.
		const sharedID strfmt.UUID = "11111111-2222-3333-4444-555555555555"

		customer1Obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         sharedID,
			Class:      "Books",
			Properties: map[string]interface{}{"title": "customer1-book"},
		}, customer1Key)
		require.NoError(t, err)
		customer2Obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         sharedID,
			Class:      "Books",
			Properties: map[string]interface{}{"title": "customer2-book"},
		}, customer2Key)
		require.NoError(t, err)
		assert.Equal(t, sharedID, customer1Obj.ID)
		assert.Equal(t, sharedID, customer2Obj.ID)

		// Operator sees both qualified classes side-by-side.
		assert.Equal(t, "customer1:Books", helper.GetClassAuth(t, "customer1:Books", adminKey).Class)
		assert.Equal(t, "customer2:Books", helper.GetClassAuth(t, "customer2:Books", adminKey).Class)

		// Object-level isolation: each tenant fetches their own object
		// via the short class name (resolver specializes to their
		// namespace) — same UUID, distinct payload, no collision.
		got1, err := helper.GetObjectAuth(t, "Books", sharedID, customer1Key)
		require.NoError(t, err)
		assert.Equal(t, "customer1-book", got1.Properties.(map[string]interface{})["title"])
		got2, err := helper.GetObjectAuth(t, "Books", sharedID, customer2Key)
		require.NoError(t, err)
		assert.Equal(t, "customer2-book", got2.Properties.(map[string]interface{})["title"])
	})

	// Group binding carries no namespace at the API layer; the matcher
	// specializes the bound role at enforce time. Bob has no direct role —
	// he inherits admin via the AllUsers group claim on his OIDC token.
	t.Run("narrowed admin via OIDC group binding", func(t *testing.T) {
		const groupName = "AllUsers"
		const bobSubject = "oidc-customer1-group-member"
		const bobUsername = "customer1:" + bobSubject

		helper.AssignRoleToGroup(t, adminKey, authorization.Admin, groupName)
		defer helper.RevokeRoleFromGroup(t, adminKey, authorization.Admin, groupName)

		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, bobSubject)

		info := helper.GetInfoForOwnUser(t, token)
		require.NotNil(t, info.Username)
		assert.Equal(t, bobUsername, *info.Username)
		assert.Contains(t, info.Groups, groupName, "OIDC token must carry the group claim")

		// Bob has no direct admin assignment. If group binding works,
		// the matcher specializes admin to customer1 via his group
		// membership and the schema create succeeds.
		helper.CreateClassAuth(t, &models.Class{Class: "Albums"}, token)
		defer helper.DeleteClassAuth(t, "customer1:Albums", adminKey)

		stored := helper.GetClassAuth(t, "customer1:Albums", adminKey)
		assert.Equal(t, "customer1:Albums", stored.Class)
	})
}
