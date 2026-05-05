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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clauthz "github.com/weaviate/weaviate/client/authz"
	clnamespaces "github.com/weaviate/weaviate/client/namespaces"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestNamespacesOIDC covers the integration points that need live cluster
// wiring on a namespace-enabled OIDC cluster. The classification matrix
// (type mismatches, both/neither claims, etc.) is unit-tested in the oidc
// package; this test exercises the parts that depend on real plumbing:
//
//   - Classification → principal username carries the namespace prefix
//     (or stays bare for global operators); existence check rejects
//     unknown-namespace tokens.
//   - Bare-form gate → assigning a role to a bare-form OIDC user ID is
//     rejected at the API.
//   - Built-in narrowing → admin assigned to a namespaced OIDC user can
//     CRUD their namespace's collections but is denied on cluster-only
//     resources (manage_namespaces).
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
		WithRBAC().
		WithRbacRoots(adminUser).
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
		// Assign admin to the namespaced OIDC user. After commit 1's gate
		// deletion this is allowed even on NS-enabled clusters.
		oidcUserID := "customer1:oidc-namespaced-customer1"
		helper.AssignRoleToUserOIDC(t, adminKey, authorization.Admin, oidcUserID)
		defer helper.RevokeRoleFromUserOIDC(t, adminKey, authorization.Admin, oidcUserID)

		token, _ := docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-namespaced-customer1")

		// Narrowed admin → can CRUD collections inside their namespace.
		// The matcher specializes the policy resource path to the
		// principal's namespace, so the user creates "Movies" which
		// becomes "customer1:Movies" cluster-side.
		class := &models.Class{Class: "Movies"}
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(
			clschema.NewSchemaObjectsCreateParams().WithObjectClass(class),
			helper.CreateAuth(token),
		)
		require.NoError(t, err, "narrowed admin should be able to create collections in own namespace")
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

		// Verify the class landed under the namespace-qualified name by
		// fetching it as the global operator (admin key sees raw stored
		// names, no namespace specialization).
		stored := helper.GetClassAuth(t, "customer1:Movies", adminKey)
		assert.Equal(t, "customer1:Movies", stored.Class)

		// Narrowed admin → DENIED on namespace management (cluster-only,
		// not in tenantSafeAdminPermissions).
		_, err = helper.Client(t).Namespaces.DeleteNamespace(
			clnamespaces.NewDeleteNamespaceParams().WithNamespaceID("customer1"),
			helper.CreateAuth(token),
		)
		require.Error(t, err, "narrowed admin must not be able to delete namespaces")
		var nsForbidden *clnamespaces.DeleteNamespaceForbidden
		assert.True(t, errors.As(err, &nsForbidden), "expected DeleteNamespaceForbidden, got %T: %v", err, err)
	})
}
