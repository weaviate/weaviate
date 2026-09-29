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

package authn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthnGetOwnInfoWithAnonAccessEnabled(t *testing.T) {
	helper.SetupClient(helper.SharedServerURI)
	defer helper.ResetClient()

	t.Run("Get own info for anonymous access", func(t *testing.T) {
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), nil)
		require.NotNil(t, err)
		parsed, ok := err.(*users.GetOwnInfoUnauthorized) //nolint:errorlint
		require.True(t, ok)
		require.Equal(t, 401, parsed.Code())
	})
}

func TestAuthnGetOwnInfoWithAdminlistAndOidc(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().
		WithWeaviate().
		WithMockOIDC().
		WithWeaviateEnv("AUTHORIZATION_ADMINLIST_ENABLED", "true").
		WithWeaviateEnv("AUTHORIZATION_ADMINLIST_USERS", "admin-user").
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())

	// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
	// description for details
	token, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	t.Run("Get own info", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, token)
		require.Equal(t, "admin-user", *info.Username)
		require.Len(t, info.Roles, 0)
		require.Len(t, info.Groups, 0)
	})

	t.Run("Unauthenticated", func(t *testing.T) {
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth("non-existent"))
		require.NotNil(t, err)
		parsed, ok := err.(*users.GetOwnInfoUnauthorized) //nolint:errorlint
		require.True(t, ok)
		require.Equal(t, 401, parsed.Code())
	})
}

// TestAuthnGetOwnInfoWithRBAC covers API-key and OIDC users on one cluster.
// Each auth type uses its own user and role so the two flows don't interfere.
func TestAuthnGetOwnInfoWithRBAC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	adminUser := "admin-user"
	adminKey := "admin-key"

	apiKeyUser := "apikey-user"
	apiKeyUserKey := "apikey-user-key"
	apiKeyRole := "testingOwnRoleApiKey"

	oidcUser := "custom-user"
	oidcRole := "testingOwnRoleOidc"

	compose, err := docker.New().
		WithWeaviate().
		WithRBAC().
		WithApiKey().
		WithUserApiKey(apiKeyUser, apiKeyUserKey).
		WithUserApiKey(adminUser, adminKey).
		WithRbacRoots(adminUser).
		WithRbacViewers(apiKeyUser, oidcUser).
		WithMockOIDC().
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())

	// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
	// description for details
	tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())
	tokenCustom, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

	defer func() {
		helper.DeleteRole(t, adminKey, apiKeyRole)
		helper.DeleteRole(t, adminKey, oidcRole)
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	createRole := func(t *testing.T, key, name string) {
		helper.CreateRole(
			t,
			key,
			&models.Role{
				Name: &name,
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			},
		)
	}

	t.Run("api key", func(t *testing.T) {
		t.Run("Get own info - no roles", func(t *testing.T) {
			info := helper.GetInfoForOwnUser(t, apiKeyUserKey)
			require.Equal(t, apiKeyUser, *info.Username)
			require.Len(t, info.Roles, 0)
			require.Len(t, info.Groups, 0)
		})

		t.Run("Create and assign role", func(t *testing.T) {
			createRole(t, adminKey, apiKeyRole)
			helper.AssignRoleToUser(t, adminKey, apiKeyRole, apiKeyUser)
		})

		t.Run("Get own roles - existing roles", func(t *testing.T) {
			info := helper.GetInfoForOwnUser(t, apiKeyUserKey)
			require.Equal(t, apiKeyUser, *info.Username)
			require.Len(t, info.Roles, 1)
			require.Equal(t, apiKeyRole, *info.Roles[0].Name)
			require.Len(t, info.Groups, 0)
		})

		t.Run("Unauthenticated", func(t *testing.T) {
			_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth("non-existent"))
			require.NotNil(t, err)
			parsed, ok := err.(*users.GetOwnInfoUnauthorized) //nolint:errorlint
			require.True(t, ok)
			require.Equal(t, 401, parsed.Code())
		})
	})

	t.Run("oidc", func(t *testing.T) {
		t.Run("Get own info - no roles", func(t *testing.T) {
			info := helper.GetInfoForOwnUser(t, tokenCustom)
			require.Equal(t, oidcUser, *info.Username)
			require.Len(t, info.Roles, 0)
			require.Len(t, info.Groups, 1)
		})

		t.Run("Create and assign role", func(t *testing.T) {
			createRole(t, tokenAdmin, oidcRole)
			helper.AssignRoleToUserOIDC(t, tokenAdmin, oidcRole, oidcUser)
		})

		t.Run("Get own roles - existing roles", func(t *testing.T) {
			info := helper.GetInfoForOwnUser(t, tokenCustom)
			require.Equal(t, oidcUser, *info.Username)
			require.Len(t, info.Roles, 1)
			require.Equal(t, oidcRole, *info.Roles[0].Name)
			require.Len(t, info.Groups, 1)
		})
	})
}
