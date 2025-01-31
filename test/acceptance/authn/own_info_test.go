//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authn

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthnGetOwnInfoWithAdminlistAndOidc(t *testing.T) {
	pwAdminUser := os.Getenv("WCS_DUMMY_CI_PW")
	if pwAdminUser == "" {
		t.Skip("No password supplied")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("AUTHORIZATION_ADMINLIST_ENABLED", "true").
		WithWeaviateEnv("AUTHORIZATION_ADMINLIST_USERS", "admin-user").
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	authEndpoint, tokenEndpoint := docker.GetEndpointsFromMockOIDC(compose.GetMockOIDC().URI())

	// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
	// description for details
	token, _ := docker.GetTokensFromMockOIDC(t, authEndpoint, tokenEndpoint)

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

func TestAuthnGetOwnInfoWithOidc(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithMockOIDC().
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	authEndpoint, tokenEndpoint := docker.GetEndpointsFromMockOIDC(compose.GetMockOIDC().URI())

	// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
	// description for details
	token, _ := docker.GetTokensFromMockOIDC(t, authEndpoint, tokenEndpoint)

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

func TestAuthnGetOwnInfoWithRBAC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	customUser := "custom-user"
	customKey := "custom-key"

	testingRole := "testingOwnRole"

	adminKey := "admin-key"
	adminUser := "admin-user"

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithRBAC().
		WithApiKey().
		WithUserApiKey(customUser, customKey).
		WithUserApiKey(adminUser, adminKey).
		WithRbacAdmins(adminUser).
		WithRbacViewers(customUser).
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	defer func() {
		helper.DeleteRole(t, adminKey, testingRole)
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	t.Run("Get own info - no roles", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, customKey)
		require.Equal(t, customUser, *info.Username)
		require.Len(t, info.Roles, 0)
		require.Len(t, info.Groups, 0)
	})

	t.Run("Create and assign role", func(t *testing.T) {
		helper.CreateRole(
			t,
			adminKey,
			&models.Role{
				Name: &testingRole,
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			},
		)
		helper.AssignRoleToUser(t, adminKey, testingRole, customUser)
	})

	t.Run("Get own roles - existing roles", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, customKey)
		require.Equal(t, customUser, *info.Username)
		require.Len(t, info.Roles, 1)
		require.Equal(t, testingRole, *info.Roles[0].Name)
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

func TestAuthnGetOwnInfoWithRBACAndOIDC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	customUser := "custom-user"

	testingRole := "testingOwnRole"

	adminUser := "admin-user"

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithRBAC().
		WithApiKey().
		WithRbacAdmins(adminUser).
		WithRbacViewers(customUser).
		WithMockOIDC().
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	authEndpoint, tokenEndpoint := docker.GetEndpointsFromMockOIDC(compose.GetMockOIDC().URI())

	// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
	// description for details
	tokenAdmin, _ := docker.GetTokensFromMockOIDC(t, authEndpoint, tokenEndpoint)
	tokenCustom, _ := docker.GetTokensFromMockOIDC(t, authEndpoint, tokenEndpoint)

	defer func() {
		helper.DeleteRole(t, tokenAdmin, testingRole)
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	t.Run("Get own info - no roles", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, tokenCustom)
		require.Equal(t, customUser, *info.Username)
		require.Len(t, info.Roles, 0)
		require.Len(t, info.Groups, 1)
	})

	t.Run("Create and assign role", func(t *testing.T) {
		helper.CreateRole(
			t,
			tokenAdmin,
			&models.Role{
				Name: &testingRole,
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			},
		)
		helper.AssignRoleToUser(t, tokenAdmin, testingRole, customUser)
	})

	t.Run("Get own roles - existing roles", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, tokenCustom)
		require.Equal(t, customUser, *info.Username)
		require.Len(t, info.Roles, 1)
		require.Equal(t, testingRole, *info.Roles[0].Name)
		require.Len(t, info.Groups, 1)
	})
}
