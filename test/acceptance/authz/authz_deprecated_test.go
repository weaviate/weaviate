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

package authz

import (
	"context"
	"testing"

	"github.com/weaviate/weaviate/test/docker"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestDeprecatedEndpoints(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	clientAuth := helper.CreateAuth(adminKey)

	customUser := "custom-user"
	customKey := "custom-key"

	ctx := context.Background()

	// enable OIDC to be able to assign to db and oidc separately
	compose, err := docker.New().
		WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots(adminUser).
		WithApiKey().WithUserApiKey(customUser, customKey).WithUserApiKey(adminUser, adminKey).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	testRoleName := "testRole1"
	createCollectionsAction := authorization.CreateCollections
	all := "*"

	testRole1 := &models.Role{
		Name: &testRoleName,
		Permissions: []*models.Permission{{
			Action:      &createCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &all},
		}},
	}

	// assign without usertype should assign to OIDC as well as db user
	t.Run("assign role to user", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName)
		helper.CreateRole(t, adminKey, testRole1)
		defer helper.DeleteRole(t, adminKey, testRoleName)

		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{testRoleName}}),
			clientAuth,
		)
		require.NoError(t, err)

		RolesDbUser := helper.GetRolesForUser(t, customUser, adminKey, true)
		require.Len(t, RolesDbUser, 1)
		require.Equal(t, testRoleName, *RolesDbUser[0].Name)
	})

	// revoke without usertype should revoke from, OIDC as well as db user
	t.Run("revoke role from user", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName)
		helper.CreateRole(t, adminKey, testRole1)
		defer helper.DeleteRole(t, adminKey, testRoleName)

		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)
		helper.AssignRoleToUserOIDC(t, adminKey, testRoleName, customUser)

		RolesDbUser := helper.GetRolesForUser(t, customUser, adminKey, true)
		require.Len(t, RolesDbUser, 1)
		require.Equal(t, testRoleName, *RolesDbUser[0].Name)

		RolesOIDCUser := helper.GetRolesForUserOIDC(t, customUser, adminKey)
		require.Len(t, RolesOIDCUser, 1)
		require.Equal(t, testRoleName, *RolesOIDCUser[0].Name)

		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{testRoleName}}),
			clientAuth,
		)
		require.NoError(t, err)
	})

	t.Run("get role for User and user for role", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName)
		helper.CreateRole(t, adminKey, testRole1)
		defer helper.DeleteRole(t, adminKey, testRoleName)

		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		resp, err := helper.Client(t).Authz.GetRolesForUserDeprecated(authz.NewGetRolesForUserDeprecatedParams().WithID(customUser), clientAuth)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		require.Equal(t, testRoleName, *resp.Payload[0].Name)

		res, err := helper.Client(t).Authz.GetUsersForRoleDeprecated(authz.NewGetUsersForRoleDeprecatedParams().WithID(testRoleName), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, customUser, res.Payload[0])

		// no duplicates after also assigning to OIDC
		helper.AssignRoleToUserOIDC(t, adminKey, testRoleName, customUser)
		resp, err = helper.Client(t).Authz.GetRolesForUserDeprecated(authz.NewGetRolesForUserDeprecatedParams().WithID(customUser), clientAuth)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		require.Equal(t, testRoleName, *resp.Payload[0].Name)

		res, err = helper.Client(t).Authz.GetUsersForRoleDeprecated(authz.NewGetUsersForRoleDeprecatedParams().WithID(testRoleName), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, customUser, res.Payload[0])

		// remove from DB user, OIDC still has role
		helper.RevokeRoleFromUser(t, adminKey, testRoleName, customUser)
		resp, err = helper.Client(t).Authz.GetRolesForUserDeprecated(authz.NewGetRolesForUserDeprecatedParams().WithID(customUser), clientAuth)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		require.Equal(t, testRoleName, *resp.Payload[0].Name)

		res, err = helper.Client(t).Authz.GetUsersForRoleDeprecated(authz.NewGetUsersForRoleDeprecatedParams().WithID(testRoleName), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, customUser, res.Payload[0])
	})
}
