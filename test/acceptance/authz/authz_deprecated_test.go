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
	"testing"

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

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	testRoleName1 := "testRole1"
	createCollectionsAction := authorization.CreateCollections
	all := "*"

	testRole1 := &models.Role{
		Name: &testRoleName1,
		Permissions: []*models.Permission{{
			Action:      &createCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &all},
		}},
	}

	// assign without usertype should assign to OIDC as well as db user
	t.Run("assign role to user", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName1)
		helper.CreateRole(t, adminKey, testRole1)
		defer helper.DeleteRole(t, adminKey, testRoleName1)

		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{testRoleName1}}),
			clientAuth,
		)
		require.NoError(t, err)

		RolesDbUser := helper.GetRolesForUser(t, customUser, adminKey)
		require.Len(t, RolesDbUser, 1)
		require.Equal(t, testRoleName1, *RolesDbUser[0].Name)

		RolesOIDCUser := helper.GetRolesForUserOIDC(t, customUser, adminKey)
		require.Len(t, RolesOIDCUser, 1)
		require.Equal(t, testRoleName1, *RolesOIDCUser[0].Name)
	})

	// revoke without usertype should revoke from, OIDC as well as db user
	t.Run("revoke role from user", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName1)
		helper.CreateRole(t, adminKey, testRole1)
		defer helper.DeleteRole(t, adminKey, testRoleName1)

		helper.AssignRoleToUser(t, adminKey, testRoleName1, customUser)
		helper.AssignRoleToUserOIDC(t, adminKey, testRoleName1, customUser)

		RolesDbUser := helper.GetRolesForUser(t, customUser, adminKey)
		require.Len(t, RolesDbUser, 1)
		require.Equal(t, testRoleName1, *RolesDbUser[0].Name)

		RolesOIDCUser := helper.GetRolesForUserOIDC(t, customUser, adminKey)
		require.Len(t, RolesOIDCUser, 1)
		require.Equal(t, testRoleName1, *RolesOIDCUser[0].Name)

		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{testRoleName1}}),
			clientAuth,
		)
		require.NoError(t, err)
	})

	t.Run("get role for User", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName1)
		helper.CreateRole(t, adminKey, testRole1)
		defer helper.DeleteRole(t, adminKey, testRoleName1)

		helper.AssignRoleToUser(t, adminKey, testRoleName1, customUser)

		resp, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(customUser), clientAuth)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		require.Equal(t, testRoleName1, *resp.Payload[0].Name)

		// no duplicates after also assigning to OIDC
		helper.AssignRoleToUserOIDC(t, adminKey, testRoleName1, customUser)
		resp, err = helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(customUser), clientAuth)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		require.Equal(t, testRoleName1, *resp.Payload[0].Name)

		// remove from DB user, OIDC still has role
		helper.RevokeRoleFromUser(t, adminKey, testRoleName1, customUser)
		resp, err = helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(customUser), clientAuth)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		require.Equal(t, testRoleName1, *resp.Payload[0].Name)
	})
}
