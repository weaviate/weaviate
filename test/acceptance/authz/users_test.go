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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzRolesForUsers(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	t.Run("all roles", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles, len(roles))
	})

	t.Run("role exists for admin", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, adminUser, adminKey)
		require.Equal(t, 1, len(roles))
	})

	t.Run("get empty roles for existing user without role", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, customUser, adminKey)
		require.Equal(t, 0, len(roles))
	})

	t.Run("get roles for non existing user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID("notExists"), helper.CreateAuth(adminKey))
		require.NotNil(t, err)
		var targetErr *authz.GetRolesForUserNotFound
		require.True(t, errors.As(err, &targetErr))
		require.Equal(t, 404, targetErr.Code())
	})
}

func TestAuthzRolesAndUserHaveTheSameName(t *testing.T) {
	adminUser := "admin"
	adminKey := "admin"
	similar := "similarRoleKeyUserName"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{similar: similar}, nil)
	defer down()

	t.Run("create role with the same name of the user", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(similar),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateCollections), Collections: &models.PermissionCollections{Collection: String("*")}},
			},
		})
	})

	t.Run("assign role to user", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, similar, similar)
	})

	t.Run("get role and user were they have the same name", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, similar)
		require.NotNil(t, role)
		require.Equal(t, similar, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateCollections, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collections.Collection)

		roles := helper.GetRolesForUser(t, similar, adminKey)
		require.Equal(t, 1, len(roles))
		require.NotNil(t, role)
		require.Equal(t, similar, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateCollections, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collections.Collection)
	})
}

func TestUserPermissions(t *testing.T) {
	// adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	//_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	//defer down()

	helper.SetupClient("127.0.0.1:8081")

	// create roles for later
	assignUserAction := authorization.AssignAndRevokeUsers
	readCollectionAction := authorization.ReadCollections
	readRolesAction := authorization.ReadRoles

	all := "*"
	roleNameUpdate := "userRoleCreate"
	otherRoleName := "collectionRead"
	roleNameReadRoles := "roleRead"

	userUpdateRole := &models.Role{
		Name: &roleNameUpdate,
		Permissions: []*models.Permission{{
			Action: &assignUserAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}
	roleReadRole := &models.Role{
		Name: &roleNameReadRoles,
		Permissions: []*models.Permission{{
			Action: &readRolesAction,
			Roles:  &models.PermissionRoles{Role: &all, Scope: String(models.PermissionRolesScopeAll)},
		}},
	}
	otherRole := &models.Role{
		Name: &otherRoleName,
		Permissions: []*models.Permission{{
			Action: &readCollectionAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}
	helper.DeleteRole(t, adminKey, roleNameUpdate)
	helper.DeleteRole(t, adminKey, otherRoleName)
	helper.DeleteRole(t, adminKey, roleNameReadRoles)
	helper.CreateRole(t, adminKey, userUpdateRole)
	helper.CreateRole(t, adminKey, otherRole)
	helper.CreateRole(t, adminKey, roleReadRole)

	t.Run("test returns", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, roleNameUpdate)
		require.NotNil(t, role)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, role.Permissions[0].Users.Users, &all)
	})

	t.Run("assign users", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{otherRoleName}}),
			helper.CreateAuth(customKey),
		)
		require.Error(t, err)
		var errType *authz.AssignRoleToUserForbidden
		require.True(t, errors.As(err, &errType))

		helper.AssignRoleToUser(t, adminKey, roleNameUpdate, customUser)
		helper.AssignRoleToUser(t, adminKey, roleNameReadRoles, customUser)

		// assigning works after user has appropriate rights
		helper.AssignRoleToUser(t, customKey, otherRoleName, customUser)

		// clean up
		helper.RevokeRoleFromUser(t, adminKey, roleNameUpdate, customUser)
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadRoles, customUser)
		helper.RevokeRoleFromUser(t, adminKey, otherRoleName, customUser)
	})

	t.Run("revoke users", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, otherRoleName, customUser)

		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{otherRoleName}}),
			helper.CreateAuth(customKey),
		)
		require.Error(t, err)
		var errType *authz.RevokeRoleFromUserForbidden
		require.True(t, errors.As(err, &errType))

		helper.AssignRoleToUser(t, adminKey, roleNameUpdate, customUser)
		helper.AssignRoleToUser(t, adminKey, roleNameReadRoles, customUser)

		// revoking works after user has appropriate rights
		require.Len(t, helper.GetRolesForUser(t, customUser, adminKey), 3)
		helper.RevokeRoleFromUser(t, customKey, otherRoleName, customUser)
		require.Len(t, helper.GetRolesForUser(t, customUser, adminKey), 2)

		helper.RevokeRoleFromUser(t, adminKey, roleNameUpdate, customUser)
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadRoles, customUser)
	})
}

func TestReadUserPermissions(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	secondUser := "viewer-user"
	secondKey := "viewer-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey, secondUser: secondKey}, nil)
	defer down()

	// create roles for later
	readUserAction := authorization.ReadUsers
	readRolesAction := authorization.ReadRoles

	all := "*"
	roleNameReadUsers := "userRead"
	otherRoleName := "otherName"
	roleNameReadRoles := "rolesRead"

	userReadRole := &models.Role{
		Name: &roleNameReadUsers,
		Permissions: []*models.Permission{{
			Action: &readUserAction,
			Users:  &models.PermissionUsers{Users: &secondUser},
		}},
	}
	roleReadRole := &models.Role{
		Name: &roleNameReadRoles,
		Permissions: []*models.Permission{{
			Action: &readRolesAction,
			Roles:  &models.PermissionRoles{Role: &all, Scope: String(models.PermissionRolesScopeAll)},
		}},
	}

	otherRole := &models.Role{
		Name: &otherRoleName,
		Permissions: []*models.Permission{{
			Action: &readUserAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}

	helper.DeleteRole(t, adminKey, roleNameReadUsers)
	helper.CreateRole(t, adminKey, userReadRole)
	helper.DeleteRole(t, adminKey, roleNameReadRoles)
	helper.CreateRole(t, adminKey, roleReadRole)
	helper.DeleteRole(t, adminKey, otherRoleName)
	helper.CreateRole(t, adminKey, otherRole)
	helper.AssignRoleToUser(t, adminKey, otherRoleName, secondUser)

	t.Run("admin can return roles", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, secondUser, adminKey)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)
	})

	t.Run("user can return roles for themselves", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, secondUser, secondKey)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)
	})

	t.Run("user cannot return roles for other user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(secondUser), helper.CreateAuth(customKey))
		require.Error(t, err)
		var errType *authz.GetRolesForUserForbidden
		require.True(t, errors.As(err, &errType))
	})

	t.Run("add permission", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, roleNameReadUsers, customUser)
		helper.AssignRoleToUser(t, adminKey, roleNameReadRoles, customUser)
		roles := helper.GetRolesForUser(t, secondUser, customKey)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)

		helper.RevokeRoleFromUser(t, adminKey, roleNameReadUsers, customUser)
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadRoles, customUser)
	})

	t.Run("check returns", func(t *testing.T) {
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadUsers, customUser)
		helper.AssignRoleToUser(t, adminKey, roleNameReadUsers, customUser)
		roles := helper.GetRolesForUser(t, customUser, customKey)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)
		require.Len(t, roles[0].Permissions, 1)

		require.Equal(t, secondUser, *roles[0].Permissions[0].Users.Users)
		require.Equal(t, readUserAction, *roles[0].Permissions[0].Action)

		helper.RevokeRoleFromUser(t, adminKey, roleNameReadUsers, customUser)
	})
}
