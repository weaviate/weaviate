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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/weaviate/weaviate/client/meta"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/test/docker"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/client/users"

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
		roles := helper.GetRolesForUser(t, adminUser, adminKey, false)
		require.Equal(t, 1, len(roles))
	})

	t.Run("get empty roles for existing user without role", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, customUser, adminKey, false)
		require.Equal(t, 0, len(roles))
	})

	t.Run("get roles for non existing user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID("notExists").WithUserType(string(models.UserTypeInputDb)), helper.CreateAuth(adminKey))
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

		roles := helper.GetRolesForUser(t, similar, adminKey, true)
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
		roles := helper.GetRolesForUser(t, customUser, customKey, true)
		require.Len(t, roles, 3)
		helper.RevokeRoleFromUser(t, customKey, otherRoleName, customUser)
		roles = helper.GetRolesForUser(t, customUser, customKey, true)
		require.Len(t, roles, 2)

		helper.RevokeRoleFromUser(t, adminKey, roleNameUpdate, customUser)
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadRoles, customUser)
	})
}

func TestReadUserPermissions(t *testing.T) {
	// adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	secondUser := "viewer-user"
	secondKey := "viewer-key"

	//_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey, secondUser: secondKey}, nil)
	//defer down()

	helper.SetupClient("127.0.0.1:8081")

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
		roles := helper.GetRolesForUser(t, secondUser, adminKey, true)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)
	})

	t.Run("user can return roles for themselves", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, secondUser, secondKey, true)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)
	})

	t.Run("user cannot return roles for other user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(secondUser).WithUserType(string(models.UserTypeInputDb)), helper.CreateAuth(customKey))
		require.Error(t, err)
		var errType *authz.GetRolesForUserForbidden
		require.True(t, errors.As(err, &errType))
	})

	t.Run("add permission", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, roleNameReadUsers, customUser)
		helper.AssignRoleToUser(t, adminKey, roleNameReadRoles, customUser)
		roles := helper.GetRolesForUser(t, secondUser, customKey, false)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)

		helper.RevokeRoleFromUser(t, adminKey, roleNameReadUsers, customUser)
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadRoles, customUser)
	})

	t.Run("check returns", func(t *testing.T) {
		helper.RevokeRoleFromUser(t, adminKey, roleNameReadUsers, customUser)
		helper.AssignRoleToUser(t, adminKey, roleNameReadUsers, customUser)
		roles := helper.GetRolesForUser(t, customUser, customKey, true)
		require.NotNil(t, roles)
		require.Len(t, roles, 1)
		require.Len(t, roles[0].Permissions, 1)

		require.Equal(t, secondUser, *roles[0].Permissions[0].Users.Users)
		require.Equal(t, readUserAction, *roles[0].Permissions[0].Action)

		helper.RevokeRoleFromUser(t, adminKey, roleNameReadUsers, customUser)
	})
}

func TestUserEndpoint(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"
	customUser := "custom-user"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, "customKey").WithDbUsers().
		WithRBAC().WithRbacRoots(adminUser).Start(ctx)
	require.Nil(t, err)

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()
	helper.SetupClient(compose.GetWeaviate().URI())

	testUser := "test-user"
	helper.DeleteUser(t, testUser, adminKey)
	testKey := helper.CreateUser(t, testUser, adminKey)

	// create roles for later
	readUserAction := authorization.ReadUsers
	createUsersAction := authorization.CreateUsers
	updateUsersAction := authorization.UpdateUsers
	deleteUsersAction := authorization.DeleteUsers

	all := "*"
	readUserRoleName := "userRead"
	createUserRoleName := "userCreate"
	updateUserRoleName := "userUpdate"
	deleteUserRoleName := "userDel"

	createUserRole := &models.Role{
		Name: &createUserRoleName,
		Permissions: []*models.Permission{{
			Action: &createUsersAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}
	readUserRole := &models.Role{
		Name: &readUserRoleName,
		Permissions: []*models.Permission{{
			Action: &readUserAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}
	updateUserRole := &models.Role{
		Name: &updateUserRoleName,
		Permissions: []*models.Permission{{
			Action: &updateUsersAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}
	deleteUserRole := &models.Role{
		Name: &deleteUserRoleName,
		Permissions: []*models.Permission{{
			Action: &deleteUsersAction,
			Users:  &models.PermissionUsers{Users: &all},
		}},
	}

	roles := []*models.Role{deleteUserRole, createUserRole, updateUserRole, readUserRole}
	for _, role := range roles {
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
	}
	defer func() {
		for _, role := range roles {
			helper.DeleteRole(t, adminKey, *role.Name)
		}
	}()

	t.Run("Create User", func(t *testing.T) {
		otherTestUser := "otherTestUser"
		defer helper.DeleteUser(t, otherTestUser, adminKey)
		_, err := helper.Client(t).Users.CreateUser(users.NewCreateUserParams().WithUserID(otherTestUser), helper.CreateAuth(testKey))
		require.Error(t, err)
		var createUserForbidden *users.CreateUserForbidden
		ok := errors.As(err, &createUserForbidden)
		assert.True(t, ok)

		helper.AssignRoleToUser(t, adminKey, createUserRoleName, testUser)
		defer helper.RevokeRoleFromUser(t, adminKey, createUserRoleName, testUser)

		otherTestUserApiKey := helper.CreateUser(t, otherTestUser, testKey)
		require.Greater(t, len(otherTestUserApiKey), 10)
	})

	t.Run("Read User", func(t *testing.T) {
		start := time.Now()
		otherTestUserName := "otherTestUser"
		helper.DeleteUser(t, otherTestUserName, adminKey)
		defer helper.DeleteUser(t, otherTestUserName, adminKey)
		helper.CreateUser(t, otherTestUserName, adminKey)

		_, err := helper.Client(t).Users.GetUserInfo(users.NewGetUserInfoParams().WithUserID(otherTestUserName), helper.CreateAuth(testKey))
		require.Error(t, err)
		var getUserForbidden *users.GetUserInfoForbidden
		ok := errors.As(err, &getUserForbidden)
		assert.True(t, ok)

		helper.AssignRoleToUser(t, adminKey, readUserRoleName, testUser)
		defer helper.RevokeRoleFromUser(t, adminKey, readUserRoleName, testUser)

		otherTestUser := helper.GetUser(t, otherTestUserName, testKey)
		require.Equal(t, *otherTestUser.UserID, otherTestUserName)
		require.Less(t, strfmt.DateTime(start), otherTestUser.CreatedAt)
		require.Less(t, otherTestUser.CreatedAt, strfmt.DateTime(time.Now()))
	})

	t.Run("Get static user", func(t *testing.T) {
		staticUser := helper.GetUser(t, customUser, adminKey)
		require.Equal(t, *staticUser.UserID, customUser)
		require.Less(t, staticUser.CreatedAt, strfmt.DateTime(time.Now().Add(-1000*time.Hour))) // static user have minimum time
	})

	t.Run("Update (rotate, Deactivate, activate) user", func(t *testing.T) {
		otherTestUser := "otherTestUser"
		helper.DeleteUser(t, otherTestUser, adminKey)
		defer helper.DeleteUser(t, otherTestUser, adminKey)
		apiKey := helper.CreateUser(t, otherTestUser, adminKey)

		// rotate, Deactivate and activate are all update
		_, err := helper.Client(t).Users.RotateUserAPIKey(users.NewRotateUserAPIKeyParams().WithUserID(otherTestUser), helper.CreateAuth(testKey))
		require.Error(t, err)
		var rotateUserForbidden *users.RotateUserAPIKeyForbidden
		assert.True(t, errors.As(err, &rotateUserForbidden))

		_, err = helper.Client(t).Users.DeactivateUser(users.NewDeactivateUserParams().WithUserID(otherTestUser), helper.CreateAuth(testKey))
		require.Error(t, err)
		var DeactivateUserForbidden *users.DeactivateUserForbidden
		assert.True(t, errors.As(err, &DeactivateUserForbidden))

		_, err = helper.Client(t).Users.ActivateUser(users.NewActivateUserParams().WithUserID(otherTestUser), helper.CreateAuth(testKey))
		require.Error(t, err)
		var activateUserForbidden *users.ActivateUserForbidden
		assert.True(t, errors.As(err, &activateUserForbidden))

		helper.AssignRoleToUser(t, adminKey, updateUserRoleName, testUser)
		defer helper.RevokeRoleFromUser(t, adminKey, updateUserRoleName, testUser)

		// with update role all three operations work
		otherTestUserApiKey := helper.RotateKey(t, otherTestUser, testKey)
		require.Greater(t, len(otherTestUserApiKey), 10)

		// key is not valid anymore
		_, err = helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(apiKey))
		var ownInfoUnauthorized *users.GetOwnInfoUnauthorized
		assert.True(t, errors.As(err, &ownInfoUnauthorized))

		helper.DeactivateUser(t, testKey, otherTestUser, false)
		helper.ActivateUser(t, testKey, otherTestUser)
	})

	t.Run("Delete user", func(t *testing.T) {
		otherTestUser := "otherTestUser"
		helper.DeleteUser(t, otherTestUser, adminKey)
		defer helper.DeleteUser(t, otherTestUser, adminKey)
		helper.CreateUser(t, otherTestUser, adminKey)

		_, err := helper.Client(t).Users.DeleteUser(users.NewDeleteUserParams().WithUserID(otherTestUser), helper.CreateAuth(testKey))
		require.Error(t, err)
		var createUserForbidden *users.DeleteUserForbidden
		assert.True(t, errors.As(err, &createUserForbidden))

		helper.AssignRoleToUser(t, adminKey, deleteUserRoleName, testUser)
		defer helper.RevokeRoleFromUser(t, adminKey, deleteUserRoleName, testUser)

		helper.DeleteUser(t, otherTestUser, testKey)
		// user does not exist after deleting
		resp, err := helper.Client(t).Users.GetUserInfo(users.NewGetUserInfoParams().WithUserID(otherTestUser), helper.CreateAuth(adminKey))
		require.Nil(t, resp)
		require.Error(t, err)
		var getUserNotFound *users.GetUserInfoNotFound
		assert.True(t, errors.As(err, &getUserNotFound))
	})

	t.Run("delete user revokes roles", func(t *testing.T) {
		testUserName := "DeleteUserTestUser"
		helper.DeleteUser(t, testUserName, adminKey)

		// create user and assign roles
		helper.CreateUser(t, testUserName, adminKey)
		helper.AssignRoleToUser(t, adminKey, deleteUserRoleName, testUserName)
		rolesRet := helper.GetRolesForUser(t, testUserName, adminKey, false)
		require.Len(t, rolesRet, 1)

		// delete user and recreate with same name => role assignment should be gone
		helper.DeleteUser(t, testUserName, adminKey)
		helper.CreateUser(t, testUserName, adminKey)
		rolesRet = helper.GetRolesForUser(t, testUserName, adminKey, false)
		require.Len(t, rolesRet, 0)
	})
}

func TestDynamicUsers(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"

	customUser := "custom-user"
	customKey := "custom-key"

	viewerUser := "viewer-user"
	viewerKey := "viewer-key"

	// match what is defined in the docker-compose file to allow switching between them
	staticUsers := map[string]string{customUser: customKey, viewerUser: viewerKey, "editor-user": "editor-key"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).WithUserApiKey(viewerUser, viewerKey).WithUserApiKey("editor-user", "editor-key").
		WithRBAC().WithRbacRoots(adminUser).
		WithDbUsers().Start(ctx)
	require.Nil(t, err)

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()
	helper.SetupClient(compose.GetWeaviate().URI())

	helper.AssignRoleToUser(t, adminKey, "viewer", viewerUser)
	t.Run("List all users", func(t *testing.T) {
		start := time.Now()
		userNames := make([]string, 0, 10)
		for i := 0; i < cap(userNames); i++ {
			userNames = append(userNames, fmt.Sprintf("user-%d", i))
		}

		apiKeys := make([]string, 0, len(userNames))
		for i, userName := range userNames {
			helper.DeleteUser(t, userName, adminKey)
			apiKey := helper.CreateUser(t, userName, adminKey)
			apiKeys = append(apiKeys, apiKey)
			defer helper.DeleteUser(t, userName, adminKey) // runs at end of test function to clear everything
			if i%2 == 0 {
				helper.AssignRoleToUser(t, adminKey, "viewer", userName)
			}
			if i%5 == 0 {
				helper.DeactivateUser(t, adminKey, userName, false)
			}
		}

		allUsersAdmin := helper.ListAllUsers(t, adminKey)
		require.Len(t, allUsersAdmin, len(userNames)+len(staticUsers)+1)

		for _, user := range allUsersAdmin {
			name := *user.UserID

			if *user.DbUserType == models.DBUserInfoDbUserTypeDbEnvUser {
				require.Less(t, user.CreatedAt, strfmt.DateTime(start)) // minimum time for static users
				continue
			}

			number, err := strconv.Atoi(strings.Split(name, "-")[1])
			require.NoError(t, err)
			if number%2 == 0 {
				require.Len(t, user.Roles, 1)
				require.Equal(t, user.Roles[0], "viewer")
			}

			require.Equal(t, number%5 != 0, *user.Active)
			require.Less(t, strfmt.DateTime(start), user.CreatedAt)
			require.Less(t, user.CreatedAt, strfmt.DateTime(time.Now()))
			require.Len(t, user.APIKeyFirstLetters, 3)
			require.Equal(t, user.APIKeyFirstLetters, apiKeys[number][:3])
		}

		allUsersViewer := helper.ListAllUsers(t, viewerKey)
		require.Len(t, allUsersViewer, len(userNames))
	})

	t.Run("List all users using non-admin", func(t *testing.T) {
		userNames := make([]string, 0, 10)
		for i := 0; i < cap(userNames); i++ {
			userNames = append(userNames, fmt.Sprintf("user-%d", i))
		}

		for i, userName := range userNames {
			helper.DeleteUser(t, userName, adminKey)
			helper.CreateUser(t, userName, adminKey)
			defer helper.DeleteUser(t, userName, adminKey) // runs at end of test function to clear everything
			if i%2 == 0 {
				helper.AssignRoleToUser(t, adminKey, "viewer", userName)
			}
			if i%5 == 0 {
				helper.DeactivateUser(t, adminKey, userName, false)
			}
		}

		allUsers := helper.ListAllUsers(t, adminKey)
		require.Len(t, allUsers, len(userNames)+len(staticUsers)+1)

		for _, user := range allUsers {
			name := *user.UserID

			if *user.DbUserType == models.DBUserInfoDbUserTypeDbEnvUser {
				continue
			}

			number, err := strconv.Atoi(strings.Split(name, "-")[1])
			require.NoError(t, err)
			if number%2 == 0 {
				require.Len(t, user.Roles, 1)
				require.Equal(t, user.Roles[0], "viewer")
			}

			require.Equal(t, number%5 != 0, *user.Active)
		}
	})

	t.Run("filtered list users", func(t *testing.T) {
		length := 10
		userNames := make([]string, 0, length)
		for i := 0; i < length; i++ {
			var userName string
			if i%2 == 0 {
				userName = fmt.Sprintf("finance-user-%d", i)
			} else {
				userName = fmt.Sprintf("sales-user-%d", i)
			}

			userNames = append(userNames, userName)
		}
		for _, userName := range userNames {
			helper.DeleteUser(t, userName, adminKey)
			helper.CreateUser(t, userName, adminKey)
			defer helper.DeleteUser(t, userName, adminKey) // runs at end of test function to clear everything
		}

		// create role that can only view finance users
		readUserAction := authorization.ReadUsers

		finance := "finance-*"
		readUserRoleName := "userRead"

		readUserRole := &models.Role{
			Name: &readUserRoleName,
			Permissions: []*models.Permission{{
				Action: &readUserAction,
				Users:  &models.PermissionUsers{Users: &finance},
			}},
		}
		financeUserViewer := "test-finance-user-viewer"
		helper.DeleteUser(t, financeUserViewer, adminKey)
		apiKey := helper.CreateUser(t, financeUserViewer, adminKey)
		defer helper.DeleteUser(t, financeUserViewer, adminKey)
		helper.DeleteRole(t, adminKey, readUserRoleName)
		helper.CreateRole(t, adminKey, readUserRole)
		defer helper.DeleteRole(t, adminKey, readUserRoleName)

		helper.AssignRoleToUser(t, adminKey, readUserRoleName, financeUserViewer)

		filteredUsers := helper.ListAllUsers(t, apiKey)
		require.Len(t, filteredUsers, length/2)
	})

	t.Run("import static user and check roles", func(t *testing.T) {
		// add a role to ensure it is present after import
		roleName := "testRole"
		testRole := &models.Role{Name: &roleName, Permissions: []*models.Permission{{Action: &authorization.ReadUsers, Users: &models.PermissionUsers{Users: &roleName}}}}
		helper.DeleteRole(t, adminKey, roleName)
		helper.CreateRole(t, adminKey, testRole)
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		roles := helper.GetRolesForUser(t, customUser, adminKey, false)
		require.Len(t, roles, 1)
		require.Equal(t, *testRole.Name, *roles[0].Name)

		oldKey := helper.CreateUserWithApiKey(t, customUser, adminKey, nil)
		require.Equal(t, oldKey, customKey)

		info := helper.GetInfoForOwnUser(t, oldKey)
		require.Equal(t, customUser, *info.Username)

		rolesAfterImport := helper.GetRolesForUser(t, customUser, adminKey, false)
		require.Len(t, rolesAfterImport, 1)
		require.Equal(t, *testRole.Name, *rolesAfterImport[0].Name)

		helper.DeleteUser(t, customUser, adminKey)
	})
}

func TestUserPermissionReturns(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	all := "*"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{}, nil)
	defer down()

	roleName := "testingUserPermissionReturns"
	defer helper.DeleteRole(t, adminKey, roleName)
	for _, action := range []string{authorization.ReadUsers, authorization.CreateUsers, authorization.UpdateUsers, authorization.DeleteUsers, authorization.AssignAndRevokeUsers} {
		helper.DeleteRole(t, adminKey, roleName)

		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{{
				Action: &action,
				Users:  &models.PermissionUsers{Users: &all},
			}},
		}

		helper.CreateRole(t, adminKey, role)
		roleRet := helper.GetRoleByName(t, adminKey, roleName)
		require.NotNil(t, roleRet)
		require.Equal(t, *roleRet.Permissions[0].Users.Users, all)
		require.Equal(t, *roleRet.Permissions[0].Action, action)
	}
}

func TestGetLastUsageMultinode(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	ctx := context.Background()
	compose, err := docker.New().
		With3NodeCluster().WithApiKey().WithUserApiKey(adminUser, adminKey).WithDbUsers().
		Start(ctx)

	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	t.Run("get last usage multinode", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())

		dynUser := "dyn-user"
		helper.DeleteUser(t, dynUser, adminKey)
		defer helper.DeleteUser(t, dynUser, adminKey)
		apiKey := helper.CreateUser(t, dynUser, adminKey)

		time.Sleep(time.Millisecond * 100) // sometimes takes a little bit until a user has been propagated to all nodes
		before := time.Now()

		info := helper.GetInfoForOwnUser(t, apiKey)
		require.Equal(t, *info.Username, dynUser)

		user := helper.GetUserWithLastUsedTime(t, dynUser, adminKey, true)
		require.Equal(t, *user.UserID, dynUser)
		require.Less(t, before, user.LastUsedAt)
		require.Less(t, user.LastUsedAt, time.Now())

		lastLoginTime := user.LastUsedAt
		// make request to other node and check that login time has been update in first node
		helper.SetupClient(compose.GetWeaviateNode2().URI())

		require.Equal(t, *helper.GetInfoForOwnUser(t, apiKey).Username, dynUser)

		helper.SetupClient(compose.GetWeaviateNode3().URI())

		user = helper.GetUserWithLastUsedTime(t, dynUser, adminKey, true)
		require.Equal(t, *user.UserID, dynUser)
		require.Less(t, lastLoginTime, user.LastUsedAt)
		require.Less(t, user.LastUsedAt, time.Now())

		allUsers := helper.ListAllUsersWithIncludeTime(t, adminKey, true)
		for _, user := range allUsers {
			if *user.UserID != dynUser {
				continue
			}
			require.Less(t, lastLoginTime, user.LastUsedAt)
			require.Less(t, user.LastUsedAt, time.Now())
		}
	})

	t.Run("last usage with shutdowns", func(t *testing.T) {
		firstNode := compose.GetWeaviateNode(2)
		secondNode := compose.GetWeaviateNode(1)
		helper.SetupClient(firstNode.URI())

		dynUser := "dyn-user"
		helper.DeleteUser(t, dynUser, adminKey)
		defer helper.DeleteUser(t, dynUser, adminKey)
		apiKey := helper.CreateUser(t, dynUser, adminKey)

		time.Sleep(time.Millisecond * 100) // sometimes takes a little bit until a user has been propagated to all nodes
		before := time.Now()

		info := helper.GetInfoForOwnUser(t, apiKey)
		require.Equal(t, *info.Username, dynUser)

		user := helper.GetUserWithLastUsedTime(t, dynUser, adminKey, true)
		require.Equal(t, *user.UserID, dynUser)
		require.Less(t, before, user.LastUsedAt)
		require.Less(t, user.LastUsedAt, time.Now())

		// shutdown node, its login time should be transferred to other nodes
		timeout := time.Minute
		err := firstNode.Container().Stop(ctx, &timeout)
		require.NoError(t, err)

		// wait to make sure that node is gone
		start := time.Now()
		for time.Since(start) < timeout {
			_, err = helper.Client(t).Meta.MetaGet(meta.NewMetaGetParams(), nil)
			if err != nil {
				break
			}
			time.Sleep(time.Second)
		}
		time.Sleep(time.Second * 5) // wait to make sure that node is gone
		_, err = helper.Client(t).Meta.MetaGet(meta.NewMetaGetParams(), nil)
		require.Error(t, err)

		helper.ResetClient()
		helper.SetupClient(secondNode.URI())

		userNode2 := helper.GetUserWithLastUsedTime(t, dynUser, adminKey, true)
		require.Equal(t, *user.UserID, dynUser)
		require.Less(t, before, userNode2.LastUsedAt)
		require.Less(t, userNode2.LastUsedAt, time.Now())
		require.Equal(t, userNode2.LastUsedAt, user.LastUsedAt)

		allUsers := helper.ListAllUsersWithIncludeTime(t, adminKey, true)
		for _, user := range allUsers {
			if *user.UserID != dynUser {
				continue
			}
			require.Less(t, user.LastUsedAt, time.Now())
			require.Equal(t, user.LastUsedAt, userNode2.LastUsedAt)
		}
	})
}

func TestStaticUserImport(t *testing.T) {
	rootKey := "root-key"
	rootUser := "root-user"

	readOnlyUser := "readOnly-user"
	readOnlyKey := "readOnly-key"

	adminUser := "admin-user"
	adminKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().
		WithApiKey().WithUserApiKey(rootUser, rootKey).WithUserApiKey(readOnlyUser, readOnlyKey).WithUserApiKey(adminUser, adminKey).
		WithRBAC().WithRbacRoots(rootUser).
		WithWeaviateEnv("EXPERIMENTAL_AUTHORIZATION_RBAC_READONLY_USERS", "readOnly-user").
		WithWeaviateEnv("EXPERIMENTAL_AUTHORIZATION_RBAC_ADMIN_USERS", "admin-user").
		WithDbUsers().Start(ctx)
	require.Nil(t, err)

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()
	helper.SetupClient(compose.GetWeaviate().URI())

	keys := map[string]string{readOnlyUser: readOnlyKey, adminUser: adminKey}

	for userName, role := range map[string]string{readOnlyUser: "viewer", adminUser: "admin"} {
		t.Run("import static user and check roles for "+userName, func(t *testing.T) {
			roles := helper.GetRolesForUser(t, userName, rootKey, false)
			require.Len(t, roles, 1)
			require.Equal(t, role, *roles[0].Name)

			oldKey := helper.CreateUserWithApiKey(t, userName, rootKey, nil)
			require.Equal(t, oldKey, keys[userName])

			newKey := helper.RotateKey(t, userName, rootKey)
			_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(oldKey))
			require.Error(t, err)

			user := helper.GetUser(t, userName, rootKey)
			require.Equal(t, user.APIKeyFirstLetters, newKey[:3])
			require.NotEqual(t, newKey, oldKey)

			info := helper.GetInfoForOwnUser(t, newKey)
			require.Equal(t, userName, *info.Username)
			require.Len(t, info.Roles, 1)
			require.Equal(t, *info.Roles[0].Name, role)
		})
	}
}
