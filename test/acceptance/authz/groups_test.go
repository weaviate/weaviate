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

	"github.com/weaviate/weaviate/client/authz"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzRolesForGroups(t *testing.T) {
	// adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	//_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	//defer down()

	helper.SetupClient("127.0.0.1:8081")

	all := "*"

	groupReadName := "groupRead"
	groupAssignName := "groupAssign"
	roleReadName := "roleRead"
	groupRead := &models.Role{
		Name: &groupReadName,
		Permissions: []*models.Permission{{
			Action: &authorization.ReadGroups,
			Groups: &models.PermissionGroups{Group: &all, GroupType: models.PermissionGroupsGroupTypeOidc},
		}},
	}
	groupAssign := &models.Role{
		Name: &groupAssignName,
		Permissions: []*models.Permission{{
			Action: &authorization.AssignAndRevokeGroups,
			Groups: &models.PermissionGroups{Group: &all, GroupType: models.PermissionGroupsGroupTypeOidc},
		}},
	}
	roleRead := &models.Role{
		Name: &roleReadName,
		Permissions: []*models.Permission{{
			Action: &authorization.ReadRoles,
			Roles:  &models.PermissionRoles{Role: &all},
		}},
	}

	helper.DeleteRole(t, adminKey, groupReadName)
	helper.DeleteRole(t, adminKey, groupAssignName)
	helper.DeleteRole(t, adminKey, roleReadName)
	helper.CreateRole(t, adminKey, groupRead)
	helper.CreateRole(t, adminKey, groupAssign)
	helper.CreateRole(t, adminKey, roleRead)
	defer helper.DeleteRole(t, adminKey, groupReadName)
	defer helper.DeleteRole(t, adminKey, groupAssignName)
	defer helper.DeleteRole(t, adminKey, roleReadName)

	t.Run("test returns", func(t *testing.T) {
		readRole := helper.GetRoleByName(t, adminKey, groupReadName)
		require.NotNil(t, readRole)
		require.Equal(t, groupReadName, *readRole.Name)
		require.Len(t, readRole.Permissions, 1)
		require.Equal(t, all, *readRole.Permissions[0].Groups.Group)
		require.Equal(t, authorization.ReadGroups, *readRole.Permissions[0].Action)

		assignRole := helper.GetRoleByName(t, adminKey, groupAssignName)
		require.NotNil(t, assignRole)
		require.Equal(t, groupAssignName, *assignRole.Name)
		require.Len(t, assignRole.Permissions, 1)
		require.Equal(t, all, *assignRole.Permissions[0].Groups.Group)
		require.Equal(t, authorization.AssignAndRevokeGroups, *assignRole.Permissions[0].Action)
	})

	t.Run("assign group", func(t *testing.T) {
		group := "some-group"
		_, err := helper.Client(t).Authz.AssignRoleToGroup(
			authz.NewAssignRoleToGroupParams().WithID(group).WithBody(authz.AssignRoleToGroupBody{GroupType: models.PermissionGroupsGroupTypeOidc, Roles: []string{groupReadName}}),
			helper.CreateAuth(customKey),
		)
		require.Error(t, err)
		var errType *authz.AssignRoleToGroupForbidden
		require.True(t, errors.As(err, &errType))

		helper.AssignRoleToUser(t, adminKey, groupAssignName, customUser)

		// assigning works after user has appropriate rights
		helper.AssignRoleToGroup(t, customKey, groupReadName, group)
		groupRoles := helper.GetRolesForGroup(t, adminKey, group, false)
		require.Len(t, groupRoles, 1)
		require.Equal(t, groupReadName, *groupRoles[0].Name)

		helper.RevokeRoleFromUser(t, adminKey, groupReadName, customUser)
		helper.RevokeRoleFromUser(t, adminKey, groupAssignName, customUser)
	})

	t.Run("revoke group", func(t *testing.T) {
		group := "revoke-group"

		helper.AssignRoleToGroup(t, adminKey, groupReadName, group)

		_, err := helper.Client(t).Authz.RevokeRoleFromGroup(
			authz.NewRevokeRoleFromGroupParams().WithID(group).WithBody(authz.RevokeRoleFromGroupBody{GroupType: models.PermissionGroupsGroupTypeOidc, Roles: []string{groupReadName}}),
			helper.CreateAuth(customKey),
		)
		require.Error(t, err)
		var errType *authz.RevokeRoleFromGroupForbidden
		require.True(t, errors.As(err, &errType))

		helper.AssignRoleToUser(t, adminKey, groupAssignName, customUser)

		// revoking works after user has appropriate rights
		helper.RevokeRoleFromGroup(t, customKey, groupReadName, group)
		groupRoles := helper.GetRolesForGroup(t, adminKey, group, false)
		require.Len(t, groupRoles, 0)

		helper.RevokeRoleFromUser(t, adminKey, groupAssignName, customUser)
	})

	t.Run("get role for group", func(t *testing.T) {
		group := "revoke-group"

		helper.AssignRoleToGroup(t, adminKey, groupReadName, group)
		helper.AssignRoleToGroup(t, adminKey, groupAssignName, group)

		_, err := helper.Client(t).Authz.GetRolesForGroup(
			authz.NewGetRolesForGroupParams().WithID(group).WithGroupType(models.PermissionGroupsGroupTypeOidc),
			helper.CreateAuth(customKey),
		)
		require.Error(t, err)
		var errType *authz.GetRolesForGroupForbidden
		require.True(t, errors.As(err, &errType))

		helper.AssignRoleToUser(t, adminKey, groupReadName, customUser)
		roles := helper.GetRolesForGroup(t, adminKey, group, false)
		require.Len(t, roles, 2)

		// get roles for groups
		truep := true
		_, err = helper.Client(t).Authz.GetRolesForGroup(
			authz.NewGetRolesForGroupParams().WithID(group).WithGroupType(models.PermissionGroupsGroupTypeOidc).WithIncludeFullRoles(&truep),
			helper.CreateAuth(customKey),
		)
		require.Error(t, err)
		require.True(t, errors.As(err, &errType))

		helper.AssignRoleToUser(t, adminKey, roleReadName, customUser)
		roles = helper.GetRolesForGroup(t, adminKey, group, true)
		require.Len(t, roles, 2)
		require.NotNil(t, roles[0].Permissions)

		helper.RevokeRoleFromUser(t, adminKey, groupReadName, customUser)
		helper.RevokeRoleFromUser(t, adminKey, roleReadName, customUser)
	})

	t.Run("list all known groups", func(t *testing.T) {
		group1 := "group1"
		group2 := "group2"

		helper.AssignRoleToGroup(t, adminKey, groupReadName, group1)
		helper.AssignRoleToGroup(t, adminKey, groupAssignName, group2)

		groups := helper.GetKnownGroups(t, adminKey)
		require.Len(t, groups, 2)
	})
}
