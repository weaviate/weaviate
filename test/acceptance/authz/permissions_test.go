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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzRolesWithPermissions(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, nil)
	defer down()

	testClass := &models.Class{
		Class:              "Foo",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
	}

	t.Run("create test collection before permissions", func(t *testing.T) {
		helper.CreateClassAuth(t, testClass, adminKey)
	})

	t.Run("create and get a role to create all collections", func(t *testing.T) {
		name := "create-all-collections"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateCollections), Collections: &models.PermissionCollections{Collection: String("*")}},
			},
		})
		role := helper.GetRoleByName(t, adminKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateCollections, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collections.Collection)
	})

	t.Run("create and get a role to create all tenants in a collection", func(t *testing.T) {
		name := "create-all-tenants-in-foo"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateCollections), Collections: &models.PermissionCollections{Collection: String(testClass.Class)}},
			},
		})
		role := helper.GetRoleByName(t, adminKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateCollections, *role.Permissions[0].Action)
		require.Equal(t, testClass.Class, *role.Permissions[0].Collections.Collection)
	})

	t.Run("create and get a role to create all roles", func(t *testing.T) {
		name := "manage-all-roles"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateRoles), Roles: &models.PermissionRoles{Role: String("*"), Scope: String(models.PermissionRolesScopeAll)}},
			},
		})
		role := helper.GetRoleByName(t, adminKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateRoles, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Roles.Role)
	})

	t.Run("create and get a role to create one role", func(t *testing.T) {
		name := "manage-one-role"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateRoles), Roles: &models.PermissionRoles{Role: String("foo"), Scope: String(models.PermissionRolesScopeAll)}},
			},
		})
		role := helper.GetRoleByName(t, adminKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateRoles, *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Roles.Role)
	})

	t.Run("create and get a role to read two roles", func(t *testing.T) {
		name := "read-one-role"
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.ReadRoles), Roles: &models.PermissionRoles{Role: String("foo"), Scope: String(models.PermissionRolesScopeAll)}},
				{Action: String(authorization.ReadRoles), Roles: &models.PermissionRoles{Role: String("bar"), Scope: String(models.PermissionRolesScopeAll)}},
			},
		})
		role := helper.GetRoleByName(t, adminKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 2)
		require.Equal(t, authorization.ReadRoles, *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Roles.Role)
		require.Equal(t, authorization.ReadRoles, *role.Permissions[1].Action)
		require.Equal(t, "bar", *role.Permissions[1].Roles.Role)
	})
}
