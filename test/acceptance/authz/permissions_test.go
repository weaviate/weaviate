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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzRolesWithPermissions(t *testing.T) {
	existingUser := "existing-user"
	existingKey := "existing-key"
	adminRole := "admin"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().WithRbacUser(existingUser, existingKey, adminRole).Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	testClass := &models.Class{
		Class:              "Foo",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
	}

	t.Run("create test collection before permissions", func(t *testing.T) {
		helper.CreateClassAuth(t, testClass, existingKey)
	})

	t.Run("create and get a role to create all collections", func(t *testing.T) {
		name := "create-all-collections"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateSchema), Collection: String("*")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateSchema, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collection)
	})

	t.Run("create and get a role to create all tenants in a collection", func(t *testing.T) {
		name := "create-all-tenants-in-foo"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateSchema), Collection: String(testClass.Class)},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateSchema, *role.Permissions[0].Action)
		require.Equal(t, testClass.Class, *role.Permissions[0].Collection)
		require.Equal(t, "*", *role.Permissions[0].Tenant)
	})

	t.Run("create and get a role to manage all roles", func(t *testing.T) {
		name := "manage-all-roles"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.ManageRoles), Role: String("*")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.ManageRoles, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Role)
	})

	t.Run("create and get a role to manage one role", func(t *testing.T) {
		name := "manage-one-role"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.ManageRoles), Role: String("foo")},
			},
		})
		require.Nil(t, err)
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.ManageRoles, *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Role)
	})

	t.Run("create and get a role to read two roles", func(t *testing.T) {
		name := "read-one-role"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String(authorization.ReadRoles), Role: String("foo")},
				{Action: String(authorization.ReadRoles), Role: String("bar")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 2)
		require.Equal(t, authorization.ReadRoles, *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Role)
		require.Equal(t, authorization.ReadRoles, *role.Permissions[1].Action)
		require.Equal(t, "bar", *role.Permissions[1].Role)
	})
}
