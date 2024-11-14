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

	t.Run("create and get a role to create all collections", func(t *testing.T) {
		name := "create-all-collections"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String("create_collections"), Collection: String("*")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, "create_collections", *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collection)
	})

	t.Run("create and get a role to create all tenants in a collection", func(t *testing.T) {
		name := "create-all-tenants-in-foo"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String("create_collections"), Collection: String("foo")},
				{Action: String("create_tenants"), Collection: String("*")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 2)
		require.Equal(t, "create_collections", *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Collection)
		require.Equal(t, "create_tenants", *role.Permissions[1].Action)
		require.Equal(t, "*", *role.Permissions[1].Collection)
	})

	t.Run("create and get a role to manage all roles", func(t *testing.T) {
		name := "manage-all-roles"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String("manage_roles"), Role: String("*")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, "manage_roles", *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Role)
	})

	t.Run("create and get a role to manage one role", func(t *testing.T) {
		name := "manage-one-role"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String("manage_roles"), Role: String("foo")},
			},
		})
		require.Nil(t, err)
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, "manage_roles", *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Role)
	})

	t.Run("create and get a role to read two roles", func(t *testing.T) {
		name := "read-one-role"
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(name),
			Permissions: []*models.Permission{
				{Action: String("read_roles"), Role: String("foo")},
				{Action: String("read_roles"), Role: String("bar")},
			},
		})
		role := helper.GetRoleByName(t, existingKey, name)
		require.NotNil(t, role)
		require.Equal(t, name, *role.Name)
		require.Len(t, role.Permissions, 2)
		require.Equal(t, "read_roles", *role.Permissions[0].Action)
		require.Equal(t, "foo", *role.Permissions[0].Role)
		require.Equal(t, "read_roles", *role.Permissions[1].Action)
		require.Equal(t, "bar", *role.Permissions[1].Role)
	})
}
