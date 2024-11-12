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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAuthzRolesJourney(t *testing.T) {
	t.Parallel()

	existingUser := "existing-user"
	existingKey := "existing-key"
	existingRole := "admin"

	testRole := "test-role"
	testAction1 := "create_collections"
	testAction2 := "delete_collections"
	all := "*"

	clientAuth := helper.CreateAuth(existingKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().WithRbacUser(existingUser, existingKey, existingRole).Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("get all roles before create", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 3, len(res.Payload))
		// require.Equal(t, existingRole, *res.Payload[0].Name)
	})

	t.Run("create role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: &testRole,
				Permissions: []*models.Permission{{
					Action:     &testAction1,
					Collection: &all,
				}},
			}),
			clientAuth,
		)
		require.Nil(t, err)
	})

	t.Run("get all roles after create", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 4, len(res.Payload))
	})

	t.Run("get role by name", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRole), clientAuth)
		require.Nil(t, err)
		require.Equal(t, testRole, *res.Payload.Name)
		require.Equal(t, 1, len(res.Payload.Permissions))
		require.Equal(t, testAction1, *res.Payload.Permissions[0].Action)
	})

	t.Run("add permission to role", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name:        &testRole,
			Permissions: []*models.Permission{{Action: &testAction2, Collection: &all}},
		}), clientAuth)
		require.Nil(t, err)
	})

	t.Run("get role by name after adding permission", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRole), clientAuth)
		require.Nil(t, err)
		require.Equal(t, testRole, *res.Payload.Name)
		require.Equal(t, 2, len(res.Payload.Permissions))
		require.Equal(t, testAction1, *res.Payload.Permissions[0].Action)
		require.Equal(t, testAction2, *res.Payload.Permissions[1].Action)
	})

	t.Run("remove permission from role", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithBody(authz.RemovePermissionsBody{
			Name:        &testRole,
			Permissions: []*models.Permission{{Action: &testAction2, Collection: &all}},
		}), clientAuth)
		require.Nil(t, err)
	})

	t.Run("get role by name after removing permission", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRole), clientAuth)
		require.Nil(t, err)
		require.Equal(t, testRole, *res.Payload.Name)
		require.Equal(t, 1, len(res.Payload.Permissions))
		require.Equal(t, testAction1, *res.Payload.Permissions[0].Action)
	})

	t.Run("assign role to user", func(t *testing.T) {
		_, err = helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(existingUser).WithBody(authz.AssignRoleBody{Roles: []string{testRole}}),
			clientAuth,
		)
		require.Nil(t, err)
	})

	t.Run("get roles for user after assignment", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(existingUser), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 2, len(res.Payload))

		names := make([]string, 2)
		for i, role := range res.Payload {
			names[i] = *role.Name
		}
		sort.Strings(names)

		roles := []string{existingRole, testRole}
		sort.Strings(roles)

		require.Equal(t, roles, names)
	})

	t.Run("get users for role after assignment", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetUsersForRole(authz.NewGetUsersForRoleParams().WithID(testRole), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, existingUser, res.Payload[0])
	})

	t.Run("delete role by name", func(t *testing.T) {
		_, err = helper.Client(t).Authz.DeleteRole(authz.NewDeleteRoleParams().WithID(testRole), clientAuth)
		require.Nil(t, err)
	})

	t.Run("get roles for user after deletion", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(existingUser), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, existingRole, *res.Payload[0].Name)
	})

	t.Run("get all roles after delete", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 3, len(res.Payload))
	})

	t.Run("get non-existent role by name", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRole), clientAuth)
		require.NotNil(t, err)
		require.ErrorIs(t, err, authz.NewGetRoleNotFound())
	})

	t.Run("upsert role using add permissions", func(t *testing.T) {
		_, err = helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name:        String("upsert-role"),
			Permissions: []*models.Permission{{Action: &testAction1, Collection: &all}},
		}), clientAuth)
		require.Nil(t, err)
		res, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID("upsert-role"), clientAuth)
		require.Nil(t, err)
		require.Equal(t, "upsert-role", *res.Payload.Name)
		require.Equal(t, 1, len(res.Payload.Permissions))
		require.Equal(t, testAction1, *res.Payload.Permissions[0].Action)
	})

	t.Run("role deletion using remove permissions", func(t *testing.T) {
		_, err = helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithBody(authz.RemovePermissionsBody{
			Name:        String("upsert-role"),
			Permissions: []*models.Permission{{Action: &testAction1, Collection: &all}},
		}), clientAuth)
		require.Nil(t, err)
		_, err = helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID("upsert-role"), clientAuth)
		require.NotNil(t, err)
		require.ErrorIs(t, err, authz.NewGetRoleNotFound())
	})
}

func String(s string) *string {
	return &s
}
