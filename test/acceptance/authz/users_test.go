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
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzRolesForUsers(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	anotherUser := "another-user"
	anotherKey := "another-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(anotherUser, anotherKey).
		WithRBAC().WithRbacAdmins(adminUser).
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("all roles", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, 3, len(roles))
	})

	t.Run("role exists for admin", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, adminUser, adminKey)
		require.Equal(t, 1, len(roles))
	})

	t.Run("get empty roles for existing user without role", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, anotherUser, adminKey)
		require.Equal(t, 0, len(roles))
	})

	t.Run("get roles for non existing user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID("notExists"), helper.CreateAuth(adminKey))
		require.NotNil(t, err)
		targetErr, ok := err.(*authz.GetRolesForUserInternalServerError)
		require.True(t, ok)

		require.Equal(t, 500, targetErr.Code())
		require.Equal(t, "user notExists does not exist", targetErr.Payload.Error[0].Message)
	})
}

func TestAuthzRolesAndUserHaveTheSameName(t *testing.T) {
	adminUser := "admin"
	adminKey := "admin"

	similar := "similarRoleKeyUserName"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(similar, similar).
		WithRBAC().WithRbacAdmins(adminUser).
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("create role with the same name of the user", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(similar),
			Permissions: []*models.Permission{
				{Action: String(authorization.CreateSchema), Collection: String("*")},
			},
		})
	})

	t.Run("create role with the same name of the user", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, similar, similar)
	})

	t.Run("get role and user were they have the same name", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, similar)
		require.NotNil(t, role)
		require.Equal(t, similar, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateSchema, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collection)

		roles := helper.GetRolesForUser(t, similar, adminKey)
		require.Equal(t, 1, len(roles))
		require.NotNil(t, role)
		require.Equal(t, similar, *role.Name)
		require.Len(t, role.Permissions, 1)
		require.Equal(t, authorization.CreateSchema, *role.Permissions[0].Action)
		require.Equal(t, "*", *role.Permissions[0].Collection)
	})
}
