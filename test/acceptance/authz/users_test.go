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

	anotherUser := "another-user"
	anotherKey := "another-key"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{anotherUser: anotherKey}, nil)
	defer down()

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
		targetErr, ok := err.(*authz.GetRolesForUserNotFound)
		require.True(t, ok)
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
