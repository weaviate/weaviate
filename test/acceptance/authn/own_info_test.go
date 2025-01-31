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

package authn

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthnGetOwnInfo(t *testing.T) {
	customUser := "custom-user"
	customKey := "custom-key"

	testingRole := "testingOwnRole"

	adminKey := "admin-key"
	adminUser := "admin-user"

	_, teardown := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)

	defer func() {
		helper.DeleteRole(t, adminKey, testingRole)
		teardown()
	}()

	t.Run("Get own info - no roles", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, customKey)
		require.Equal(t, customUser, *info.Username)
		require.Len(t, info.Roles, 0)
	})

	t.Run("Create and assign role", func(t *testing.T) {
		helper.CreateRole(
			t,
			adminKey,
			&models.Role{
				Name: &testingRole,
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			},
		)
		helper.AssignRoleToUser(t, "admin-key", testingRole, customUser)
	})

	t.Run("Get own roles - existing roles", func(t *testing.T) {
		info := helper.GetInfoForOwnUser(t, customKey)
		require.Equal(t, customUser, *info.Username)
		require.Len(t, info.Roles, 1)
		require.Equal(t, testingRole, *info.Roles[0].Name)
	})
}
