//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authn

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// testRotateKeyNeedsTargetPermissions checks that a caller holding update_users
// on every user cannot rotate the key of a user holding the admin role.
func testRotateKeyNeedsTargetPermissions(t *testing.T) {
	adminKey := "admin-key"

	callerUser := "RotateKeyCaller"
	helper.DeleteUser(t, callerUser, adminKey)
	callerKey := helper.CreateUser(t, callerUser, adminKey)
	defer helper.DeleteUser(t, callerUser, adminKey)

	updateUsers := authorization.UpdateUsers
	all := "*"
	updateUsersRole := "RotateKeyUpdateUsers"
	helper.DeleteRole(t, adminKey, updateUsersRole)
	helper.CreateRole(t, adminKey, &models.Role{
		Name:        &updateUsersRole,
		Permissions: []*models.Permission{{Action: &updateUsers, Users: &models.PermissionUsers{Users: &all}}},
	})
	defer helper.DeleteRole(t, adminKey, updateUsersRole)
	helper.AssignRoleToUser(t, adminKey, updateUsersRole, callerUser)

	target := "RotateKeyAdminTarget"
	helper.DeleteUser(t, target, adminKey)
	targetKey := helper.CreateUser(t, target, adminKey)
	defer helper.DeleteUser(t, target, adminKey)
	helper.AssignRoleToUser(t, adminKey, authorization.Admin, target)

	_, err := helper.Client(t).Users.RotateUserAPIKey(users.NewRotateUserAPIKeyParams().WithUserID(target), helper.CreateAuth(callerKey))
	var forbidden *users.RotateUserAPIKeyForbidden
	require.True(t, errors.As(err, &forbidden), "expected 403, got %T: %v", err, err)

	info := helper.GetInfoForOwnUser(t, targetKey)
	require.Equal(t, target, *info.Username)
}
