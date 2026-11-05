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

// testRotateKeyNeedsTargetPermissions runs a caller holding update_users on
// every user against targets with more, and with no more, permissions than it.
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

	rotateAsCaller := func(target string) error {
		_, err := helper.Client(t).Users.RotateUserAPIKey(users.NewRotateUserAPIKeyParams().WithUserID(target), helper.CreateAuth(callerKey))
		return err
	}
	requireForbiddenAndOldKeyWorks := func(t *testing.T, target, targetKey string) {
		err := rotateAsCaller(target)
		var forbidden *users.RotateUserAPIKeyForbidden
		require.True(t, errors.As(err, &forbidden), "expected 403, got %T: %v", err, err)

		info := helper.GetInfoForOwnUser(t, targetKey)
		require.Equal(t, target, *info.Username)
	}

	t.Run("imported root user", func(t *testing.T) {
		// DeleteUser refuses root users, so the import outlives this suite until
		// the container is torn down.
		key := helper.CreateUserWithApiKey(t, importedRootUser, adminKey, nil)
		require.Equal(t, importedRootKey, key)

		requireForbiddenAndOldKeyWorks(t, importedRootUser, importedRootKey)
	})

	t.Run("user holding the admin role", func(t *testing.T) {
		target := "RotateKeyAdminTarget"
		helper.DeleteUser(t, target, adminKey)
		targetKey := helper.CreateUser(t, target, adminKey)
		defer helper.DeleteUser(t, target, adminKey)
		helper.AssignRoleToUser(t, adminKey, authorization.Admin, target)

		requireForbiddenAndOldKeyWorks(t, target, targetKey)
	})

	t.Run("user holding manage_backups, which the caller can only read", func(t *testing.T) {
		backupManager := "RotateKeyBackupManager"
		helper.DeleteRole(t, adminKey, backupManager)
		helper.CreateRole(t, adminKey, &models.Role{
			Name:        &backupManager,
			Permissions: []*models.Permission{helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection("*").Permission()},
		})
		defer helper.DeleteRole(t, adminKey, backupManager)

		target := "RotateKeyBackupTarget"
		helper.DeleteUser(t, target, adminKey)
		targetKey := helper.CreateUser(t, target, adminKey)
		defer helper.DeleteUser(t, target, adminKey)
		helper.AssignRoleToUser(t, adminKey, backupManager, target)

		helper.AssignRoleToUser(t, adminKey, authorization.Viewer, callerUser)
		defer helper.RevokeRoleFromUser(t, adminKey, authorization.Viewer, callerUser)

		requireForbiddenAndOldKeyWorks(t, target, targetKey)
	})

	t.Run("user holding only permissions the caller holds", func(t *testing.T) {
		target := "RotateKeyPeerTarget"
		helper.DeleteUser(t, target, adminKey)
		oldKey := helper.CreateUser(t, target, adminKey)
		defer helper.DeleteUser(t, target, adminKey)
		helper.AssignRoleToUser(t, adminKey, updateUsersRole, target)

		require.NoError(t, rotateAsCaller(target))

		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(oldKey))
		var unauthorized *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &unauthorized), "expected the old key to stop working, got %T: %v", err, err)
	})
}
