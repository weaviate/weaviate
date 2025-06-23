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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestBackupAndRestoreDynamicUsers(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).
		WithRBAC().WithRbacRoots(adminUser).
		WithBackendFilesystem().WithDbUsers().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	backend := "filesystem"
	testUserName := "test-user"

	// one class is needed for backup
	par := articles.ParagraphsClass()

	t.Run("Backup and full restore", func(t *testing.T) {
		backupID := "backup-1"

		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		helper.DeleteUser(t, testUserName, adminKey)
		helper.CreateUser(t, testUserName, adminKey)
		defer helper.DeleteUser(t, testUserName, adminKey)

		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), par.Class, backend, backupID, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		waitForBackup(t, backupID, backend, adminKey)

		// delete user
		helper.DeleteUser(t, testUserName, adminKey)
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		all := "all"
		restoreConf := helper.DefaultRestoreConfig()
		restoreConf.UsersOptions = &all
		respR, err := helper.RestoreBackupWithAuthz(t, restoreConf, par.Class, backend, backupID, map[string]string{}, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, respR.Payload)
		require.Equal(t, "", respR.Payload.Error)
		waitForRestore(t, backupID, backend, adminKey)

		user := helper.GetUser(t, testUserName, adminKey)
		require.NotNil(t, user)
		require.Equal(t, *user.UserID, testUserName)
	})

	t.Run("Backup and restore without users", func(t *testing.T) {
		backupID := "backup-2"

		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		helper.DeleteUser(t, testUserName, adminKey)
		helper.CreateUser(t, testUserName, adminKey)
		defer helper.DeleteUser(t, testUserName, adminKey)

		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), par.Class, backend, backupID, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		waitForBackup(t, backupID, backend, adminKey)

		// delete user
		helper.DeleteUser(t, testUserName, adminKey)
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		noRestore := "noRestore"
		restoreConf := helper.DefaultRestoreConfig()
		restoreConf.UsersOptions = &noRestore
		respR, err := helper.RestoreBackupWithAuthz(t, restoreConf, par.Class, backend, backupID, map[string]string{}, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, respR.Payload)
		require.Equal(t, "", respR.Payload.Error)
		waitForRestore(t, backupID, backend, adminKey)

		respU, err := helper.Client(t).Users.GetUserInfo(users.NewGetUserInfoParams().WithUserID(testUserName), helper.CreateAuth(adminKey))
		require.Nil(t, respU)
		require.Error(t, err)
	})

	testRoleName := "testRole"
	testCollectionName := "TestCollection"
	testRole := &models.Role{
		Name: String(testRoleName),
		Permissions: []*models.Permission{
			{Action: String(authorization.ReadRoles), Backups: &models.PermissionBackups{Collection: String(testCollectionName)}},
		},
	}

	t.Run("Backup and full restore users and roles", func(t *testing.T) {
		backupID := "backup-3"

		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		helper.DeleteUser(t, testUserName, adminKey)
		helper.CreateUser(t, testUserName, adminKey)
		defer helper.DeleteUser(t, testUserName, adminKey)

		helper.DeleteRole(t, adminKey, testRoleName)
		helper.CreateRole(t, adminKey, testRole)
		defer helper.DeleteRole(t, adminKey, testRoleName)
		helper.AssignRoleToUser(t, adminKey, testRoleName, testUserName)

		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), par.Class, backend, backupID, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		waitForBackup(t, backupID, backend, adminKey)

		// delete user and role
		helper.DeleteUser(t, testUserName, adminKey)
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		helper.DeleteRole(t, adminKey, testRoleName)

		all := "all"
		restoreConf := helper.DefaultRestoreConfig()
		restoreConf.UsersOptions = &all
		restoreConf.RolesOptions = &all
		respR, err := helper.RestoreBackupWithAuthz(t, restoreConf, par.Class, backend, backupID, map[string]string{}, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, respR.Payload)
		require.Equal(t, "", respR.Payload.Error)

		waitForRestore(t, backupID, backend, adminKey)

		user := helper.GetUser(t, testUserName, adminKey)
		require.NotNil(t, user)
		require.Equal(t, *user.UserID, testUserName)
		require.Equal(t, user.Roles[0], testRoleName)
	})
}

func waitForBackup(t *testing.T, backupID, backend, adminKey string) {
	for {
		resp, err := helper.CreateBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		if *resp.Payload.Status == "SUCCESS" {
			break
		}
		if *resp.Payload.Status == "FAILED" {
			t.Fatalf("backup failed: %s", resp.Payload.Error)
		}
		time.Sleep(time.Second / 10)
	}
}

func waitForRestore(t *testing.T, backupID, backend, adminKey string) {
	for {
		resp, err := helper.RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		if *resp.Payload.Status == "SUCCESS" {
			break
		}
		if *resp.Payload.Status == "FAILED" {
			t.Fatalf("backup failed: %s", resp.Payload.Error)
		}
		time.Sleep(time.Second / 10)
	}
}
