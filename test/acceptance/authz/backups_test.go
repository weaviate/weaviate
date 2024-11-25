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
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZBackupsManageJourney(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"
	adminRole := "admin"

	customUser := "custom-user"
	customKey := "custom-key"
	customRole := "custom"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	backend := "filesystem"
	backupID := "backup-1"

	compose, err := docker.
		New().
		WithWeaviate().
		WithRBAC().
		WithRbacUser(adminUser, adminKey, adminRole).
		WithRbacUser(customUser, customKey, customRole).
		WithBackendFilesystem().
		Start(ctx)
	require.Nil(t, err)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	testRoleName := "test-role"

	clsA := articles.ArticlesClass()
	clsP := articles.ParagraphsClass()
	objA := articles.NewArticle().WithTitle("Programming 101")
	objP := articles.NewParagraph().WithContents("hello world")

	t.Run("setup", func(t *testing.T) {
		helper.CreateClassWithAuthz(t, clsP, helper.CreateAuth(adminKey))
		helper.CreateClassWithAuthz(t, clsA, helper.CreateAuth(adminKey))
		helper.CreateObjectsBatchAuth(t, []*models.Object{objA.Object(), objP.Object()}, adminKey)
	})

	t.Run("create and assign a role that does have the manage_backups permission", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{
				{Action: String(authorization.ReadRoles), Collection: String(testRoleName)},
			},
		})
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)
	})

	t.Run("fail to create a backup due to missing manage_backups action", func(t *testing.T) {
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsCreateForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to restore a backup due to missing manage_backups action", func(t *testing.T) {
		_, err := helper.RestoreBackupWithAuthz(t, helper.DefaultRestoreConfig(), clsA.Class, backend, backupID, map[string]string{}, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsRestoreForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to list backups due to missing read_backups action", func(t *testing.T) {
		_, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsListForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to cancel a backup due to missing manage_backups action", func(t *testing.T) {
		err := helper.CancelBackupWithAuthz(t, backend, backupID, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsCancelForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to get create backup status due to missing read_backups action", func(t *testing.T) {
		_, err := helper.CreateBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsCreateStatusForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to get restore backup status due to missing read_backups action", func(t *testing.T) {
		_, err := helper.RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsRestoreStatusForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add manage all backups permission to role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, testRoleName, helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithBackend(backend).Permission())
	})

	t.Run("fail to create a backup due to missing read_schema action on clsA.Class", func(t *testing.T) {
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsCreateForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add read_schema action on clsA.Class to the role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, testRoleName, helper.NewSchemaPermission().WithAction(authorization.ReadSchema).WithCollection(clsA.Class).Permission())
	})

	t.Run("fail to create a backup due to missing read_data action on clsA.Class", func(t *testing.T) {
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsCreateForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add read_data action on clsA.Class to the role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, testRoleName, helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(clsA.Class).Permission())
	})

	t.Run("successfully create a backup with sufficient permissions", func(t *testing.T) {
		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		for {
			resp, err := helper.CreateBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(customKey))
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
	})

	t.Run("delete clsA", func(t *testing.T) {
		helper.DeleteClassWithAuthz(t, clsA.Class, helper.CreateAuth(adminKey))
	})

	t.Run("fail to create a backup due to missing create_schema action on clsA.Class", func(t *testing.T) {
		_, err := helper.RestoreBackupWithAuthz(t, helper.DefaultRestoreConfig(), clsA.Class, backend, backupID, map[string]string{}, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsRestoreForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add create_schema action on clsA.Class to the role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, testRoleName, helper.NewSchemaPermission().WithAction(authorization.CreateSchema).WithCollection(clsA.Class).Permission())
	})

	t.Run("fail to restore a backup due to missing create_data action on clsA.Class", func(t *testing.T) {
		_, err := helper.RestoreBackupWithAuthz(t, helper.DefaultRestoreConfig(), clsA.Class, backend, backupID, map[string]string{}, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*backups.BackupsRestoreForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add create_data action on clsA.Class to the role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, testRoleName, helper.NewDataPermission().WithAction(authorization.CreateData).WithCollection(clsA.Class).Permission())
	})

	t.Run("successfully restore a backup with sufficient permissions", func(t *testing.T) {
		resp, err := helper.RestoreBackupWithAuthz(t, helper.DefaultRestoreConfig(), clsA.Class, backend, backupID, map[string]string{}, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		for {
			resp, err := helper.RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(customKey))
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
	})

	t.Run("successfully cancel an in-progress backup", func(t *testing.T) {
		backupID = "backup-2"
		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		err = helper.CancelBackupWithAuthz(t, backend, backupID, helper.CreateAuth(customKey))
		require.Nil(t, err)

		for {
			resp, err := helper.CreateBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(customKey))
			require.Nil(t, err)
			require.NotNil(t, resp.Payload)
			if *resp.Payload.Status == "CANCELED" {
				break
			}
			if *resp.Payload.Status == "FAILED" {
				t.Fatalf("backup failed: %s", resp.Payload.Error)
			}
			time.Sleep(time.Second / 10)
		}
	})
}
