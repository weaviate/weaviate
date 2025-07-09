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

package authz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZBackupsManageJourney(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	viewerUser := "viewer-user"
	viewerKey := "viewer-key"

	customUser := "custom-user"
	customKey := "custom-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).WithUserApiKey(viewerUser, viewerKey).
		WithRBAC().WithRbacRoots(adminUser).WithRbacViewers(viewerUser).
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

	backend := "filesystem"
	backupID := "backup-1"
	testRoleName := "test-role"

	clsA := articles.ArticlesClass()
	clsP := articles.ParagraphsClass()
	objA := articles.NewArticle().WithTitle("Programming 101")
	objP := articles.NewParagraph().WithContents("hello world")

	// cleanup
	deleteObjectClass(t, clsA.Class, helper.CreateAuth(adminKey))
	deleteObjectClass(t, clsP.Class, helper.CreateAuth(adminKey))
	helper.DeleteRole(t, adminKey, testRoleName)

	helper.CreateClassAuth(t, clsP, adminKey)
	helper.CreateClassAuth(t, clsA, adminKey)
	helper.CreateObjectsBatchAuth(t, []*models.Object{objA.Object(), objP.Object()}, adminKey)

	t.Run("create and assign a role that does have the manage_backups permission", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{
				{Action: String(authorization.ReadRoles), Backups: &models.PermissionBackups{Collection: String("IDoNotExist")}},
			},
		})
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)
	})

	t.Run("viewer cannot create a backup", func(t *testing.T) {
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(viewerKey))
		require.NotNil(t, err)
		var parsed *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to create a backup due to missing manage_backups action", func(t *testing.T) {
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, backupID, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		var parsed *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to cancel a backup due to missing manage_backups action", func(t *testing.T) {
		err := helper.CancelBackupWithAuthz(t, backend, backupID, helper.CreateAuth(customKey))
		require.NotNil(t, err)
		var parsed *backups.BackupsCancelForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("manage backups of clsA.Class collection", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, testRoleName, helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(clsA.Class).Permission())
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

	t.Run("viewer cannot restore a backup", func(t *testing.T) {
		_, err := helper.RestoreBackupWithAuthz(t, helper.DefaultRestoreConfig(), clsA.Class, backend, backupID, map[string]string{}, helper.CreateAuth(viewerKey))
		require.Error(t, err)

		var parsed *backups.BackupsRestoreForbidden
		forbidden := errors.As(err, &parsed)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
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
			// handle success also in case of the backup was fast
			if *resp.Payload.Status == string(backup.Cancelled) || *resp.Payload.Status == string(backup.Success) {
				break
			}
			if *resp.Payload.Status == "FAILED" {
				t.Fatalf("backup failed: %s", resp.Payload.Error)
			}
			time.Sleep(time.Second / 10)
		}
	})
}
