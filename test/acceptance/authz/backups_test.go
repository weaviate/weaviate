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

	t.Run("list backups filters in response instead of returning 403", func(t *testing.T) {
		// Admin sees every backup.
		adminResp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.Len(t, adminResp.Payload, 1)
		require.Equal(t, backupID, adminResp.Payload[0].ID)

		// customUser has manage_backups on clsA → sees backup-1.
		customResp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, customResp.Payload, 1)
		require.Equal(t, backupID, customResp.Payload[0].ID)

		// Viewer has no backup permissions → 200 with an empty list, not 403.
		viewerResp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(viewerKey))
		require.Nil(t, err)
		require.Len(t, viewerResp.Payload, 0)
	})

	t.Run("multi-class backup is filtered out for caller missing permission on one of its classes", func(t *testing.T) {
		multiBackupID := "backup-multi-1"
		params := backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:      multiBackupID,
				Include: []string{clsA.Class, clsP.Class},
				Config:  helper.DefaultBackupConfig(),
			})
		_, err := helper.Client(t).Backups.BackupsCreate(params, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		helper.ExpectBackupEventuallyCreated(t, multiBackupID, backend, helper.CreateAuth(adminKey))

		// Admin sees both backups.
		adminResp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.Len(t, adminResp.Payload, 2)

		// customUser has manage_backups on clsA only; backup-multi-1 spans clsP
		// so the every-class READ requirement filters it out.
		customResp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, customResp.Payload, 1)
		require.Equal(t, backupID, customResp.Payload[0].ID)
	})

	t.Run("backup status is 403 for callers without permission on the backup classes", func(t *testing.T) {
		_, err := helper.CreateBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(viewerKey))
		require.Error(t, err)
		var parsed *backups.BackupsCreateStatusForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("cancel is 403 for callers without permission on the backup classes", func(t *testing.T) {
		err := helper.CancelBackupWithAuthz(t, backend, backupID, helper.CreateAuth(viewerKey))
		require.Error(t, err)
		var parsed *backups.BackupsCancelForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("empty Include filters to classes the caller is permitted to back up", func(t *testing.T) {
		filterBackupID := "backup-filter-1"
		params := backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:     filterBackupID,
				Config: helper.DefaultBackupConfig(),
			})
		resp, err := helper.Client(t).Backups.BackupsCreate(params, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)
		// customUser has manage_backups on clsA only; clsP must be filtered out.
		require.Equal(t, []string{clsA.Class}, resp.Payload.Classes)

		helper.ExpectBackupEventuallyCreated(t, filterBackupID, backend, helper.CreateAuth(customKey))
	})

	t.Run("empty Include with no backup permissions returns 403", func(t *testing.T) {
		params := backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:     "backup-filter-viewer",
				Config: helper.DefaultBackupConfig(),
			})
		_, err := helper.Client(t).Backups.BackupsCreate(params, helper.CreateAuth(viewerKey))
		require.Error(t, err)
		var parsed *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("explicit Include is forbidden when caller lacks permission on a listed class", func(t *testing.T) {
		params := backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:      "backup-explicit-forbidden",
				Include: []string{clsP.Class},
				Config:  helper.DefaultBackupConfig(),
			})
		_, err := helper.Client(t).Backups.BackupsCreate(params, helper.CreateAuth(customKey))
		require.Error(t, err)
		var parsed *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
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

		helper.ExpectBackupEventuallyRestored(t, backupID, backend, helper.CreateAuth(adminKey))
	})

	t.Run("restoration status is 403 for callers without permission on the backup classes", func(t *testing.T) {
		// Restore meta has been written by the previous step; the authz
		// check resolves its classes and rejects the viewer.
		_, err := helper.RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(viewerKey))
		require.Error(t, err)
		var parsed *backups.BackupsRestoreStatusForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
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
			// also accept success in case the backup was fast
			if *resp.Payload.Status == string(backup.Cancelled) || *resp.Payload.Status == string(backup.Success) {
				break
			}
			if *resp.Payload.Status == "FAILED" {
				t.Fatalf("backup failed: %s", resp.Payload.Error)
			}
			time.Sleep(time.Second / 10)
		}
	})

	t.Run("restoration cancel is 403 for callers without permission on the backup classes", func(t *testing.T) {
		err := helper.RestoreCancelWithAuthz(t, backend, "backup-1", helper.CreateAuth(viewerKey))
		require.Error(t, err)
		var parsed *backups.BackupsRestoreCancelForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("empty Include restore with no backup permissions returns 403", func(t *testing.T) {
		params := backups.NewBackupsRestoreParams().
			WithBackend(backend).
			WithID("backup-1").
			WithBody(&models.BackupRestoreRequest{
				Config: helper.DefaultRestoreConfig(),
			})
		_, err := helper.Client(t).Backups.BackupsRestore(params, helper.CreateAuth(viewerKey))
		require.Error(t, err)
		var parsed *backups.BackupsRestoreForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("explicit Include restore is forbidden when caller lacks permission on a listed class", func(t *testing.T) {
		params := backups.NewBackupsRestoreParams().
			WithBackend(backend).
			WithID("backup-multi-1").
			WithBody(&models.BackupRestoreRequest{
				Include: []string{clsP.Class},
				Config:  helper.DefaultRestoreConfig(),
			})
		_, err := helper.Client(t).Backups.BackupsRestore(params, helper.CreateAuth(customKey))
		require.Error(t, err)
		var parsed *backups.BackupsRestoreForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("empty Include restore filters to classes the caller is permitted to restore", func(t *testing.T) {
		// Delete clsA and clsP so backup-multi-1 can be restored; otherwise
		// restore fails because the classes already exist on the cluster.
		helper.DeleteClassWithAuthz(t, clsA.Class, helper.CreateAuth(adminKey))
		helper.DeleteClassWithAuthz(t, clsP.Class, helper.CreateAuth(adminKey))

		params := backups.NewBackupsRestoreParams().
			WithBackend(backend).
			WithID("backup-multi-1").
			WithBody(&models.BackupRestoreRequest{
				Config: helper.DefaultRestoreConfig(),
			})
		resp, err := helper.Client(t).Backups.BackupsRestore(params, helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)
		// customUser has manage_backups on clsA only; clsP must be filtered out.
		require.Equal(t, []string{clsA.Class}, resp.Payload.Classes)

		helper.ExpectBackupEventuallyRestored(t, "backup-multi-1", backend, helper.CreateAuth(adminKey))
	})

	t.Run("successfully cancel an in-progress restore", func(t *testing.T) {
		// Create a fresh backup, then delete the class so a restore can run
		// and be cancelled mid-flight.
		cancelRestoreID := "backup-3"
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, cancelRestoreID, helper.CreateAuth(customKey))
		require.Nil(t, err)
		helper.ExpectBackupEventuallyCreated(t, cancelRestoreID, backend, helper.CreateAuth(customKey))

		helper.DeleteClassWithAuthz(t, clsA.Class, helper.CreateAuth(adminKey))

		_, err = helper.RestoreBackupWithAuthz(t, helper.DefaultRestoreConfig(), clsA.Class, backend, cancelRestoreID, map[string]string{}, helper.CreateAuth(customKey))
		require.Nil(t, err)

		err = helper.RestoreCancelWithAuthz(t, backend, cancelRestoreID, helper.CreateAuth(customKey))
		require.Nil(t, err)

		for {
			resp, err := helper.RestoreBackupStatusWithAuthz(t, backend, cancelRestoreID, "", "", helper.CreateAuth(customKey))
			require.Nil(t, err)
			require.NotNil(t, resp.Payload)
			// also accept success in case the restore was fast
			if *resp.Payload.Status == string(backup.Cancelled) || *resp.Payload.Status == string(backup.Success) {
				break
			}
			if *resp.Payload.Status == "FAILED" {
				t.Fatalf("restore failed: %s", resp.Payload.Error)
			}
			time.Sleep(time.Second / 10)
		}
	})

	// The base backup ID of an incremental backup is exposed via the get-status
	// and list endpoints only to root users. customUser holds the full
	// manage_backups permission on clsA.Class (granted above) but is not root, so
	// it must not see it. Create a base + incremental backup as prerequisites for
	// the visibility checks below; these are setup, not standalone subtests.
	baseBackupID := "base-backup"
	incrementalBackupID := "incremental-backup"

	_, err = helper.CreateBackupWithBaseAndAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, baseBackupID, "", helper.CreateAuth(adminKey))
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, baseBackupID, backend, helper.CreateAuth(adminKey))

	// add more data so the incremental backup has something to capture, then back
	// it up on top of the base
	objB := articles.NewArticle().WithTitle("Programming 102")
	helper.CreateObjectsBatchAuth(t, []*models.Object{objB.Object()}, adminKey)

	_, err = helper.CreateBackupWithBaseAndAuthz(t, helper.DefaultBackupConfig(), clsA.Class, backend, incrementalBackupID, baseBackupID, helper.CreateAuth(adminKey))
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, incrementalBackupID, backend, helper.CreateAuth(adminKey))

	t.Run("root sees the base backup id in the create-status response", func(t *testing.T) {
		resp, err := helper.CreateBackupStatusWithAuthz(t, backend, incrementalBackupID, "", "", helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, baseBackupID, resp.Payload.IncrementalBaseBackupID)
	})

	t.Run("non-root with manage_backups does not see the base backup id in the create-status response", func(t *testing.T) {
		resp, err := helper.CreateBackupStatusWithAuthz(t, backend, incrementalBackupID, "", "", helper.CreateAuth(customKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		require.Empty(t, resp.Payload.IncrementalBaseBackupID)
	})

	t.Run("root sees the base backup id in the list response", func(t *testing.T) {
		resp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(adminKey))
		require.NoError(t, err)
		item := findBackupListItem(resp.Payload, incrementalBackupID)
		require.NotNil(t, item)
		require.Equal(t, baseBackupID, item.IncrementalBaseBackupID)
	})

	t.Run("non-root with manage_backups does not see the base backup id in the list response", func(t *testing.T) {
		resp, err := helper.ListBackupsWithAuthz(t, backend, helper.CreateAuth(customKey))
		require.NoError(t, err)
		item := findBackupListItem(resp.Payload, incrementalBackupID)
		require.NotNil(t, item)
		require.Empty(t, item.IncrementalBaseBackupID)
	})
}

// findBackupListItem returns the list item with the given backup ID, or nil.
func findBackupListItem(items models.BackupListResponse, backupID string) *models.BackupListResponseItems0 {
	for _, item := range items {
		if item != nil && item.ID == backupID {
			return item
		}
	}
	return nil
}
