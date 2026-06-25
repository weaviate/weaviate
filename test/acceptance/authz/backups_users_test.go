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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestAuthZBackupIncludeUsersJourney pins the dynamic-user authz gate that sits
// alongside the per-collection one: exporting a user's credential material (on
// create) and re-materialising it (on restore) each require a backup permission
// on backups/users/<id>. Both callers below already hold manage_backups on the
// collection, so every 403 is attributable to the missing user permission alone.
func TestAuthZBackupIncludeUsersJourney(t *testing.T) {
	adminUser, adminKey := "admin-user", "admin-key"
	colOnlyUser, colOnlyKey := "col-only-user", "col-only-key"
	fullUser, fullKey := "full-user", "full-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(colOnlyUser, colOnlyKey).WithUserApiKey(fullUser, fullKey).
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
	backupID := "backup-users-1"
	dynUser := "alice"
	userResource := "backups/users/" + dynUser

	cls := articles.ParagraphsClass()
	helper.CreateClassAuth(t, cls, adminKey)
	helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewParagraph().WithContents("hello world").Object()}, adminKey)
	helper.CreateUser(t, dynUser, adminKey)

	colPerm := helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(cls.Class).Permission()
	userPerm := helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithUser(dynUser).Permission()

	// colOnlyUser may back up the collection but not the user; fullUser may do both.
	helper.CreateRole(t, adminKey, &models.Role{Name: String("col-only-role"), Permissions: []*models.Permission{colPerm}})
	helper.AssignRoleToUser(t, adminKey, "col-only-role", colOnlyUser)
	helper.CreateRole(t, adminKey, &models.Role{Name: String("full-role"), Permissions: []*models.Permission{colPerm, userPerm}})
	helper.AssignRoleToUser(t, adminKey, "full-role", fullUser)

	createWithUsers := func(t *testing.T, id, key string) (*backups.BackupsCreateOK, error) {
		return helper.Client(t).Backups.BackupsCreate(
			backups.NewBackupsCreateParams().WithBackend(backend).WithBody(&models.BackupCreateRequest{
				ID:           id,
				Include:      []string{cls.Class},
				IncludeUsers: []string{dynUser},
				Config:       helper.DefaultBackupConfig(),
			}),
			helper.CreateAuth(key),
		)
	}

	restoreWithUsers := func(t *testing.T, key string) (*backups.BackupsRestoreOK, error) {
		all := "all"
		conf := helper.DefaultRestoreConfig()
		conf.UsersOptions = &all // default is noRestore, which skips the user gate entirely
		return helper.Client(t).Backups.BackupsRestore(
			backups.NewBackupsRestoreParams().WithBackend(backend).WithID(backupID).WithBody(&models.BackupRestoreRequest{
				Config: conf,
			}),
			helper.CreateAuth(key),
		)
	}

	t.Run("create with includeUsers is forbidden without the user permission", func(t *testing.T) {
		_, err := createWithUsers(t, "backup-users-denied", colOnlyKey)
		var parsed *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
		require.Contains(t, parsed.Payload.Error[0].Message, userResource)
	})

	t.Run("create with includeUsers succeeds with the user permission", func(t *testing.T) {
		resp, err := createWithUsers(t, backupID, fullKey)
		require.Nil(t, err)
		require.Equal(t, "", resp.Payload.Error)
		require.Contains(t, resp.Payload.Users, dynUser)
		helper.ExpectBackupEventuallyCreated(t, backupID, backend, helper.CreateAuth(fullKey))
	})

	// Drop the collection so the restore actually re-materialises it; the user
	// remains, since restore overwrites it from the artefact.
	helper.DeleteClassWithAuthz(t, cls.Class, helper.CreateAuth(adminKey))

	t.Run("restore of an artefact with users is forbidden without the user permission", func(t *testing.T) {
		_, err := restoreWithUsers(t, colOnlyKey)
		var parsed *backups.BackupsRestoreForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
		require.Contains(t, parsed.Payload.Error[0].Message, userResource)
	})

	t.Run("restore of an artefact with users succeeds with the user permission", func(t *testing.T) {
		resp, err := restoreWithUsers(t, fullKey)
		require.Nil(t, err)
		require.Equal(t, "", resp.Payload.Error)
		helper.ExpectBackupEventuallyRestored(t, backupID, backend, helper.CreateAuth(fullKey))
	})
}
