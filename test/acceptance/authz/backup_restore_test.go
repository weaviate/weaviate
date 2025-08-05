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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	envS3UseSSL                       = "BACKUP_S3_USE_SSL"
	s3BackupJourneyClassName          = "S3Backup"
	s3BackupJourneyBackupIDSingleNode = "s3-backup-single-node"
	s3BackupJourneyBackupIDCluster    = "s3-backup-cluster"
	s3BackupJourneyRegion             = "eu-west-1"
	s3BackupJourneyAccessKey          = "aws_access_key"
	s3BackupJourneySecretKey          = "aws_secret_key"
)

func TestBackupAndRestoreRBAC(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, "custom-key").
		WithRBAC().WithRbacRoots(adminUser).WithDbUsers().
		WithBackendS3("bucket", s3BackupJourneyRegion).
		WithWeaviateCluster(3).
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	backend := "s3"
	testRoleName := "testRole"
	testCollectionName := "TestCollection"

	// one class is needed for backup
	par := articles.ParagraphsClass()

	testRole := &models.Role{
		Name: String(testRoleName),
		Permissions: []*models.Permission{
			{Action: String(authorization.ReadRoles), Backups: &models.PermissionBackups{Collection: String(testCollectionName)}},
		},
	}

	t.Run("Backup and full restore", func(t *testing.T) {
		backupID := "backup-1"

		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		helper.DeleteRole(t, adminKey, testRoleName)
		helper.CreateRole(t, adminKey, testRole)
		defer helper.DeleteRole(t, adminKey, testRoleName)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), par.Class, backend, backupID, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		waitForBackup(t, backupID, backend, adminKey)

		// delete role and assignment
		helper.DeleteRole(t, adminKey, testRoleName)
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		all := "all"
		restoreConf := helper.DefaultRestoreConfig()
		restoreConf.RolesOptions = &all
		respR, err := helper.RestoreBackupWithAuthz(t, restoreConf, par.Class, backend, backupID, map[string]string{}, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, respR.Payload)
		require.Equal(t, "", respR.Payload.Error)

		role := helper.GetRoleByName(t, adminKey, testRoleName)
		require.NotNil(t, role)
		require.Equal(t, *role.Name, testRoleName)

		user := helper.GetUser(t, customUser, adminKey)
		require.NotNil(t, user)
		require.Equal(t, *user.UserID, customUser)
		require.Equal(t, user.Roles[0], testRoleName)

		roles := helper.GetRolesForUser(t, customUser, adminKey, false)
		require.Len(t, roles, 1)
		require.Equal(t, *roles[0].Name, testRoleName)
	})

	time.Sleep(2 * time.Second) // wait for the backup to be fully processed

	t.Run("Backup and restore without roles", func(t *testing.T) {
		backupID := "backup-2"

		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		helper.DeleteRole(t, adminKey, testRoleName)
		helper.CreateRole(t, adminKey, testRole)
		defer helper.DeleteRole(t, adminKey, testRoleName)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), par.Class, backend, backupID, helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.Equal(t, "", resp.Payload.Error)

		waitForBackup(t, backupID, backend, adminKey)

		// delete role and assignment
		helper.DeleteRole(t, adminKey, testRoleName)
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		noRestore := "noRestore"
		restoreConf := helper.DefaultRestoreConfig()
		restoreConf.RolesOptions = &noRestore
		respR, err := helper.RestoreBackupWithAuthz(t, restoreConf, par.Class, backend, backupID, map[string]string{}, helper.CreateAuth(adminKey))
		if err != nil {
			n := err.(interface{})
			e := n.(*backups.BackupsRestoreUnprocessableEntity)
			fmt.Printf("Full error restoring backup:  %+v | %+v | %+v \n", e.GetPayload().Error, e.GetPayload().Error, e.Payload)
		}
		require.Nil(t, err)
		require.NotNil(t, respR.Payload)
		require.Equal(t, "", respR.Payload.Error)

		respRole, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRoleName), helper.CreateAuth(adminKey))
		require.Nil(t, respRole)
		require.Error(t, err)

		roles := helper.GetRolesForUser(t, customUser, adminKey, false)
		require.Len(t, roles, 0)
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
