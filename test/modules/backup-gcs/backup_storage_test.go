//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/modules/backup-gcs/gcs"
	"github.com/semi-technologies/weaviate/test/docker"
	moduleshelper "github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GCSStorage_Backup(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithGCS().Start(ctx)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "cannot start"))
	}

	require.Nil(t, os.Setenv(envGCSEndpoint, compose.GetGCS().URI()))

	t.Run("store backup meta", moduleLevelStoreBackupMeta)
	t.Run("copy objects", moduleLevelCopyObjects)
	t.Run("copy files", moduleLevelCopyFiles)

	if err := compose.Terminate(ctx); err != nil {
		t.Fatal(errors.Wrapf(err, "failed to terminte test containers"))
	}
}

func moduleLevelStoreBackupMeta(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	className := "BackupClass"
	backupID := "backup_id"
	bucketName := "bucket"
	projectID := "project-id"
	endpoint := os.Getenv(envGCSEndpoint)
	metadataFilename := "backup.json"

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envGCSEndpoint, endpoint))
		require.Nil(t, os.Setenv(envGCSStorageEmulatorHost, endpoint))
		require.Nil(t, os.Setenv(envGCSCredentials, ""))
		require.Nil(t, os.Setenv(envGCSProjectID, projectID))
		require.Nil(t, os.Setenv(envGCSBucket, bucketName))

		createBucket(testCtx, t, projectID, bucketName)
	})

	t.Run("store backup meta in gcs", func(t *testing.T) {
		gcsConfig := gcs.NewConfig(bucketName, "")
		gcs, err := gcs.New(testCtx, gcsConfig, dataDir)
		require.Nil(t, err)

		t.Run("access permissions", func(t *testing.T) {
			err := gcs.Initialize(testCtx, backupID)
			assert.Nil(t, err)
		})

		t.Run("backup meta does not exist yet", func(t *testing.T) {
			meta, err := gcs.GetObject(testCtx, backupID, metadataFilename)
			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.IsType(t, backup.ErrNotFound{}, err)
		})

		t.Run("put backup meta on storage", func(t *testing.T) {
			desc := &backup.BackupDescriptor{
				StartedAt:   time.Now(),
				CompletedAt: time.Time{},
				ID:          backupID,
				Classes: []backup.ClassDescriptor{
					{
						Name: className,
					},
				},
				Status: string(backup.Started),
			}

			b, err := json.Marshal(desc)
			require.Nil(t, err)

			err = gcs.PutObject(testCtx, backupID, metadataFilename, b)
			require.Nil(t, err)

			dest := gcs.HomeDir(backupID)
			expected := fmt.Sprintf("gs://%s/%s", bucketName, backupID)
			assert.Equal(t, expected, dest)
		})

		t.Run("assert backup meta contents", func(t *testing.T) {
			obj, err := gcs.GetObject(testCtx, backupID, metadataFilename)
			require.Nil(t, err)

			var meta backup.BackupDescriptor
			err = json.Unmarshal(obj, &meta)
			require.Nil(t, err)
			assert.NotEmpty(t, meta.StartedAt)
			assert.Empty(t, meta.CompletedAt)
			assert.Equal(t, meta.Status, string(backup.Started))
			assert.Empty(t, meta.Error)
			assert.Len(t, meta.Classes, 1)
			assert.Equal(t, meta.Classes[0].Name, className)
			assert.Nil(t, meta.Classes[0].Error)
		})
	})
}

func moduleLevelCopyObjects(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	key := "moduleLevelCopyObjects"
	backupID := "backup_id"
	bucketName := "bucket"
	projectID := "project-id"
	endpoint := os.Getenv(envGCSEndpoint)

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envGCSEndpoint, endpoint))
		require.Nil(t, os.Setenv(envGCSStorageEmulatorHost, endpoint))
		require.Nil(t, os.Setenv(envGCSCredentials, ""))
		require.Nil(t, os.Setenv(envGCSProjectID, projectID))
		require.Nil(t, os.Setenv(envGCSBucket, bucketName))

		createBucket(testCtx, t, projectID, bucketName)
	})

	t.Run("copy objects", func(t *testing.T) {
		gcsConfig := gcs.NewConfig(bucketName, "")
		gcs, err := gcs.New(testCtx, gcsConfig, dataDir)
		require.Nil(t, err)

		t.Run("put object to backet", func(t *testing.T) {
			err := gcs.PutObject(testCtx, backupID, key, []byte("hello"))
			assert.Nil(t, err)
		})

		t.Run("get object from backet", func(t *testing.T) {
			meta, err := gcs.GetObject(testCtx, backupID, key)
			assert.Nil(t, err)
			assert.Equal(t, []byte("hello"), meta)
		})
	})
}

func moduleLevelCopyFiles(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	key := "moduleLevelCopyFiles"
	backupID := "backup_id"
	bucketName := "backet"
	projectID := "project-id"
	endpoint := os.Getenv(envGCSEndpoint)

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envGCSEndpoint, endpoint))
		require.Nil(t, os.Setenv(envGCSStorageEmulatorHost, endpoint))
		require.Nil(t, os.Setenv(envGCSCredentials, ""))
		require.Nil(t, os.Setenv(envGCSProjectID, projectID))
		require.Nil(t, os.Setenv(envGCSBucket, bucketName))

		createBucket(testCtx, t, projectID, bucketName)
	})

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		gcsConfig := gcs.NewConfig(bucketName, "")
		gcs, err := gcs.New(testCtx, gcsConfig, dataDir)
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			assert.Equal(t, dataDir, gcs.SourceDataPath())
		})

		t.Run("copy file to storage", func(t *testing.T) {
			srcPath, _ := filepath.Rel(dataDir, fpath)
			err := gcs.PutFile(testCtx, backupID, key, srcPath)
			require.Nil(t, err)

			contents, err := gcs.GetObject(testCtx, backupID, key)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from storage", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			err := gcs.WriteToFile(testCtx, backupID, key, destPath)
			require.Nil(t, err)

			contents, err := os.ReadFile(destPath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})
	})
}
