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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/moduletools"
	mod "github.com/weaviate/weaviate/modules/backup-gcs"
	"github.com/weaviate/weaviate/test/docker"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_GCSBackend_Backup(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithGCS().Start(ctx)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "cannot start"))
	}

	t.Setenv(envGCSEndpoint, compose.GetGCS().URI())

	t.Run("store backup meta", moduleLevelStoreBackupMeta)
	t.Run("copy objects", moduleLevelCopyObjects)
	t.Run("copy files", moduleLevelCopyFiles)

	if err := compose.Terminate(ctx); err != nil {
		t.Fatal(errors.Wrapf(err, "failed to terminate test containers"))
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
	gcsUseAuth := "false"

	t.Log("setup env")
	t.Setenv(envGCSEndpoint, endpoint)
	t.Setenv(envGCSStorageEmulatorHost, endpoint)
	t.Setenv(envGCSCredentials, "")
	t.Setenv(envGCSProjectID, projectID)
	t.Setenv(envGCSBucket, bucketName)
	t.Setenv(envGCSUseAuth, gcsUseAuth)
	moduleshelper.CreateGCSBucket(testCtx, t, projectID, bucketName)

	t.Run("store backup meta in gcs", func(t *testing.T) {
		t.Setenv("BACKUP_GCS_BUCKET", bucketName)
		gcs := mod.New()
		err := gcs.Init(testCtx, newFakeModuleParams(dataDir))
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

		t.Run("put backup meta on backend", func(t *testing.T) {
			desc := &backup.BackupDescriptor{
				StartedAt:   time.Now(),
				CompletedAt: time.Time{},
				ID:          backupID,
				Classes: []backup.ClassDescriptor{
					{
						Name: className,
					},
				},
				Status:  string(backup.Started),
				Version: ubak.Version,
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
			assert.Equal(t, meta.Version, ubak.Version)
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
	gcsUseAuth := "false"

	t.Log("setup env")
	t.Setenv(envGCSEndpoint, endpoint)
	t.Setenv(envGCSStorageEmulatorHost, endpoint)
	t.Setenv(envGCSCredentials, "")
	t.Setenv(envGCSProjectID, projectID)
	t.Setenv(envGCSBucket, bucketName)
	t.Setenv(envGCSUseAuth, gcsUseAuth)
	moduleshelper.CreateGCSBucket(testCtx, t, projectID, bucketName)

	t.Run("copy objects", func(t *testing.T) {
		t.Setenv("BACKUP_GCS_BUCKET", bucketName)
		gcs := mod.New()
		err := gcs.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("put object to bucket", func(t *testing.T) {
			err := gcs.PutObject(testCtx, backupID, key, []byte("hello"))
			assert.Nil(t, err)
		})

		t.Run("get object from bucket", func(t *testing.T) {
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
	bucketName := "bucket"
	projectID := "project-id"
	endpoint := os.Getenv(envGCSEndpoint)
	gcsUseAuth := "false"

	t.Log("setup env")
	t.Setenv(envGCSEndpoint, endpoint)
	t.Setenv(envGCSStorageEmulatorHost, endpoint)
	t.Setenv(envGCSCredentials, "")
	t.Setenv(envGCSProjectID, projectID)
	t.Setenv(envGCSBucket, bucketName)
	t.Setenv(envGCSUseAuth, gcsUseAuth)
	moduleshelper.CreateGCSBucket(testCtx, t, projectID, bucketName)

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		t.Setenv("BACKUP_GCS_BUCKET", bucketName)
		gcs := mod.New()
		err = gcs.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			assert.Equal(t, dataDir, gcs.SourceDataPath())
		})

		t.Run("copy file to backend", func(t *testing.T) {
			srcPath, _ := filepath.Rel(dataDir, fpath)
			err := gcs.PutFile(testCtx, backupID, key, srcPath)
			require.Nil(t, err)

			contents, err := gcs.GetObject(testCtx, backupID, key)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from backend", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			err := gcs.WriteToFile(testCtx, backupID, key, destPath)
			require.Nil(t, err)

			contents, err := os.ReadFile(destPath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})
	})
}

type fakeModuleParams struct {
	logger   logrus.FieldLogger
	provider fakeStorageProvider
	config   config.Config
}

func newFakeModuleParams(dataPath string) *fakeModuleParams {
	logger, _ := logrustest.NewNullLogger()
	return &fakeModuleParams{
		logger:   logger,
		provider: fakeStorageProvider{dataPath: dataPath},
	}
}

func (f *fakeModuleParams) GetStorageProvider() moduletools.StorageProvider {
	return &f.provider
}

func (f *fakeModuleParams) GetAppState() interface{} {
	return nil
}

func (f *fakeModuleParams) GetLogger() logrus.FieldLogger {
	return f.logger
}

func (f *fakeModuleParams) GetConfig() config.Config {
	return f.config
}

type fakeStorageProvider struct {
	dataPath string
}

func (f *fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (f *fakeStorageProvider) DataPath() string {
	return f.dataPath
}
