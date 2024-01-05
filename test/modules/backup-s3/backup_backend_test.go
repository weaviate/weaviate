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
	mod "github.com/weaviate/weaviate/modules/backup-s3"
	"github.com/weaviate/weaviate/test/docker"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_S3Backend_Backup(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithMinIO().Start(ctx)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "cannot start"))
	}

	t.Setenv(envMinioEndpoint, compose.GetMinIO().URI())

	t.Run("store backup meta", moduleLevelStoreBackupMeta)
	t.Run("copy objects", moduleLevelCopyObjects)
	t.Run("copy files", moduleLevelCopyFiles)

	if err := compose.Terminate(ctx); err != nil {
		t.Fatal(errors.Wrapf(err, "failed to terminate test containers"))
	}
}

func moduleLevelStoreBackupMeta(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	className := "BackupClass"
	backupID := "backup_id"
	bucketName := "bucket"
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)
	metadataFilename := "backup.json"

	t.Log("setup env")
	t.Setenv(envAwsRegion, region)
	t.Setenv(envS3AccessKey, "aws_access_key")
	t.Setenv(envS3SecretKey, "aws_secret_key")
	t.Setenv(envS3Bucket, bucketName)
	createBucket(testCtx, t, endpoint, region, bucketName)

	t.Run("store backup meta in s3", func(t *testing.T) {
		t.Setenv(envS3UseSSL, "false")
		t.Setenv(envS3Endpoint, endpoint)
		s3 := mod.New()
		err := s3.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("access permissions", func(t *testing.T) {
			err := s3.Initialize(testCtx, backupID)
			assert.Nil(t, err)
		})

		t.Run("backup meta does not exist yet", func(t *testing.T) {
			meta, err := s3.GetObject(testCtx, backupID, metadataFilename)
			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.IsType(t, backup.ErrNotFound{}, err)
		})

		t.Run("put backup meta in backend", func(t *testing.T) {
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

			err = s3.PutObject(testCtx, backupID, metadataFilename, b)
			require.Nil(t, err)

			dest := s3.HomeDir(backupID)
			expected := fmt.Sprintf("s3://%s/%s", bucketName, backupID)
			assert.Equal(t, expected, dest)
		})

		t.Run("assert backup meta contents", func(t *testing.T) {
			obj, err := s3.GetObject(testCtx, backupID, metadataFilename)
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
	testCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	key := "moduleLevelCopyObjects"
	backupID := "backup_id"
	bucketName := "bucket"
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)

	t.Log("setup env")
	t.Setenv(envAwsRegion, region)
	t.Setenv(envS3AccessKey, "aws_access_key")
	t.Setenv(envS3SecretKey, "aws_secret_key")
	t.Setenv(envS3Bucket, bucketName)
	createBucket(testCtx, t, endpoint, region, bucketName)

	t.Run("copy objects", func(t *testing.T) {
		t.Setenv(envS3UseSSL, "false")
		t.Setenv(envS3Endpoint, endpoint)
		s3 := mod.New()
		err := s3.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("put object to bucket", func(t *testing.T) {
			err := s3.PutObject(testCtx, backupID, key, []byte("hello"))
			assert.Nil(t, err, "expected nil, got: %v", err)
		})

		t.Run("get object from bucket", func(t *testing.T) {
			meta, err := s3.GetObject(testCtx, backupID, key)
			assert.Nil(t, err, "expected nil, got: %v", err)
			assert.Equal(t, []byte("hello"), meta)
		})
	})
}

func moduleLevelCopyFiles(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	key := "moduleLevelCopyFiles"
	backupID := "backup_id"
	bucketName := "bucket"
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)

	t.Log("setup env")
	t.Setenv(envAwsRegion, region)
	t.Setenv(envS3AccessKey, "aws_access_key")
	t.Setenv(envS3SecretKey, "aws_secret_key")
	t.Setenv(envS3Bucket, bucketName)
	createBucket(testCtx, t, endpoint, region, bucketName)

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		t.Setenv(envS3UseSSL, "false")
		t.Setenv(envS3Endpoint, endpoint)
		s3 := mod.New()
		err = s3.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			assert.Equal(t, dataDir, s3.SourceDataPath())
		})

		t.Run("copy file to backend", func(t *testing.T) {
			srcPath, _ := filepath.Rel(dataDir, fpath)
			err := s3.PutFile(testCtx, backupID, key, srcPath)
			require.Nil(t, err)

			contents, err := s3.GetObject(testCtx, backupID, key)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from backend", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			err := s3.WriteToFile(testCtx, backupID, key, destPath)
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
