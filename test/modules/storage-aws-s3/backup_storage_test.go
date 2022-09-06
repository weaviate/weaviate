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
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
	"github.com/semi-technologies/weaviate/test/docker"
	moduleshelper "github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_S3Storage_Backup(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithMinIO().Start(ctx)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "cannot start"))
	}

	require.Nil(t, os.Setenv(envMinioEndpoint, compose.GetMinIO().URI()))

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
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)
	metadataFilename := "backup.json"

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envAwsRegion, region))
		require.Nil(t, os.Setenv(envS3AccessKey, "aws_access_key"))
		require.Nil(t, os.Setenv(envS3SecretKey, "aws_secret_key"))
		require.Nil(t, os.Setenv(envS3Bucket, bucketName))

		createBucket(testCtx, t, endpoint, region, bucketName)
	})

	t.Run("store backup meta in s3", func(t *testing.T) {
		s3Config := s3.NewConfig(endpoint, bucketName, "", false)
		logger, _ := test.NewNullLogger()
		s3, err := s3.New(s3Config, logger, dataDir)
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

			err = s3.PutObject(testCtx, backupID, metadataFilename, b)
			require.Nil(t, err)

			dest := s3.DestinationPath(backupID)
			expected := fmt.Sprintf("s3://%s/%s/snapshot.json", bucketName, backupID)
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
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envAwsRegion, region))
		require.Nil(t, os.Setenv(envS3AccessKey, "aws_access_key"))
		require.Nil(t, os.Setenv(envS3SecretKey, "aws_secret_key"))
		require.Nil(t, os.Setenv(envS3Bucket, bucketName))

		createBucket(testCtx, t, endpoint, region, bucketName)
	})

	t.Run("copy objects", func(t *testing.T) {
		s3Config := s3.NewConfig(endpoint, bucketName, "", false)
		logger, _ := test.NewNullLogger()
		s3, err := s3.New(s3Config, logger, dataDir)
		require.Nil(t, err)

		t.Run("put object to backet", func(t *testing.T) {
			err := s3.PutObject(testCtx, backupID, key, []byte("hello"))
			assert.Nil(t, err)
		})

		t.Run("get object from backet", func(t *testing.T) {
			meta, err := s3.GetObject(testCtx, backupID, key)
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
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envAwsRegion, region))
		require.Nil(t, os.Setenv(envS3AccessKey, "aws_access_key"))
		require.Nil(t, os.Setenv(envS3SecretKey, "aws_secret_key"))
		require.Nil(t, os.Setenv(envS3Bucket, bucketName))

		createBucket(testCtx, t, endpoint, region, bucketName)
	})

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		s3Config := s3.NewConfig(endpoint, bucketName, "", false)
		logger, _ := test.NewNullLogger()
		s3, err := s3.New(s3Config, logger, dataDir)
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			assert.Equal(t, dataDir, s3.SourceDataPath())
		})

		t.Run("copy file to storage", func(t *testing.T) {
			srcPath, _ := filepath.Rel(dataDir, fpath)
			err := s3.PutFile(testCtx, backupID, key, srcPath)
			require.Nil(t, err)

			contents, err := s3.GetObject(testCtx, backupID, key)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from storage", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			err := s3.WriteToFile(testCtx, backupID, key, destPath)
			require.Nil(t, err)

			contents, err := os.ReadFile(destPath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})
	})
}
