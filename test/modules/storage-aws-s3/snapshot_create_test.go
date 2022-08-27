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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_S3Storage_StoreSnapshot(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	bucketName := "bucket"
	region := "eu-west-1"
	endpoint := os.Getenv(minioEndpoint)

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv("AWS_REGION", region))
		require.Nil(t, os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key"))
		require.Nil(t, os.Setenv("AWS_SECRET_KEY", "aws_secret_key"))
		require.Nil(t, os.Setenv("STORAGE_S3_BUCKET", bucketName))

		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewEnvAWS(),
			Region: region,
			Secure: false,
		})
		require.Nil(t, err)

		err = client.MakeBucket(testCtx, bucketName, minio.MakeBucketOptions{})
		minioErr, ok := err.(minio.ErrorResponse)
		if ok {
			// the bucket persists from a previous test.
			// if the bucket already exists, we can proceed
			if minioErr.Code == "BucketAlreadyOwnedByYou" {
				return
			}
		}
		require.Nil(t, err)
	})

	t.Run("store snapshot in s3", func(t *testing.T) {
		logger, _ := test.NewNullLogger()

		s3Config := s3.NewConfig(endpoint, bucketName, "", false)
		path, _ := os.Getwd()
		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		snapshot, err := s3.InitSnapshot(testCtx, className, snapshotID)
		require.Nil(t, err)

		err = s3.StoreSnapshot(testCtx, snapshot)
		assert.Nil(t, err)

		dest := s3.DestinationPath(className, snapshotID)
		expected := fmt.Sprintf("s3://%s/%s/%s/snapshot.json", bucketName, className, snapshotID)
		assert.Equal(t, expected, dest)

		t.Run("assert snapshot meta contents", func(t *testing.T) {
			meta, err := s3.GetMeta(testCtx, className, snapshotID)
			require.Nil(t, err)
			assert.NotEmpty(t, meta.StartedAt)
			assert.Empty(t, meta.CompletedAt)
			assert.Equal(t, meta.Status, string(snapshots.CreateStarted))
			assert.Empty(t, meta.Error)
		})
	})

	t.Run("restores snapshot data from S3", func(t *testing.T) {
		s3Config := s3.NewConfig(endpoint, "bucket", "", false)
		logger, _ := test.NewNullLogger()
		path, _ := os.Getwd()
		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		// List all files in testDir
		files, _ := os.ReadDir(testDir)

		// Remove the files, ready for restore
		for _, f := range files {
			require.Nil(t, os.Remove(filepath.Join(testDir, f.Name())))
			assert.NoFileExists(t, filepath.Join(testDir, f.Name()))
		}

		// Use the previous test snapshot to test the restore function
		_, err = s3.RestoreSnapshot(testCtx, className, snapshotID)
		require.Nil(t, err)

		assert.DirExists(t, path)

		// Check that every file in the snapshot exists in testDir
		for _, filePath := range files {
			expectedFilePath := filepath.Join(testDir, filePath.Name())
			assert.FileExists(t, expectedFilePath)
		}

		dest := s3.DestinationPath(className, snapshotID)
		expected := fmt.Sprintf("s3://%s/%s/%s/snapshot.json", bucketName, className, snapshotID)
		assert.Equal(t, expected, dest)
	})
}

func Test_S3Storage_MetaStatus(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	createTestFiles(t, testDir)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	bucketName := "bucket"
	region := "eu-west-1"
	endpoint := os.Getenv(minioEndpoint)

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv("AWS_REGION", region))
		require.Nil(t, os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key"))
		require.Nil(t, os.Setenv("AWS_SECRET_KEY", "aws_secret_key"))
		require.Nil(t, os.Setenv("STORAGE_S3_BUCKET", bucketName))

		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewEnvAWS(),
			Region: region,
			Secure: false,
		})
		require.Nil(t, err)

		err = client.MakeBucket(testCtx, bucketName, minio.MakeBucketOptions{})
		minioErr, ok := err.(minio.ErrorResponse)
		if ok {
			// the bucket persists from a previous test.
			// if the bucket already exists, we can proceed
			if minioErr.Code == "BucketAlreadyOwnedByYou" {
				return
			}
		}
		require.Nil(t, err)
	})

	s3Config := s3.NewConfig(endpoint, bucketName, "", false)
	logger, _ := test.NewNullLogger()

	t.Run("store snapshot in s3", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, testDir)
		require.Nil(t, err)

		snapshot, err := s3.InitSnapshot(testCtx, className, snapshotID)
		require.Nil(t, err)

		err = s3.StoreSnapshot(testCtx, snapshot)
		assert.Nil(t, err)
	})

	t.Run("set snapshot status", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, testDir)
		require.Nil(t, err)

		err = s3.SetMetaStatus(testCtx, className, snapshotID, "STARTED")
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, testDir)
		require.Nil(t, err)

		meta, err := s3.GetMeta(testCtx, className, snapshotID)
		assert.Nil(t, err)
		assert.Equal(t, "STARTED", meta.Status)
	})
}
