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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
	"github.com/semi-technologies/weaviate/test/docker"
	"github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_S3Storage_SnapshotCreate(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithMinIO().Start(ctx)
	if err != nil {
		panic(errors.Wrapf(err, "cannot start"))
	}

	require.Nil(t, os.Setenv(envMinioEndpoint, compose.GetMinIO().URI()))

	t.Run("store snapshot", moduleLevelStoreSnapshot)
	t.Run("get meta status", moduleLevelGetMetaStatus)

	if err := compose.Terminate(ctx); err != nil {
		t.Fatalf("failed to terminte test containers: %s", err.Error())
	}
}

func moduleLevelStoreSnapshot(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testdataMainDir := "./testData"
	testDir := moduleshelper.MakeTestDir(t, testdataMainDir)
	defer moduleshelper.RemoveDir(t, testdataMainDir)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
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

func moduleLevelGetMetaStatus(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testdataMainDir := "./testData"
	testDir := moduleshelper.MakeTestDir(t, testdataMainDir)
	defer moduleshelper.RemoveDir(t, testdataMainDir)

	moduleshelper.CreateTestFiles(t, testDir)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
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
