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
	"os"
	"path/filepath"
	"testing"

	s3 "github.com/semi-technologies/weaviate/modules/storage-aws-s3/s3"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_S3Storage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	require.Nil(t, os.Setenv("AWS_REGION", "eu-west-1"))
	require.Nil(t, os.Setenv("AWS_ACCESS_KEY", "aws_access_key"))
	require.Nil(t, os.Setenv("AWS_SECRET_KEY", "aws_secret_key"))

	t.Run("store snapshot in s3", func(t *testing.T) {
		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		logger, _ := test.NewNullLogger()

		endpoint := os.Getenv(minioEndpoint)
		s3Config := s3.NewConfig(endpoint, "bucket", false)
		path, _ := os.Getwd()
		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		err = s3.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})

	t.Run("restores snapshot data from S3", func(t *testing.T) {
		ctxSnapshot := context.Background()

		endpoint := os.Getenv(minioEndpoint)
		s3Config := s3.NewConfig(endpoint, "bucket", false)
		logger, _ := test.NewNullLogger()
		path, _ := os.Getwd()
		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		// List all files in testDir
		files, _ := os.ReadDir(testDir)

		// Remove the files, ready for restore
		for _, f := range files {
			os.Remove(filepath.Join(testDir, f.Name()))
			assert.NoFileExists(t, filepath.Join(testDir, f.Name()))
		}

		// Use the previous test snapshot to test the restore function
		err = s3.RestoreSnapshot(ctxSnapshot, "SnapshotClass", "snapshot_id")
		require.Nil(t, err)

		assert.DirExists(t, path)

		// Check that every file in the snapshot exists in testDir
		for _, filePath := range files {
			expectedFilePath := filepath.Join(testDir, filePath.Name())
			assert.FileExists(t, expectedFilePath)
		}
	})
}

func Test_S3Storage_MetaStatus(t *testing.T) {
	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	require.Nil(t, os.Setenv("AWS_REGION", "eu-west-1"))
	require.Nil(t, os.Setenv("AWS_ACCESS_KEY", "aws_access_key"))
	require.Nil(t, os.Setenv("AWS_SECRET_KEY", "aws_secret_key"))

	endpoint := os.Getenv(minioEndpoint)
	s3Config := s3.NewConfig(endpoint, "bucket", false)
	logger, _ := test.NewNullLogger()
	path, _ := os.Getwd()

	t.Run("store snapshot in gcs", func(t *testing.T) {
		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		err = s3.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})

	t.Run("set snapshot status", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		err = s3.SetMetaStatus(context.Background(), "SnapshotClass", "snapshot_id", "STARTED")
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, path)
		require.Nil(t, err)

		status, err := s3.GetMetaStatus(context.Background(), "SnapshotClass", "snapshot_id")
		assert.Nil(t, err)
		assert.Equal(t, "STARTED", status)
	})
}
