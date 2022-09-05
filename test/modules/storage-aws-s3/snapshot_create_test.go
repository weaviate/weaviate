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

// TODO: test remaining methods

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
	// testDir := moduleshelper.MakeTestDir(t, testdataMainDir)
	defer moduleshelper.RemoveDir(t, testdataMainDir)

	className := "SnapshotClass"
	backupID := "backup_id"
	bucketName := "bucket"
	region := "eu-west-1"
	endpoint := os.Getenv(envMinioEndpoint)
	metadataFilename := "snapshot.json"

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

		meta, err := s3.GetObject(testCtx, backupID, metadataFilename)
		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrNotFound{}, err)

		err = s3.Initialize(testCtx, backupID)
		assert.Nil(t, err)

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

		t.Run("assert snapshot meta contents", func(t *testing.T) {
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

func moduleLevelGetMetaStatus(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testdataMainDir := "./testData"
	testDir := moduleshelper.MakeTestDir(t, testdataMainDir)
	defer moduleshelper.RemoveDir(t, testdataMainDir)

	moduleshelper.CreateTestFiles(t, testDir)

	key := "moduleLevelGetMetaStatus"
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

	t.Run("set snapshot status", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, testDir)
		require.Nil(t, err)

		err = s3.PutObject(testCtx, snapshotID, key, []byte("hello"))
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		s3, err := s3.New(s3Config, logger, testDir)
		require.Nil(t, err)

		meta, err := s3.GetObject(testCtx, snapshotID, key)
		assert.Nil(t, err)
		assert.Equal(t, []byte("hello"), meta)
	})
}
