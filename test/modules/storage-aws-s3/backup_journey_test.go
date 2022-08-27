package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-aws-s3"
	"github.com/semi-technologies/weaviate/test/docker"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	s3BackupJourneyClassName  = "S3Backup"
	s3BackupJourneySnapshotID = "s3-snapshot"
	s3BackupJourneyBucketName = "snapshots"
	s3BackupJourneyRegion     = "eu-west-1"
	s3BackupJourneyAccessKey  = "aws_access_key"
	s3BackupJourneySecretKey  = "aws_secret_key"
)

func Test_BackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Run("pre-instance env setup", func(t *testing.T) {
		require.Nil(t, os.Setenv("TEST_WEAVIATE_IMAGE", "weaviate:module-tests"))
		require.Nil(t, os.Setenv(envS3AccessKey, s3BackupJourneyAccessKey))
		require.Nil(t, os.Setenv(envS3SecretKey, s3BackupJourneySecretKey))
		require.Nil(t, os.Setenv(envS3Bucket, s3BackupJourneyBucketName))
	})

	compose, err := docker.New().
		WithMinIO().
		WithText2VecContextionary().
		WithWeaviate().
		Start(ctx)
	require.Nil(t, err)

	t.Run("post-instance env setup", func(t *testing.T) {
		createBucket(ctx, t, compose.GetMinIO().URI(), s3BackupJourneyRegion, s3BackupJourneyBucketName)
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	// add test data
	addTestClass(t, s3BackupJourneyClassName)
	addTestObjects(t, s3BackupJourneyClassName)

	// journey tests
	t.Run("single shard backup with AWS S3", singleShardBackupJourneyWithS3)

	t.Run("cleanup", func(t *testing.T) {
		// class cleanup -- might not need this
		// since containers are ephemeral here
		helper.DeleteClass(t, s3BackupJourneyClassName)

		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminte test containers: %s", err.Error())
		}
	})
}

func singleShardBackupJourneyWithS3(t *testing.T) {
	// create
	helper.CreateBackup(t, s3BackupJourneyClassName, modstgs3.Name, s3BackupJourneySnapshotID)

	// wait for create success
	{
		createTime := time.Now()
		for {
			if time.Now().After(createTime.Add(10 * time.Second)) {
				break
			}

			status := helper.CreateBackupStatus(t, s3BackupJourneyClassName, modstgs3.Name, s3BackupJourneySnapshotID)
			require.NotNil(t, status)
			if *status.Status == string(snapshots.CreateSuccess) {
				break
			}
		}

		createStatus := helper.CreateBackupStatus(t, s3BackupJourneyClassName, modstgs3.Name, s3BackupJourneySnapshotID)
		require.NotNil(t, createStatus)
		require.Equal(t, *createStatus.Status, string(snapshots.CreateSuccess))
	}

	// remove the class so we can restore it
	helper.DeleteClass(t, s3BackupJourneyClassName)

	// restore
	helper.RestoreBackup(t, s3BackupJourneyClassName, modstgs3.Name, s3BackupJourneySnapshotID)

	// wait for restore success
	{
		restoreTime := time.Now()
		for {
			if time.Now().After(restoreTime.Add(10 * time.Second)) {
				break
			}

			status := helper.RestoreBackupStatus(t, s3BackupJourneyClassName, modstgs3.Name, s3BackupJourneySnapshotID)
			require.NotNil(t, status)
			if *status.Status == string(snapshots.CreateSuccess) {
				break
			}
		}

		restoreStatus := helper.RestoreBackupStatus(t, s3BackupJourneyClassName, modstgs3.Name, s3BackupJourneySnapshotID)
		require.NotNil(t, restoreStatus)
		require.Equal(t, *restoreStatus.Status, string(snapshots.CreateSuccess))
	}

	// assert class exists again it its entirety
	count := moduleshelper.GetClassCount(t, s3BackupJourneyClassName)
	assert.Equal(t, int64(500), count)
}
