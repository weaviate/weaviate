package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/snapshots"
	modstggcs "github.com/semi-technologies/weaviate/modules/storage-gcs"
	"github.com/semi-technologies/weaviate/test/docker"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	gcsBackupJourneyClassName  = "GcsBackup"
	gcsBackupJourneySnapshotID = "gcs-snapshot"
	gcsBackupJourneyProjectID  = "gcs-backup-journey"
	gcsBackupJourneyBucketName = "snapshots"
)

func Test_BackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Run("pre-instance env setup", func(t *testing.T) {
		require.Nil(t, os.Setenv("TEST_WEAVIATE_IMAGE", "weaviate:module-tests"))
		require.Nil(t, os.Setenv(envGcsCredentials, ""))
		require.Nil(t, os.Setenv(envGcsProjectID, gcsBackupJourneyProjectID))
		require.Nil(t, os.Setenv(envGcsBucket, gcsBackupJourneyBucketName))
	})

	compose, err := docker.New().
		WithGCS().
		WithText2VecContextionary().
		WithWeaviate().
		Start(ctx)
	require.Nil(t, err)

	t.Run("post-instance env setup", func(t *testing.T) {
		require.Nil(t, os.Setenv(envGcsEndpoint, compose.GetGCS().URI()))
		require.Nil(t, os.Setenv(envGcsStorageEmulatorHost, compose.GetGCS().URI()))

		createBucket(ctx, t, gcsBackupJourneyProjectID, gcsBackupJourneyBucketName)
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	// add test data
	addTestClass(t, gcsBackupJourneyClassName)
	addTestData(t, gcsBackupJourneyClassName)

	// journey tests
	t.Run("single shard backup with GCS", singleShardBackupJourneyWithGCS)

	t.Run("cleanup", func(t *testing.T) {
		// class cleanup -- might not need this
		// since containers are ephemeral here
		deleteClass(t, gcsBackupJourneyClassName)

		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminte test containers: %s", err.Error())
		}
	})
}

func singleShardBackupJourneyWithGCS(t *testing.T) {
	// create
	helper.CreateBackup(t, gcsBackupJourneyClassName, modstggcs.Name, gcsBackupJourneySnapshotID)

	// wait for create success
	{
		createTime := time.Now()
		for {
			if time.Now().After(createTime.Add(10 * time.Second)) {
				break
			}

			status := getCreateBackupStatus(t, gcsBackupJourneyClassName, modstggcs.Name, gcsBackupJourneySnapshotID)
			require.NotNil(t, status)
			if *status.Status == string(snapshots.CreateSuccess) {
				break
			}
		}

		createStatus := getCreateBackupStatus(t, gcsBackupJourneyClassName, modstggcs.Name, gcsBackupJourneySnapshotID)
		require.NotNil(t, createStatus)
		require.Equal(t, *createStatus.Status, string(snapshots.CreateSuccess))
	}

	// remove the class so we can restore it
	deleteClass(t, gcsBackupJourneyClassName)

	// restore
	helper.RestoreBackup(t, gcsBackupJourneyClassName, modstggcs.Name, gcsBackupJourneySnapshotID)

	// wait for restore success
	{
		restoreTime := time.Now()
		for {
			if time.Now().After(restoreTime.Add(10 * time.Second)) {
				break
			}

			status := getRestoreBackupStatus(t, gcsBackupJourneyClassName, modstggcs.Name, gcsBackupJourneySnapshotID)
			require.NotNil(t, status)
			if *status.Status == string(snapshots.CreateSuccess) {
				break
			}
		}

		restoreStatus := getRestoreBackupStatus(t, gcsBackupJourneyClassName, modstggcs.Name, gcsBackupJourneySnapshotID)
		require.NotNil(t, restoreStatus)
		require.Equal(t, *restoreStatus.Status, string(snapshots.CreateSuccess))
	}

	// assert class exists again it its entirety
	count := getClassCount(t, gcsBackupJourneyClassName)
	assert.Equal(t, int64(500), count)
}
