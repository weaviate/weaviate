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

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
	"github.com/semi-technologies/weaviate/test/docker"
	moduleshelper "github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func Test_GCSStorage_SnapshotCreate(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithGCS().Start(ctx)
	if err != nil {
		panic(errors.Wrapf(err, "cannot start"))
	}

	require.Nil(t, os.Setenv(envGCSEndpoint, compose.GetGCS().URI()))

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

	path, err := os.Getwd()
	require.Nil(t, err)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	endpoint := os.Getenv(envGCSEndpoint)
	bucketName := "weaviate-snapshots"
	projectID := "project-id"

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envGCSEndpoint, endpoint))
		require.Nil(t, os.Setenv(envGCSStorageEmulatorHost, endpoint))
		require.Nil(t, os.Setenv(envGCSCredentials, ""))
		require.Nil(t, os.Setenv(envGCSProjectID, projectID))
		require.Nil(t, os.Setenv(envGCSBucket, bucketName))

		createBucket(testCtx, t, projectID, bucketName)
	})

	t.Run("store snapshot in gcs", func(t *testing.T) {
		gcsConfig := gcs.NewConfig(bucketName, "")
		path, _ := os.Getwd()

		gcs, err := gcs.New(testCtx, gcsConfig, path)
		require.Nil(t, err)

		snapshot, err := gcs.InitSnapshot(testCtx, className, snapshotID)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(testCtx, snapshot)
		require.Nil(t, err)

		dest := gcs.DestinationPath(className, snapshotID)
		expected := fmt.Sprintf("gs://%s/%s/%s/snapshot.json", bucketName, className, snapshotID)
		assert.Equal(t, expected, dest)

		t.Run("assert snapshot meta contents", func(t *testing.T) {
			meta, err := gcs.GetMeta(testCtx, className, snapshotID)
			require.Nil(t, err)
			assert.NotEmpty(t, meta.StartedAt)
			assert.Empty(t, meta.CompletedAt)
			assert.Equal(t, meta.Status, string(backup.CreateStarted))
			assert.Empty(t, meta.Error)
		})
	})

	t.Run("restore snapshot in gcs", func(t *testing.T) {
		gcsConfig := gcs.NewConfig(bucketName, "")
		gcs, err := gcs.New(testCtx, gcsConfig, path)
		require.Nil(t, err)

		// List all files in testDir
		files, _ := os.ReadDir(testDir)

		// Remove the files, ready for restore
		for _, f := range files {
			require.Nil(t, os.Remove(filepath.Join(testDir, f.Name())))
			assert.NoFileExists(t, filepath.Join(testDir, f.Name()))
		}

		_, err = gcs.RestoreSnapshot(testCtx, "SnapshotClass", "snapshot_id")
		assert.Nil(t, err)

		// Check that every file in the snapshot exists in testDir
		for _, filePath := range files {
			expectedFilePath := filepath.Join(testDir, filePath.Name())
			assert.FileExists(t, expectedFilePath)
		}

		dest := gcs.DestinationPath(className, snapshotID)
		expected := fmt.Sprintf("gs://%s/%s/%s/snapshot.json", bucketName, className, snapshotID)
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
	endpoint := os.Getenv(envGCSEndpoint)
	bucketName := "weaviate-snapshots"
	projectID := "project-id"

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv(envGCSEndpoint, endpoint))
		require.Nil(t, os.Setenv(envGCSStorageEmulatorHost, endpoint))
		require.Nil(t, os.Setenv(envGCSCredentials, ""))
		require.Nil(t, os.Setenv(envGCSProjectID, projectID))
		require.Nil(t, os.Setenv(envGCSBucket, bucketName))

		createBucket(testCtx, t, projectID, bucketName)
	})

	gcsConfig := gcs.NewConfig(bucketName, "")

	t.Run("store snapshot in gcs", func(t *testing.T) {
		gcs, err := gcs.New(testCtx, gcsConfig, testDir)
		require.Nil(t, err)

		snapshot, err := gcs.InitSnapshot(testCtx, className, snapshotID)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(testCtx, snapshot)
		assert.Nil(t, err)
	})

	t.Run("set snapshot status", func(t *testing.T) {
		client, err := storage.NewClient(testCtx, option.WithoutAuthentication())
		require.Nil(t, err)

		if _, err := client.Bucket(bucketName).Attrs(testCtx); err != nil {
			t.Fatal(err.Error())
		}

		gcs, err := gcs.New(testCtx, gcsConfig, testDir)
		require.Nil(t, err)

		err = gcs.SetMetaStatus(testCtx, className, snapshotID, "STARTED")
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		gcs, err := gcs.New(testCtx, gcsConfig, testDir)
		require.Nil(t, err)

		meta, err := gcs.GetMeta(testCtx, className, snapshotID)
		require.Nil(t, err)
		assert.Equal(t, "STARTED", meta.Status)
	})
}
