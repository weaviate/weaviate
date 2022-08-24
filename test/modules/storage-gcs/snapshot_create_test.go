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

	"cloud.google.com/go/storage"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func Test_GCSStorage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	path, err := os.Getwd()
	require.Nil(t, err)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	endpoint := os.Getenv(gcsEndpoint)
	bucketName := "weaviate-snapshots"
	projectID := "project-id"

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv("GCS_ENDPOINT", endpoint))
		require.Nil(t, os.Setenv("STORAGE_EMULATOR_HOST", endpoint))
		require.Nil(t, os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
		require.Nil(t, os.Setenv("GOOGLE_CLOUD_PROJECT", projectID))
		require.Nil(t, os.Setenv("STORAGE_GCS_BUCKET", bucketName))

		client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
		require.Nil(t, err)

		err = client.Bucket(bucketName).Create(context.Background(), projectID, nil)
		require.Nil(t, err)
	})

	t.Run("store snapshot in gcs", func(t *testing.T) {
		ctxSnapshot := context.Background()

		gcsConfig := gcs.NewConfig(bucketName, "")
		path, _ := os.Getwd()

		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		snapshot, err := gcs.InitSnapshot(ctxSnapshot, className, snapshotID)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(ctxSnapshot, snapshot)
		require.Nil(t, err)

		dest := gcs.DestinationPath(className, snapshotID)
		expected := fmt.Sprintf("gs://%s/%s/%s/snapshot.json", bucketName, className, snapshotID)
		assert.Equal(t, expected, dest)

		t.Run("assert snapshot meta contents", func(t *testing.T) {
			meta, err := gcs.GetMeta(context.Background(), className, snapshotID)
			require.Nil(t, err)
			assert.NotEmpty(t, meta.StartedAt)
			assert.Empty(t, meta.CompletedAt)
			assert.Equal(t, meta.Status, string(snapshots.CreateStarted))
			assert.Empty(t, meta.Error)
		})
	})

	t.Run("restore snapshot in gcs", func(t *testing.T) {
		ctxSnapshot := context.Background()

		gcsConfig := gcs.NewConfig(bucketName, "")
		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		// List all files in testDir
		files, _ := os.ReadDir(testDir)

		// Remove the files, ready for restore
		for _, f := range files {
			require.Nil(t, os.Remove(filepath.Join(testDir, f.Name())))
			assert.NoFileExists(t, filepath.Join(testDir, f.Name()))
		}

		_, err = gcs.RestoreSnapshot(ctxSnapshot, "SnapshotClass", "snapshot_id")
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

func Test_GCSStorage_MetaStatus(t *testing.T) {
	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	createTestFiles(t, testDir)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	endpoint := os.Getenv(gcsEndpoint)
	bucketName := "weaviate-snapshots"
	projectID := "project-id"

	t.Run("setup env", func(t *testing.T) {
		require.Nil(t, os.Setenv("GCS_ENDPOINT", endpoint))
		require.Nil(t, os.Setenv("STORAGE_EMULATOR_HOST", endpoint))
		require.Nil(t, os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
		require.Nil(t, os.Setenv("GOOGLE_CLOUD_PROJECT", projectID))
		require.Nil(t, os.Setenv("STORAGE_GCS_BUCKET", bucketName))

		client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
		require.Nil(t, err)

		err = client.Bucket(bucketName).Create(context.Background(), projectID, nil)
		require.Nil(t, err)
	})

	gcsConfig := gcs.NewConfig(bucketName, "")

	t.Run("store snapshot in gcs", func(t *testing.T) {
		ctxSnapshot := context.Background()

		gcs, err := gcs.New(context.Background(), gcsConfig, testDir)
		require.Nil(t, err)

		snapshot, err := gcs.InitSnapshot(ctxSnapshot, className, snapshotID)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})

	t.Run("set snapshot status", func(t *testing.T) {
		client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
		require.Nil(t, err)

		if _, err := client.Bucket(bucketName).Attrs(context.Background()); err != nil {
			t.Fatal(err.Error())
		}

		gcs, err := gcs.New(context.Background(), gcsConfig, testDir)
		require.Nil(t, err)

		err = gcs.SetMetaStatus(context.Background(), className, snapshotID, "STARTED")
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		gcs, err := gcs.New(context.Background(), gcsConfig, testDir)
		require.Nil(t, err)

		meta, err := gcs.GetMeta(context.Background(), className, snapshotID)
		require.Nil(t, err)
		assert.Equal(t, "STARTED", meta.Status)
	})
}
