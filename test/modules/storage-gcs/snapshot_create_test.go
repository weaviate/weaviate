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

	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GCSStorage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	require.Nil(t, os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
	require.Nil(t, os.Setenv("GOOGLE_CLOUD_PROJECT", "project-id"))
	require.Nil(t, os.Setenv("STORAGE_EMULATOR_HOST", os.Getenv(gcsEndpoint)))

	path, err := os.Getwd()
	require.Nil(t, err)

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	bucketName := "bucket"

	t.Run("store snapshot in gcs", func(t *testing.T) {
		snapshot := createSnapshotInstance(t, testDir, className, snapshotID)
		ctxSnapshot := context.Background()

		gcsConfig := gcs.NewConfig(bucketName)
		path, _ := os.Getwd()

		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)

		dest := gcs.DestinationPath(className, snapshotID)
		expected := fmt.Sprintf("gs://%s/%s/%s/snapshot.json", bucketName, className, snapshotID)
		assert.Equal(t, expected, dest)
	})

	t.Run("restore snapshot in gcs", func(t *testing.T) {
		ctxSnapshot := context.Background()

		gcsConfig := gcs.NewConfig(bucketName)
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

	require.Nil(t, os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
	require.Nil(t, os.Setenv("GOOGLE_CLOUD_PROJECT", "project-id"))
	require.Nil(t, os.Setenv("STORAGE_EMULATOR_HOST", os.Getenv(gcsEndpoint)))

	className := "SnapshotClass"
	snapshotID := "snapshot_id"
	bucketName := "bucket"

	gcsConfig := gcs.NewConfig(bucketName)
	path, err := os.Getwd()
	require.Nil(t, err)

	t.Run("store snapshot in gcs", func(t *testing.T) {
		snapshot := createSnapshotInstance(t, testDir, className, snapshotID)
		ctxSnapshot := context.Background()

		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})

	t.Run("set snapshot status", func(t *testing.T) {
		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		err = gcs.SetMetaStatus(context.Background(), "SnapshotClass", "snapshot_id", "STARTED")
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		status, err := gcs.GetMetaStatus(context.Background(), "SnapshotClass", "snapshot_id")
		assert.Nil(t, err)
		assert.Equal(t, "STARTED", status)
	})
}
