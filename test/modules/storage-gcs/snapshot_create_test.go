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

	"github.com/semi-technologies/weaviate/modules/storage-gcs/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GCSStorage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	os.Setenv("GOOGLE_CLOUD_PROJECT", "project-id")
	os.Setenv("STORAGE_EMULATOR_HOST", os.Getenv(gcsEndpoint))
	path, _ := os.Getwd()

	t.Run("store snapshot in gcs", func(t *testing.T) {
		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		gcsConfig := gcs.NewConfig("")
		path, _ := os.Getwd()

		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		err = gcs.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})

	t.Run("restore snapshot in gcs", func(t *testing.T) {
		ctxSnapshot := context.Background()

		gcsConfig := gcs.NewConfig("")
		gcs, err := gcs.New(context.Background(), gcsConfig, path)
		require.Nil(t, err)

		// List all files in testDir
		files, _ := os.ReadDir(testDir)

		// Remove the files, ready for restore
		for _, f := range files {
			os.Remove(filepath.Join(testDir, f.Name()))
			assert.NoFileExists(t, filepath.Join(testDir, f.Name()))
		}

		err = gcs.RestoreSnapshot(ctxSnapshot, "snapshot_id")
		assert.Nil(t, err)

		// Check that every file in the snapshot exists in testDir
		for _, filePath := range files {
			expectedFilePath := filepath.Join(testDir, filePath.Name())
			assert.FileExists(t, expectedFilePath)
		}
	})
}
