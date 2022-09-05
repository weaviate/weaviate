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

// TODO: test remaining methods
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
	// testDir := moduleshelper.MakeTestDir(t, testdataMainDir)
	defer moduleshelper.RemoveDir(t, testdataMainDir)

	className := "BackupClass"
	backupID := "backup_id"
	endpoint := os.Getenv(envGCSEndpoint)
	bucketName := "weaviate-snapshots"
	projectID := "project-id"
	metadataFilename := "snapshot.json"

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

		meta, err := gcs.GetObject(testCtx, backupID, metadataFilename)
		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrNotFound{}, err)

		err = gcs.Initialize(testCtx, backupID)
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

		err = gcs.PutObject(testCtx, backupID, metadataFilename, b)
		require.Nil(t, err)

		dest := gcs.DestinationPath(backupID)
		expected := fmt.Sprintf("gs://%s/%s/snapshot.json", bucketName, backupID)
		assert.Equal(t, expected, dest)

		t.Run("assert snapshot meta contents", func(t *testing.T) {
			obj, err := gcs.GetObject(testCtx, backupID, metadataFilename)
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

	snapshotID := "snapshot_id"
	key := "moduleLevelGetMetaStatus"

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

	t.Run("set snapshot status", func(t *testing.T) {
		client, err := storage.NewClient(testCtx, option.WithoutAuthentication())
		require.Nil(t, err)

		if _, err := client.Bucket(bucketName).Attrs(testCtx); err != nil {
			t.Fatal(err.Error())
		}

		gcs, err := gcs.New(testCtx, gcsConfig, testDir)
		require.Nil(t, err)

		err = gcs.PutObject(testCtx, snapshotID, key, []byte("hello"))
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		gcs, err := gcs.New(testCtx, gcsConfig, testDir)
		require.Nil(t, err)

		meta, err := gcs.GetObject(testCtx, snapshotID, key)
		assert.Nil(t, err)
		assert.Equal(t, []byte("hello"), meta)
	})
}
