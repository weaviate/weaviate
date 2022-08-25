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

//go:build integrationTest
// +build integrationTest

package backups

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/snapshots"
	modstgs3 "github.com/semi-technologies/weaviate/modules/storage-aws-s3"
	modstgfs "github.com/semi-technologies/weaviate/modules/storage-filesystem"
	modstggcs "github.com/semi-technologies/weaviate/modules/storage-gcs"
	"github.com/semi-technologies/weaviate/test/docker"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackupManagerInt_CreateBackup(t *testing.T) {
	t.Run("storage-fs, single shard", func(t *testing.T) {
		ctx, cancel := testCtx()
		defer cancel()

		painters, paintings := createPainterClass(), createPaintingsClass()

		harness := setupTestingHarness(t, ctx, painters, paintings)

		defer func() {
			require.Nil(t, harness.db.Shutdown(ctx))
			require.Nil(t, os.RemoveAll(harness.dbRootDir))
		}()

		require.Nil(t, os.Setenv("STORAGE_FS_SNAPSHOTS_PATH", path.Join(harness.dbRootDir, "snapshots")))

		moduleProvider := testModuleProvider(ctx, t, harness, modstgfs.New())
		snapshotterProvider := NewSnapshotterProvider(harness.db)
		shardingStateFunc := func(className string) *sharding.State {
			return harness.shardingState
		}

		snapshotID := "storage-fs-test-snapshot"

		manager := NewBackupManager(harness.logger, snapshotterProvider, moduleProvider, shardingStateFunc)

		t.Run("create backup", func(t *testing.T) {
			snapshot, err := manager.CreateBackup(ctx, painters.Class, modstgfs.Name, snapshotID)
			require.Nil(t, err)
			assert.Equal(t, snapshots.CreateStarted, snapshot.Status)

			startTime := time.Now()
			for {
				if time.Now().After(startTime.Add(5 * time.Second)) {
					cancel()
					t.Fatal("snapshot took to long to succeed")
				}

				meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstgfs.Name, snapshotID)
				require.Nil(t, err, "expected nil error, received: %s", err)
				if meta.Status != nil && *meta.Status == string(snapshots.CreateSuccess) {
					break
				}

				time.Sleep(10 * time.Millisecond)
			}

			meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstgfs.Name, snapshotID)
			require.Nil(t, err)
			assert.NotNil(t, meta.Status)
			assert.Equal(t, string(snapshots.CreateSuccess), *meta.Status)
		})
	})

	t.Run("storage-gcs, single shard", func(t *testing.T) {
		ctx, cancel := testCtx()
		defer cancel()

		compose := docker.New()
		compose.WithGCS()
		container, err := compose.Start(ctx)
		require.Nil(t, err)
		require.Nil(t, os.Setenv("GCS_ENDPOINT", container.GetGCS().URI()))
		require.Nil(t, os.Setenv("STORAGE_EMULATOR_HOST", container.GetGCS().URI()))
		require.Nil(t, os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
		require.Nil(t, os.Setenv("GOOGLE_CLOUD_PROJECT", "project-id"))

		painters, paintings := createPainterClass(), createPaintingsClass()

		harness := setupTestingHarness(t, ctx, painters, paintings)

		defer func() {
			require.Nil(t, harness.db.Shutdown(ctx))
			require.Nil(t, os.RemoveAll(harness.dbRootDir))
			require.Nil(t, container.Terminate(ctx))
		}()

		moduleProvider := testModuleProvider(ctx, t, harness, modstggcs.New())
		snapshotterProvider := NewSnapshotterProvider(harness.db)
		shardingStateFunc := func(className string) *sharding.State {
			return harness.shardingState
		}

		snapshotID := "storage-gcs-test-snapshot"

		manager := NewBackupManager(harness.logger, snapshotterProvider, moduleProvider, shardingStateFunc)

		t.Run("create backup", func(t *testing.T) {
			snapshot, err := manager.CreateBackup(ctx, painters.Class, modstggcs.Name, snapshotID)
			require.Nil(t, err, "expected nil error, received: %s", err)
			assert.Equal(t, snapshots.CreateStarted, snapshot.Status)

			startTime := time.Now()
			for {
				if time.Now().After(startTime.Add(5 * time.Second)) {
					cancel()
					t.Fatal("snapshot took to long to succeed")
				}

				meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstggcs.Name, snapshotID)
				require.Nil(t, err, "expected nil error, received: %s", err)
				if meta.Status != nil && *meta.Status == string(snapshots.CreateSuccess) {
					break
				}

				time.Sleep(10 * time.Millisecond)
			}

			meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstggcs.Name, snapshotID)
			require.Nil(t, err)
			assert.NotNil(t, meta.Status)
			assert.Equal(t, string(snapshots.CreateSuccess), *meta.Status)
		})
	})

	t.Run("storage-aws-s3, single shard", func(t *testing.T) {
		ctx, cancel := testCtx()
		defer cancel()

		compose := docker.New()
		compose.WithMinIO()
		container, err := compose.Start(ctx)
		require.Nil(t, err)
		require.Nil(t, os.Setenv("AWS_REGION", "eu-west-1"))
		require.Nil(t, os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key"))
		require.Nil(t, os.Setenv("AWS_SECRET_KEY", "aws_secret_key"))
		require.Nil(t, os.Setenv("STORAGE_S3_ENDPOINT", container.GetMinIO().URI()))

		painters, paintings := createPainterClass(), createPaintingsClass()

		harness := setupTestingHarness(t, ctx, painters, paintings)

		defer func() {
			require.Nil(t, harness.db.Shutdown(ctx))
			require.Nil(t, os.RemoveAll(harness.dbRootDir))
			require.Nil(t, container.Terminate(ctx))
		}()

		moduleProvider := testModuleProvider(ctx, t, harness, modstgs3.New())
		snapshotterProvider := NewSnapshotterProvider(harness.db)
		shardingStateFunc := func(className string) *sharding.State {
			return harness.shardingState
		}
		
		snapshotID := "storage-aws-s3-test-snapshot"

		manager := NewBackupManager(harness.logger, snapshotterProvider, moduleProvider, shardingStateFunc)

		t.Run("create backup", func(t *testing.T) {
			snapshot, err := manager.CreateBackup(ctx, painters.Class, modstgs3.Name, snapshotID)
			require.Nil(t, err, "expected nil error, received: %s", err)
			assert.Equal(t, snapshots.CreateStarted, snapshot.Status)

			startTime := time.Now()
			for {
				if time.Now().After(startTime.Add(5 * time.Second)) {
					cancel()
					t.Fatal("snapshot took to long to succeed")
				}

				meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstgs3.Name, snapshotID)
				require.Nil(t, err, "expected nil error, received: %s", err)
				if meta.Status != nil && *meta.Status == string(snapshots.CreateSuccess) {
					break
				}

				time.Sleep(10 * time.Millisecond)
			}

			meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstgs3.Name, snapshotID)
			require.Nil(t, err)
			assert.NotNil(t, meta.Status)
			assert.Equal(t, string(snapshots.CreateSuccess), *meta.Status)
		})
	})
}
