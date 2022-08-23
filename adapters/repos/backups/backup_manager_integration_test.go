//go:build integrationTest
// +build integrationTest

package backups

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/modules"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/modules/storage-filesystem"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackupManager_CreateBackup(t *testing.T) {
	t.Run("storage-fs, single node", func(t *testing.T) {
		ctx, cancel := testCtx()
		defer cancel()

		painters, paintings := createPainterClass(), createPaintingsClass()

		harness := setupTestingHarness(t, ctx, painters, paintings)

		defer func() {
			require.Nil(t, harness.db.Shutdown(ctx))
			require.Nil(t, os.RemoveAll(harness.dbRootDir))
		}()

		require.Nil(t, os.Setenv("STORAGE_FS_SNAPSHOTS_PATH", path.Join(harness.dbRootDir, "snapshots")))

		storageProvider, err := modulestorage.NewRepo(harness.dbRootDir, harness.logger)
		require.Nil(t, err)

		moduleParams := moduletools.NewInitParams(storageProvider, nil,
			harness.logger)

		moduleProvider := modules.NewProvider()
		moduleProvider.Register(modstgfs.New())
		require.Nil(t, moduleProvider.Init(ctx, moduleParams, harness.logger))

		shardingStateFunc := func(className string) *sharding.State {
			return harness.shardingState
		}

		snapshotID := "storage-fs-test-snapshot"

		manager := NewBackupManager(harness.db, harness.logger, moduleProvider, shardingStateFunc)

		snapshot, err := manager.CreateBackup(ctx, painters.Class, modstgfs.Name, snapshotID)
		require.Nil(t, err)

		startTime := time.Now()
		for {
			if time.Now().After(startTime.Add(5 * time.Second)) {
				cancel()
				t.Fatal("snapshot took to long to succeed")
			}

			meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstgfs.Name, snapshotID)
			require.Nil(t, err)
			if meta.Status != nil && *meta.Status == string(snapshots.CreateSuccess) {
				break
			}
		}

		meta, err := manager.CreateBackupStatus(ctx, painters.Class, modstgfs.Name, snapshotID)
		require.Nil(t, err)
		assert.NotNil(t, meta.Status)
		assert.Equal(t, string(snapshots.CreateSuccess), *meta.Status)
		t.Logf("snapshot: %+v", snapshot)
	})
}
