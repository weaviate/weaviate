//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestBackup_Integration(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dirName := t.TempDir()
	indexID := "backup-integration-test"

	parentCommitLoggerCallbacks := cyclemanager.NewCallbackGroup("parentCommitLogger", logger, 1)
	parentCommitLoggerCycle := cyclemanager.NewManager(
		cyclemanager.HnswCommitLoggerCycleTicker(),
		parentCommitLoggerCallbacks.CycleCallback, logger)
	parentCommitLoggerCycle.Start()
	defer parentCommitLoggerCycle.StopAndWait(ctx)
	commitLoggerCallbacks := cyclemanager.NewCallbackGroup("childCommitLogger", logger, 1)
	commitLoggerCallbacksCtrl := parentCommitLoggerCallbacks.Register("commitLogger", commitLoggerCallbacks.CycleCallback)

	parentTombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup("parentTombstoneCleanup", logger, 1)
	parentTombstoneCleanupCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(enthnsw.DefaultCleanupIntervalSeconds*time.Second),
		parentTombstoneCleanupCallbacks.CycleCallback, logger)
	parentTombstoneCleanupCycle.Start()
	defer parentTombstoneCleanupCycle.StopAndWait(ctx)
	tombstoneCleanupCallbacks := cyclemanager.NewCallbackGroup("childTombstoneCleanup", logger, 1)
	tombstoneCleanupCallbacksCtrl := parentTombstoneCleanupCallbacks.Register("tombstoneCleanup", tombstoneCleanupCallbacks.CycleCallback)

	combinedCtrl := cyclemanager.NewCombinedCallbackCtrl(2, logger, commitLoggerCallbacksCtrl, tombstoneCleanupCallbacksCtrl)

	idx, err := New(Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logger,
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logger, commitLoggerCallbacks)
		},
	}, enthnsw.NewDefaultUserConfig(), tombstoneCleanupCallbacks, nil)
	require.Nil(t, err)
	idx.PostStartup()

	t.Run("insert vector into index", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			inc := float32(i)
			err := idx.Add(ctx, uint64(i), []float32{inc, inc + 1, inc + 2})
			require.Nil(t, err)
		}
	})

	// let the index age for a second so that
	// the commitlogger filenames, which are
	// based on current timestamp, can differ
	time.Sleep(time.Second)

	t.Run("pause maintenance", func(t *testing.T) {
		err = combinedCtrl.Deactivate(ctx)
		require.Nil(t, err)
	})

	t.Run("switch commit logs", func(t *testing.T) {
		err = idx.SwitchCommitLogs(ctx)
		require.Nil(t, err)
	})

	// after switch commit logs, to have source log(s)
	t.Run("create snapshot", func(t *testing.T) {
		created, _, err := idx.commitLog.CreateSnapshot()
		require.Nil(t, err)
		require.True(t, created)
	})

	t.Run("list files", func(t *testing.T) {
		files, err := idx.ListFiles(ctx, dirName)
		require.Nil(t, err)

		// by this point there should be two files in the commitlog directory.
		// one is the active log file, and the other is the previous active
		// log which was in use prior to `SwitchCommitLogs`. additionally,
		// maintenance has been paused, so we shouldn't see any .condensed
		// files either.
		//
		// because `ListFiles` is used within the context of backups,
		// it excludes any currently active log files, which are not part
		// of the backup. in this case, the only other file is the prev
		// commitlog, so we should only have 1 result here.
		//
		// additionally snapshot was created which consist of 2 files,
		// so total of 3 files are expected
		assert.Len(t, files, 3)

		filesUnique := make(map[string]struct{}, len(files))
		for i := range files {
			filesUnique[files[i]] = struct{}{}
		}
		require.Len(t, filesUnique, len(files))

		t.Run("verify commitlog dir contents", func(t *testing.T) {
			// checking to ensure that indeed there are only 2 files in the
			// commit log directory, and that one of them is the one result
			// from `ListFiles`, and that the other is not a .condensed file
			ls, err := os.ReadDir(path.Join(dirName, fmt.Sprintf("%s.hnsw.commitlog.d", indexID)))
			require.Nil(t, err)
			assert.Len(t, ls, 2)

			var prevLogFound bool
			for _, info := range ls {
				if path.Base(files[0]) == info.Name() {
					prevLogFound = true
				}

				assert.Empty(t, path.Ext(info.Name()))
			}
			assert.True(t, prevLogFound, "previous commitlog not found in commitlog root dir")
		})

		t.Run("verify snapshot dir contents", func(t *testing.T) {
			snapshotDir := snapshotDirectory(idx.commitLog.RootPath(), idx.commitLog.ID())
			relSnapshotDir := snapshotDirectory("", idx.commitLog.ID())

			ls, err := os.ReadDir(snapshotDir)
			require.Nil(t, err)

			for i := range ls {
				snapshotFilePath := path.Join(relSnapshotDir, ls[i].Name())
				assert.Contains(t, filesUnique, snapshotFilePath)
			}
		})
	})

	t.Run("resume maintenance", func(t *testing.T) {
		err = combinedCtrl.Activate()
		require.Nil(t, err)
	})

	err = idx.Shutdown(ctx)
	require.Nil(t, err)

	err = combinedCtrl.Unregister(ctx)
	require.Nil(t, err)
}
