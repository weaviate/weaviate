//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compact

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// TestMigrator_RecoveryAfterRenameFailure verifies migrator recovery
// when rename fails during snapshot migration.
func TestMigrator_RecoveryAfterRenameFailure(t *testing.T) {
	baseDir := t.TempDir()
	commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
	snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")

	// Create directories
	require.NoError(t, os.MkdirAll(commitlogDir, 0o755))
	require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

	// Create a V3 snapshot in the old location
	snapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
	writeV3Snapshot(t, snapshotPath)

	// Create TestFS that always fails on rename (simulating cross-filesystem move failure)
	fs := common.NewTestFS()
	renameCount := atomic.Int32{}
	fs.OnRename = func(oldpath, newpath string) error {
		count := renameCount.Add(1)
		// Fail on the first rename (direct rename fails, triggers copy+rename)
		// And fail on the second rename too (the copy+rename step)
		if count <= 2 {
			return os.ErrPermission
		}
		return os.Rename(oldpath, newpath)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// First attempt - should fail
	// The first rename fails, then it falls back to copy+rename which also fails
	migrator := NewMigratorWithFS(commitlogDir, logger, fs)
	err := migrator.MigrateSnapshotDirectory()
	require.Error(t, err, "first attempt should fail")

	// Verify snapshot still in old location (migration failed)
	_, err = os.Stat(snapshotPath)
	require.NoError(t, err, "snapshot should still be in old location after failed migration")

	// Second attempt with working FS should succeed
	migrator2 := NewMigrator(commitlogDir, logger)
	err = migrator2.MigrateSnapshotDirectory()
	require.NoError(t, err, "second attempt should succeed")

	// Verify snapshot was moved
	newSnapshotPath := filepath.Join(commitlogDir, "1000.snapshot")
	_, err = os.Stat(newSnapshotPath)
	require.NoError(t, err, "snapshot should be in new location")

	// Old location should be empty
	_, err = os.Stat(snapshotPath)
	assert.True(t, os.IsNotExist(err), "snapshot should not be in old location")
}

// TestMigrator_StateFileRecovery verifies that migration state file
// enables recovery after crash during conversion.
func TestMigrator_StateFileRecovery(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create a migration state file simulating interrupted migration
	migrator := NewMigrator(dir, logger)

	state := &MigrationState{
		CondensedConverted: []string{filepath.Join(dir, "1000.condensed")},
		CondensedPending:   []string{filepath.Join(dir, "2000.condensed")},
	}
	err := migrator.SaveState(state)
	require.NoError(t, err)

	// Verify state file exists
	statePath := filepath.Join(dir, migrationStateFile)
	_, err = os.Stat(statePath)
	require.NoError(t, err, "state file should exist")

	// Load state
	loadedState, err := migrator.LoadState()
	require.NoError(t, err)
	assert.Equal(t, state.CondensedConverted, loadedState.CondensedConverted)
	assert.Equal(t, state.CondensedPending, loadedState.CondensedPending)
}

// TestMigrator_OrphanedMigratingFilesCleanedUp verifies that orphaned
// .migrating files are cleaned up on startup.
func TestMigrator_OrphanedMigratingFilesCleanedUp(t *testing.T) {
	dir := t.TempDir()

	// Create an orphaned .migrating file
	orphanPath := filepath.Join(dir, "snapshot.migrating")
	err := os.WriteFile(orphanPath, []byte("orphaned"), 0o666)
	require.NoError(t, err)

	// Create a valid file
	validPath := filepath.Join(dir, "data.txt")
	err = os.WriteFile(validPath, []byte("valid"), 0o666)
	require.NoError(t, err)

	// Cleanup should remove orphaned migrating file
	err = CleanupMigratingFiles(dir)
	require.NoError(t, err)

	// Verify orphaned file is removed
	_, err = os.Stat(orphanPath)
	assert.True(t, os.IsNotExist(err), "orphaned .migrating file should be removed")

	// Verify valid file still exists
	_, err = os.Stat(validPath)
	require.NoError(t, err, "valid file should still exist")
}

// TestMigrator_SaveStateAtomicity verifies that state file writes are atomic.
func TestMigrator_SaveStateAtomicity(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()

	// Create TestFS that fails on rename during SaveState
	fs := common.NewTestFS()
	renameFailed := atomic.Bool{}
	fs.OnRename = func(oldpath, newpath string) error {
		if !renameFailed.Load() {
			renameFailed.Store(true)
			return os.ErrPermission
		}
		return os.Rename(oldpath, newpath)
	}

	migrator := NewMigratorWithFS(dir, logger, fs)

	state := &MigrationState{
		CondensedConverted: []string{"/path/to/converted.condensed"},
		CondensedPending:   []string{"/path/to/pending.condensed"},
	}

	// First save should fail
	err := migrator.SaveState(state)
	require.Error(t, err, "first save should fail on rename")

	// State file should not exist (temp file should be cleaned up)
	statePath := filepath.Join(dir, migrationStateFile)
	_, err = os.Stat(statePath)
	assert.True(t, os.IsNotExist(err), "state file should not exist after failed save")

	// Second save with working rename should succeed
	err = migrator.SaveState(state)
	require.NoError(t, err, "second save should succeed")

	// State file should now exist
	_, err = os.Stat(statePath)
	require.NoError(t, err, "state file should exist after successful save")
}

// TestMigrator_RecoveryAfterStateSaveFailurePostConversion verifies recovery when
// state persistence fails after a successful file conversion.
func TestMigrator_RecoveryAfterStateSaveFailurePostConversion(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	condensedPath := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	initialMigrator := NewMigrator(dir, logger)
	require.NoError(t, initialMigrator.SaveState(&MigrationState{
		CondensedConverted: []string{},
		CondensedPending:   []string{condensedPath},
	}))

	fs := common.NewTestFS()
	renameCount := atomic.Int32{}
	fs.OnRename = func(oldpath, newpath string) error {
		// 1st rename: sorted temp -> sorted (conversion commit)
		// 2nd rename: state temp -> state (SaveState), force failure here
		if renameCount.Add(1) == 2 {
			return os.ErrPermission
		}
		return os.Rename(oldpath, newpath)
	}

	failingMigrator := NewMigratorWithFS(dir, logger, fs)
	err := failingMigrator.Migrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "save state after conversion")

	sortedPath := filepath.Join(dir, "1000.sorted")
	_, err = os.Stat(sortedPath)
	require.NoError(t, err, "sorted file should exist after converted file save-state failure")

	_, err = os.Stat(condensedPath)
	assert.True(t, os.IsNotExist(err), "condensed file should be removed after successful conversion")

	loadedState, err := initialMigrator.LoadState()
	require.NoError(t, err)
	require.NotNil(t, loadedState)
	assert.Empty(t, loadedState.CondensedConverted, "state should still reflect pre-conversion data")
	assert.Equal(t, []string{condensedPath}, loadedState.CondensedPending)

	err = initialMigrator.Migrate(context.Background())
	require.NoError(t, err, "rerun should recover and complete")
	assert.True(t, initialMigrator.IsMigrationComplete(), "migration should complete on rerun")
}

type failSentinelWriteFS struct {
	common.FS
	sentinelPath string
	failedOnce   atomic.Bool
}

func (f *failSentinelWriteFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if name == f.sentinelPath && !f.failedOnce.Swap(true) {
		return os.ErrPermission
	}
	return f.FS.WriteFile(name, data, perm)
}

// TestMigrator_RecoveryAfterMarkMigrationCompleteFailure verifies that a failure
// writing the sentinel can be recovered by rerunning migration.
func TestMigrator_RecoveryAfterMarkMigrationCompleteFailure(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	condensedPath := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	sentinelPath := filepath.Join(dir, migrationSentinelFile)
	fs := &failSentinelWriteFS{
		FS:           common.NewOSFS(),
		sentinelPath: sentinelPath,
	}

	failingMigrator := NewMigratorWithFS(dir, logger, fs)
	err := failingMigrator.Migrate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark migration complete")

	_, err = os.Stat(filepath.Join(dir, "1000.sorted"))
	require.NoError(t, err, "sorted file should exist")
	_, err = os.Stat(condensedPath)
	assert.True(t, os.IsNotExist(err), "condensed file should be removed")
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel should not exist after failed mark complete")

	resumeMigrator := NewMigrator(dir, logger)
	err = resumeMigrator.Migrate(context.Background())
	require.NoError(t, err, "rerun should create sentinel and finish cleanly")
	assert.True(t, resumeMigrator.IsMigrationComplete())
}

// TestMigrator_ContextCancellationMidFlight verifies interruption after partial
// progress and successful resume on next run.
func TestMigrator_ContextCancellationMidFlight(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	condensed1 := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensed1, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})
	condensed2 := filepath.Join(dir, "2000.condensed")
	createTestWALFile(t, condensed2, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	ctx, cancel := context.WithCancel(context.Background())
	fs := common.NewTestFS()
	sortedRenameCount := atomic.Int32{}
	fs.OnRename = func(oldpath, newpath string) error {
		if filepath.Ext(newpath) == suffixSorted && sortedRenameCount.Add(1) == 1 {
			cancel()
		}
		return os.Rename(oldpath, newpath)
	}

	migrator := NewMigratorWithFS(dir, logger, fs)
	err := migrator.Migrate(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")

	_, err = os.Stat(filepath.Join(dir, "1000.sorted"))
	require.NoError(t, err, "first file should be converted before cancellation")
	_, err = os.Stat(condensed1)
	assert.True(t, os.IsNotExist(err), "first condensed file should be removed")

	_, err = os.Stat(condensed2)
	require.NoError(t, err, "second condensed file should remain for resume")

	resumeMigrator := NewMigrator(dir, logger)
	err = resumeMigrator.Migrate(context.Background())
	require.NoError(t, err)
	assert.True(t, resumeMigrator.IsMigrationComplete())
	_, err = os.Stat(filepath.Join(dir, "2000.sorted"))
	require.NoError(t, err, "second file should convert on rerun")
}

// writeV3Snapshot creates a minimal V3 snapshot for testing.
func writeV3Snapshot(t *testing.T, path string) {
	t.Helper()

	sfw, err := NewSafeFileWriter(path, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	writer := NewSnapshotWriter(sfw.Writer())
	writer.SetEntrypoint(0, 0)
	err = writer.Flush()
	require.NoError(t, err)

	err = sfw.Commit()
	require.NoError(t, err)
}
