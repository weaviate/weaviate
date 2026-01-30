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

package compactv2

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// TestCompactor_RecoveryAfterRemoveFailure verifies compactor recovery
// when remove of source files fails after successful snapshot creation.
func TestCompactor_RecoveryAfterRemoveFailure(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create some sorted files to process
	writeTestSortedFile(t, dir, 1000, 1000)
	writeTestSortedFile(t, dir, 2000, 2000)

	// Create TestFS that fails on remove
	fs := common.NewTestFS()
	removeCount := atomic.Int32{}
	fs.OnRemove = func(name string) error {
		count := removeCount.Add(1)
		if count <= 2 { // Fail first two removes
			return os.ErrPermission
		}
		return os.Remove(name)
	}

	// Create compactor with failing FS
	// With no snapshot and only 2 files, it will create a snapshot (not merge)
	config := CompactorConfig{
		Dir:               dir,
		MaxFilesPerMerge:  5,
		SnapshotThreshold: 0.2,
		BufferSize:        DefaultBufferSize,
		FS:                fs,
	}
	compactor := NewCompactor(config, logger)

	// First run - snapshot succeeds but remove of source files fails
	// This is OK because the snapshot is written atomically
	action, err := compactor.RunCycle()
	// Remove failures are logged as warnings, not errors
	require.NoError(t, err)
	assert.Equal(t, ActionCreateSnapshot, action)

	// Verify snapshot exists
	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()
	require.NoError(t, err)
	assert.NotNil(t, state.Snapshot, "snapshot should exist")

	// The old sorted files may still exist (remove failed)
	// This creates an overlap situation

	// On next compactor cycle with working FS, overlaps will be detected and resolved
	config.FS = common.NewOSFS()
	compactor2 := NewCompactor(config, logger)

	// Run cleanup cycle
	_, err = compactor2.RunCycle()
	require.NoError(t, err)

	// Final state should be clean
	state, err = discovery.Scan()
	require.NoError(t, err)

	// Should have resolved all overlaps
	assert.Empty(t, state.Overlaps, "overlaps should be resolved")
	assert.NotNil(t, state.Snapshot, "snapshot should still exist")
}

// TestCompactor_RecoveryAfterRenameFailure verifies compactor recovery
// when rename fails during SafeFileWriter commit.
func TestCompactor_RecoveryAfterRenameFailure(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create sorted files
	writeTestSortedFile(t, dir, 1000, 1000)
	writeTestSortedFile(t, dir, 2000, 2000)

	// Create TestFS that fails on rename
	fs := common.NewTestFS()
	renameFailed := atomic.Bool{}
	fs.OnRename = func(oldpath, newpath string) error {
		// Only fail once
		if !renameFailed.Load() {
			renameFailed.Store(true)
			return os.ErrPermission
		}
		return os.Rename(oldpath, newpath)
	}

	// Create compactor with failing FS
	config := CompactorConfig{
		Dir:               dir,
		MaxFilesPerMerge:  5,
		SnapshotThreshold: 0.2,
		BufferSize:        DefaultBufferSize,
		FS:                fs,
	}
	compactor := NewCompactor(config, logger)

	// First run should fail on rename
	_, err := compactor.RunCycle()
	require.Error(t, err, "first run should fail due to rename failure")
	assert.Contains(t, err.Error(), "rename")

	// Clean up orphaned temp files (simulates startup recovery)
	CleanupOrphanedTempFiles(dir)

	// Second run with working FS should succeed
	config.FS = common.NewOSFS()
	compactor2 := NewCompactor(config, logger)

	action, err := compactor2.RunCycle()
	require.NoError(t, err, "second run should succeed")
	// With 2 sorted files and no snapshot, it will create a snapshot
	assert.Equal(t, ActionCreateSnapshot, action)
}

// TestCompactor_OrphanedTempFilesCleanedUp verifies that orphaned temp files
// are cleaned up at the start of each cycle.
func TestCompactor_OrphanedTempFilesCleanedUp(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create an orphaned temp file (simulating crash during previous write)
	orphanPath := filepath.Join(dir, "1000_2000.sorted.tmp")
	err := os.WriteFile(orphanPath, []byte("orphaned data"), 0o666)
	require.NoError(t, err)

	// Create a valid sorted file
	writeTestSortedFile(t, dir, 3000, 3000)

	// Verify setup
	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()
	require.NoError(t, err)
	require.Len(t, state.SortedFiles, 1, "should have 1 sorted file before compaction")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	// Run cycle - should cleanup orphaned temp file first
	// With only 1 sorted file, action will be ActionNone or ActionCreateSnapshot
	_, err = compactor.RunCycle()
	require.NoError(t, err)

	// Verify orphaned temp file was cleaned up
	_, err = os.Stat(orphanPath)
	assert.True(t, os.IsNotExist(err), "orphaned temp file should be removed")

	// Verify state - either snapshot was created or sorted file remains
	state, err = discovery.Scan()
	require.NoError(t, err)
	// Should have either a snapshot or a sorted file
	hasSortedOrSnapshot := state.Snapshot != nil || len(state.SortedFiles) > 0
	assert.True(t, hasSortedOrSnapshot, "should have either snapshot or sorted file")
}

// writeTestSortedFile creates a minimal sorted file for testing.
func writeTestSortedFile(t *testing.T, dir string, startTS, endTS int64) {
	t.Helper()

	filename := BuildMergedFilename(startTS, endTS, FileTypeSorted)
	path := filepath.Join(dir, filename)

	sfw, err := NewSafeFileWriter(path, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	// Write using WALWriter directly
	walWriter := NewWALWriter(sfw.Writer())
	err = walWriter.WriteSetEntryPointMaxLevel(0, 0)
	require.NoError(t, err)

	err = walWriter.WriteAddNode(0, 0)
	require.NoError(t, err)

	err = sfw.Commit()
	require.NoError(t, err)
}
