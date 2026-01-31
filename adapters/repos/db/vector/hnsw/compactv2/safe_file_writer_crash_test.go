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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// TestSafeFileWriter_CrashDuringSync verifies recovery when fsync fails.
func TestSafeFileWriter_CrashDuringSync(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("hello, crash test!")

	// Create TestFS that fails on sync
	fs := common.NewTestFS()
	syncCalled := atomic.Bool{}
	fs.OnOpenFile = func(f common.File) common.File {
		return &common.TestFile{
			File: f,
			OnSync: func() error {
				syncCalled.Store(true)
				return os.ErrPermission // Simulate sync failure
			},
		}
	}

	// First attempt - should fail on sync
	sfw, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw.Abort()

	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sync file")
	assert.True(t, syncCalled.Load(), "sync should have been called")

	// Verify temp file still exists (commit failed)
	tmpPath := finalPath + tempFileSuffix
	_, err = os.Stat(tmpPath)
	require.NoError(t, err, "temp file should still exist after sync failure")

	// Verify final file does not exist
	_, err = os.Stat(finalPath)
	assert.True(t, os.IsNotExist(err), "final file should not exist after sync failure")

	// Abort to cleanup
	sfw.Abort()
}

// TestSafeFileWriter_CrashDuringRename verifies recovery when rename fails.
func TestSafeFileWriter_CrashDuringRename(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("hello, rename crash!")

	// Create TestFS that fails on rename
	fs := common.NewTestFS()
	renameCalled := atomic.Bool{}
	fs.OnRename = func(oldpath, newpath string) error {
		renameCalled.Store(true)
		return os.ErrPermission // Simulate rename failure
	}

	// First attempt - should fail on rename
	sfw, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw.Abort()

	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename")
	assert.True(t, renameCalled.Load(), "rename should have been called")

	// Recovery: cleanup temp file and retry with working FS
	sfw.Abort()

	// Verify temp file was cleaned up by Abort
	tmpPath := finalPath + tempFileSuffix
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "temp file should be removed by abort")

	// Second attempt with working FS should succeed
	sfw2, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw2.Abort()

	_, err = sfw2.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw2.Commit()
	require.NoError(t, err)

	// Verify final file exists with correct content
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}

// TestSafeFileWriter_CrashDuringWrite verifies recovery when write fails during flush.
func TestSafeFileWriter_CrashDuringWrite(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	// Create TestFS that always fails on write
	fs := common.NewTestFS()
	fs.OnOpenFile = func(f common.File) common.File {
		return &common.TestFile{
			File: f,
			OnWrite: func(b []byte) (n int, err error) {
				// Fail all writes
				return 0, os.ErrPermission
			},
		}
	}

	// Attempt to write - buffered writer caches, fails on flush during commit
	sfw, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw.Abort()

	// Write some data (goes to buffer)
	_, err = sfw.Writer().Write([]byte("test data"))
	require.NoError(t, err, "write to buffer should succeed")

	// Commit should fail when flushing buffer to disk
	err = sfw.Commit()
	require.Error(t, err, "commit should fail when flush fails")
	assert.Contains(t, err.Error(), "flush buffer")

	// Verify final file does not exist
	_, err = os.Stat(finalPath)
	assert.True(t, os.IsNotExist(err), "final file should not exist after write failure")

	// Cleanup
	sfw.Abort()
}

// TestSafeFileWriter_CrashDuringClose verifies recovery when close fails.
func TestSafeFileWriter_CrashDuringClose(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("hello, close crash!")

	// Create TestFS that fails on close
	fs := common.NewTestFS()
	closeCalled := atomic.Bool{}
	fs.OnOpenFile = func(f common.File) common.File {
		return &common.TestFile{
			File: f,
			OnClose: func() error {
				closeCalled.Store(true)
				// Close the underlying file first, then return error
				f.Close()
				return os.ErrPermission
			},
		}
	}

	// Attempt - should fail on close
	sfw, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw.Abort()

	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "close file")
	assert.True(t, closeCalled.Load(), "close should have been called")

	// Verify final file does not exist
	_, err = os.Stat(finalPath)
	assert.True(t, os.IsNotExist(err), "final file should not exist after close failure")
}

// TestSafeFileWriter_RecoveryAfterCrash simulates a full crash recovery scenario.
func TestSafeFileWriter_RecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "snapshot.dat")
	tmpPath := finalPath + tempFileSuffix

	// Step 1: Simulate a crash by leaving an orphaned temp file
	err := os.WriteFile(tmpPath, []byte("partial data from crash"), 0o666)
	require.NoError(t, err)

	// Verify temp file exists
	_, err = os.Stat(tmpPath)
	require.NoError(t, err, "orphaned temp file should exist")

	// Step 2: Cleanup orphaned temp files (simulates startup)
	err = CleanupOrphanedTempFiles(dir)
	require.NoError(t, err)

	// Verify temp file is cleaned up
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "orphaned temp file should be removed")

	// Step 3: Now we can write normally
	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	testData := []byte("new valid data")
	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw.Commit()
	require.NoError(t, err)

	// Verify final file has correct content
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}

// TestSafeFileWriter_SyncIsActuallyCalled verifies that Sync is called during commit.
func TestSafeFileWriter_SyncIsActuallyCalled(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	// Track that sync was called
	fs := common.NewTestFS()
	syncCalled := atomic.Bool{}
	fs.OnOpenFile = func(f common.File) common.File {
		return &common.TestFile{
			File: f,
			OnSync: func() error {
				syncCalled.Store(true)
				return f.Sync() // Actually perform the sync
			},
		}
	}

	sfw, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw.Abort()

	_, err = sfw.Writer().Write([]byte("test"))
	require.NoError(t, err)

	err = sfw.Commit()
	require.NoError(t, err)

	assert.True(t, syncCalled.Load(), "Sync must be called during commit for crash safety")
}

// TestSafeFileWriter_RenameIsAtomic verifies the rename pattern for atomicity.
func TestSafeFileWriter_RenameIsAtomic(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("atomic test data")

	// Track rename parameters
	fs := common.NewTestFS()
	var capturedOldPath, capturedNewPath string
	fs.OnRename = func(oldpath, newpath string) error {
		capturedOldPath = oldpath
		capturedNewPath = newpath
		return os.Rename(oldpath, newpath) // Actually perform the rename
	}

	sfw, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw.Abort()

	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw.Commit()
	require.NoError(t, err)

	// Verify rename was from .tmp to final path
	assert.Equal(t, finalPath+tempFileSuffix, capturedOldPath, "should rename from .tmp path")
	assert.Equal(t, finalPath, capturedNewPath, "should rename to final path")
}

// TestSafeFileWriter_MultipleFailureModes tests multiple failures in sequence.
func TestSafeFileWriter_MultipleFailureModes(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("multiple failure test")

	attemptCount := atomic.Int32{}

	// Create TestFS that fails differently on each attempt
	fs := common.NewTestFS()
	fs.OnOpenFile = func(f common.File) common.File {
		attempt := attemptCount.Load()
		switch attempt {
		case 0:
			// First attempt: fail on sync
			return &common.TestFile{
				File: f,
				OnSync: func() error {
					return os.ErrPermission
				},
			}
		default:
			// Later attempts: succeed
			return f
		}
	}

	// First attempt - should fail
	attemptCount.Store(0)
	sfw1, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)

	_, err = sfw1.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw1.Commit()
	require.Error(t, err, "first attempt should fail on sync")
	sfw1.Abort()

	// Clean up temp file
	CleanupOrphanedTempFiles(dir)

	// Second attempt - should succeed
	attemptCount.Store(1)
	sfw2, err := NewSafeFileWriterWithFS(finalPath, DefaultBufferSize, fs)
	require.NoError(t, err)
	defer sfw2.Abort()

	_, err = sfw2.Writer().Write(testData)
	require.NoError(t, err)

	err = sfw2.Commit()
	require.NoError(t, err, "second attempt should succeed")

	// Verify content
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}
