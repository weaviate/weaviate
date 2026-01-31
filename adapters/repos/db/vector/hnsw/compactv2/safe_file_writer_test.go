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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeFileWriter_HappyPath(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("hello, world!")

	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	// Verify temp file exists
	tmpPath := finalPath + tempFileSuffix
	_, err = os.Stat(tmpPath)
	require.NoError(t, err, "temp file should exist")

	// Write data
	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	// Commit
	err = sfw.Commit()
	require.NoError(t, err)

	// Verify final file exists with correct content
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, testData, content)

	// Verify temp file is gone
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "temp file should be removed after commit")
}

func TestSafeFileWriter_AbortRemovesTempFile(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	tmpPath := finalPath + tempFileSuffix

	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)

	// Verify temp file exists
	_, err = os.Stat(tmpPath)
	require.NoError(t, err, "temp file should exist")

	// Write some data
	_, err = sfw.Writer().Write([]byte("test data"))
	require.NoError(t, err)

	// Abort
	err = sfw.Abort()
	require.NoError(t, err)

	// Verify temp file is gone
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "temp file should be removed after abort")

	// Verify final file does not exist
	_, err = os.Stat(finalPath)
	assert.True(t, os.IsNotExist(err), "final file should not exist after abort")
}

func TestSafeFileWriter_CommitIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	_, err = sfw.Writer().Write([]byte("test"))
	require.NoError(t, err)

	// First commit
	err = sfw.Commit()
	require.NoError(t, err)

	// Second commit should be no-op
	err = sfw.Commit()
	require.NoError(t, err)
}

func TestSafeFileWriter_AbortIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)

	// First abort
	err = sfw.Abort()
	require.NoError(t, err)

	// Second abort should be no-op
	err = sfw.Abort()
	require.NoError(t, err)
}

func TestSafeFileWriter_AbortAfterCommit(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	testData := []byte("test data")

	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)

	_, err = sfw.Writer().Write(testData)
	require.NoError(t, err)

	// Commit
	err = sfw.Commit()
	require.NoError(t, err)

	// Abort after commit should be no-op
	err = sfw.Abort()
	require.NoError(t, err)

	// Final file should still exist with correct content
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}

func TestSafeFileWriter_OExclFailsIfTempExists(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")
	tmpPath := finalPath + tempFileSuffix

	// Create a pre-existing temp file
	err := os.WriteFile(tmpPath, []byte("orphaned"), 0o666)
	require.NoError(t, err)

	// NewSafeFileWriter should fail due to O_EXCL
	_, err = NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create temp file")
}

func TestSafeFileWriter_DefaultBufferSize(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	// Pass 0 for buffer size, should use default
	sfw, err := NewSafeFileWriter(finalPath, 0)
	require.NoError(t, err)
	defer sfw.Abort()

	// Should work normally
	_, err = sfw.Writer().Write([]byte("test"))
	require.NoError(t, err)
}

func TestSafeFileWriter_NegativeBufferSize(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	// Pass negative buffer size, should use default
	sfw, err := NewSafeFileWriter(finalPath, -1)
	require.NoError(t, err)
	defer sfw.Abort()

	// Should work normally
	_, err = sfw.Writer().Write([]byte("test"))
	require.NoError(t, err)
}

func TestCleanupOrphanedTempFiles(t *testing.T) {
	dir := t.TempDir()

	// Create some regular files
	regularFile := filepath.Join(dir, "regular.dat")
	err := os.WriteFile(regularFile, []byte("regular"), 0o666)
	require.NoError(t, err)

	// Create some orphaned temp files
	orphan1 := filepath.Join(dir, "snapshot1.dat.tmp")
	orphan2 := filepath.Join(dir, "snapshot2.dat.tmp")
	err = os.WriteFile(orphan1, []byte("orphan1"), 0o666)
	require.NoError(t, err)
	err = os.WriteFile(orphan2, []byte("orphan2"), 0o666)
	require.NoError(t, err)

	// Run cleanup
	err = CleanupOrphanedTempFiles(dir)
	require.NoError(t, err)

	// Verify regular file still exists
	_, err = os.Stat(regularFile)
	require.NoError(t, err, "regular file should still exist")

	// Verify temp files are gone
	_, err = os.Stat(orphan1)
	assert.True(t, os.IsNotExist(err), "orphan1 should be removed")
	_, err = os.Stat(orphan2)
	assert.True(t, os.IsNotExist(err), "orphan2 should be removed")
}

func TestCleanupOrphanedTempFiles_NonExistentDir(t *testing.T) {
	// Should not error on non-existent directory
	err := CleanupOrphanedTempFiles("/non/existent/path")
	require.NoError(t, err)
}

func TestCleanupOrphanedTempFiles_SkipsSubdirectories(t *testing.T) {
	dir := t.TempDir()

	// Create a subdirectory with .tmp suffix (unusual but possible)
	subdir := filepath.Join(dir, "somedir.tmp")
	err := os.Mkdir(subdir, 0o755)
	require.NoError(t, err)

	// Run cleanup - should not attempt to remove directory
	err = CleanupOrphanedTempFiles(dir)
	require.NoError(t, err)

	// Subdirectory should still exist
	_, err = os.Stat(subdir)
	require.NoError(t, err, "subdirectory should still exist")
}

func TestSafeFileWriter_LargeWrite(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "test.dat")

	// Use small buffer to test multiple flushes
	sfw, err := NewSafeFileWriter(finalPath, 1024)
	require.NoError(t, err)
	defer sfw.Abort()

	// Write more data than buffer size
	largeData := make([]byte, 10*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	_, err = sfw.Writer().Write(largeData)
	require.NoError(t, err)

	err = sfw.Commit()
	require.NoError(t, err)

	// Verify content
	content, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	assert.Equal(t, largeData, content)
}

func TestSafeFileWriter_IntegrationWithSnapshotWriter(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "snapshot.dat")

	// Create SafeFileWriter
	sfw, err := NewSafeFileWriter(finalPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	// Use with SnapshotWriter
	sw := NewSnapshotWriterWithBlockSize(sfw.Writer(), 1024)
	sw.SetEntrypoint(42, 3)
	sw.AddNode(0, 1, [][]uint64{{1, 2, 3}}, false)
	sw.AddNode(1, 0, [][]uint64{{0, 2}}, false)

	err = sw.Flush()
	require.NoError(t, err)

	// Commit
	err = sfw.Commit()
	require.NoError(t, err)

	// Verify file exists and has content
	info, err := os.Stat(finalPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}
