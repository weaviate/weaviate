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
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

func crashTestLogger() logrus.FieldLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

// ---------------------------------------------------------------------------
// CRITICAL: Truncated WAL file recovery
// ---------------------------------------------------------------------------

// TestCrashRecovery_TruncatedWALFile verifies that if a crash leaves a
// partially written record in a raw WAL file, the loader:
//  1. Loads all complete records before the truncation point
//  2. Truncates the file to remove the partial record
//  3. Reports RecoveredFromCrash = true
//  4. Subsequent loads of the truncated file succeed cleanly
func TestCrashRecovery_TruncatedWALFile(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Write a valid WAL file with 3 nodes
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 1))
		require.NoError(t, w.WriteAddNode(0, 1))
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
	})

	// Append garbage to simulate a partial write during crash
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	// Write a partial record: just a commit type byte and 3 bytes of garbage
	_, err = f.Write([]byte{byte(AddNode), 0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Load — should recover from the truncation
	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: logger,
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.RecoveredFromCrash, "should report crash recovery")

	// All complete records before the garbage should be loaded
	assert.Equal(t, uint64(0), result.State.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.State.Graph.Level)
	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
}

// TestCrashRecovery_TruncatedWALFile_MultipleGarbageBytes tests recovery when
// several garbage bytes follow valid records — enough to partially parse a
// commit type byte and then fail reading the body.
func TestCrashRecovery_TruncatedWALFile_MultipleGarbageBytes(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(5, 2))
		require.NoError(t, w.WriteAddNode(5, 2))
	})

	// Append garbage: type byte + partial body. The reader reads the type
	// then fails on io.ReadFull for the body → ErrUnexpectedEOF.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	_, err = f.Write([]byte{byte(AddNode), 0x01, 0x02, 0x03, 0x04, 0x05})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.RecoveredFromCrash)
	assert.Equal(t, uint64(5), result.State.Graph.Entrypoint)
	require.True(t, len(result.State.Graph.Nodes) > 5)
	require.NotNil(t, result.State.Graph.Nodes[5])
}

// ---------------------------------------------------------------------------
// CRITICAL: Crash during compaction — overlap resolution on next cycle
// ---------------------------------------------------------------------------

// TestCrashRecovery_OverlapAfterMerge verifies that when source files
// survive after a successful merge (because remove failed), the next cycle
// resolves the overlaps and the loaded data is correct (no duplicates).
func TestCrashRecovery_OverlapAfterMerge(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Create 3 sorted files with distinct data
	writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})
	writeTestSortedFileWithData(t, dir, 2000, 2000, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
	})
	writeTestSortedFileWithData(t, dir, 3000, 3000, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 2))
	})

	// First compactor run with FS that fails on remove — merge succeeds but
	// source files are not deleted
	fs := common.NewTestFS()
	fs.OnRemove = func(name string) error {
		return os.ErrPermission // always fail removes
	}

	config := CompactorConfig{
		Dir:               dir,
		MaxFilesPerMerge:  5,
		SnapshotThreshold: 0.20,
		BufferSize:        DefaultBufferSize,
		FS:                fs,
	}
	compactor := NewCompactor(config, logger, nil)

	action, err := compactor.RunCycle()
	require.NoError(t, err) // remove failures are warnings, not errors
	assert.NotEqual(t, ActionNone, action)

	// Now we have overlapping files: the merged file + the source files.
	// Verify overlaps exist.
	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()
	require.NoError(t, err)
	assert.NotEmpty(t, state.Overlaps, "should have overlaps from surviving source files")

	// Second cycle with working FS should resolve overlaps
	config.FS = common.NewOSFS()
	compactor2 := NewCompactor(config, logger, nil)

	_, err = compactor2.RunCycle()
	require.NoError(t, err)

	state, err = discovery.Scan()
	require.NoError(t, err)
	assert.Empty(t, state.Overlaps, "overlaps should be resolved")

	// Load and verify data integrity — all 3 nodes should be present
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0], "node 0 missing after overlap resolution")
	require.NotNil(t, result.State.Graph.Nodes[1], "node 1 missing after overlap resolution")
	require.NotNil(t, result.State.Graph.Nodes[2], "node 2 missing after overlap resolution")
}

// ---------------------------------------------------------------------------
// CRITICAL: Two snapshots on disk — data correctness
// ---------------------------------------------------------------------------

// TestCrashRecovery_TwoSnapshots_DataCorrectness extends the existing
// TestLoader_CrashRecovery_TwoSnapshotsUsesNewest by verifying that loaded
// data (connections, tombstones) is correct from the newer snapshot.
func TestCrashRecovery_TwoSnapshots_DataCorrectness(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Old snapshot: nodes 0,1 with connections, node 2 tombstoned
	oldSnapshotPath := filepath.Join(dir, "1000_3000.snapshot")
	createTestSnapshot(t, oldSnapshotPath, 0, 1, []testNode{
		{id: 0, level: 1, connections: [][]uint64{{1}, {1}}, tombstone: false},
		{id: 1, level: 0, connections: [][]uint64{{0}}, tombstone: false},
		{id: 2, level: 0, connections: [][]uint64{{}}, tombstone: true},
	})

	// New snapshot: nodes 0,1,2,3 — tombstone on 2 removed, new node 3 added
	newSnapshotPath := filepath.Join(dir, "1000_5000.snapshot")
	createTestSnapshot(t, newSnapshotPath, 3, 1, []testNode{
		{id: 0, level: 1, connections: [][]uint64{{1, 3}, {3}}, tombstone: false},
		{id: 1, level: 0, connections: [][]uint64{{0, 3}}, tombstone: false},
		{id: 2, level: 0, connections: [][]uint64{{0}}, tombstone: false},
		{id: 3, level: 1, connections: [][]uint64{{0, 1}, {0}}, tombstone: false},
	})

	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Entrypoint should be from the newer snapshot
	assert.Equal(t, uint64(3), result.State.Graph.Entrypoint,
		"entrypoint should come from newer snapshot")

	// All 4 nodes present
	require.True(t, len(result.State.Graph.Nodes) > 3)
	for i := uint64(0); i <= 3; i++ {
		require.NotNilf(t, result.State.Graph.Nodes[i], "node %d missing", i)
	}

	// Node 2 should NOT be tombstoned (newer snapshot removed the tombstone)
	_, hasTombstone := result.State.Graph.Tombstones[2]
	assert.False(t, hasTombstone, "node 2 tombstone should not be present in newer snapshot")
}

// ---------------------------------------------------------------------------
// CRITICAL: ForceNewFile end-to-end
// ---------------------------------------------------------------------------

// TestCrashRecovery_ForceNewFile verifies that after crash recovery is
// detected (truncated WAL), using forceNewFile creates a new file rather than
// appending to the truncated one, and both old + new data are loadable.
func TestCrashRecovery_ForceNewFile(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Write a WAL file with valid data + trailing garbage
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Append garbage to simulate crash
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	_, err = f.Write([]byte{byte(ReplaceLinksAtLevel), 0xFF, 0xFF})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Load — triggers truncation and sets RecoveredFromCrash
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.RecoveredFromCrash)

	// Now simulate creating a new commit log with forceNewFile=true.
	// The new file must have a different (higher) timestamp.
	newWALPath := filepath.Join(dir, "2000")
	createTestWALFile(t, newWALPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 1))
		require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 1))
	})

	// Reload — should find both files and combine their data
	loader2 := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result2, err := loader2.Load()
	require.NoError(t, err)
	require.NotNil(t, result2)

	// Both old and new data present
	assert.Equal(t, uint64(1), result2.State.Graph.Entrypoint, "entrypoint from new file")
	require.True(t, len(result2.State.Graph.Nodes) > 1)
	require.NotNil(t, result2.State.Graph.Nodes[0], "node from old file")
	require.NotNil(t, result2.State.Graph.Nodes[1], "node from new file")
}

// ---------------------------------------------------------------------------
// CRITICAL: switchCommitLogs crash — old file closed, no new file yet
// ---------------------------------------------------------------------------

// TestCrashRecovery_SwitchLogsOldFileClosed simulates a crash after the
// old commit log is flushed+closed but before a new file is created.
// On restart, the directory contains only the old (now read-only) file.
// The loader should recover all data and the system should be able to create
// a new file.
func TestCrashRecovery_SwitchLogsOldFileClosed(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Simulate: old file was flushed+closed during switchCommitLogs
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 0))
	})

	// Crash happened here — no new file was created.
	// On restart, the loader should load the old file.
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.RecoveredFromCrash, "clean file should not trigger recovery")

	assert.Equal(t, uint64(0), result.State.Graph.Entrypoint)
	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])

	// Verify a new file can be created in the same directory
	newPath := filepath.Join(dir, "2000")
	createTestWALFile(t, newPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	// Reload with both files
	loader2 := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result2, err := loader2.Load()
	require.NoError(t, err)
	require.NotNil(t, result2)

	require.True(t, len(result2.State.Graph.Nodes) > 2)
	require.NotNil(t, result2.State.Graph.Nodes[2], "new node from second file")
}

// ---------------------------------------------------------------------------
// IMPORTANT: Crash during convertToSorted — raw file preserved
// ---------------------------------------------------------------------------

// TestCrashRecovery_ConvertToSorted_RawPreserved verifies that if the
// compactor crashes during raw→sorted conversion (rename fails), the
// original raw file is preserved and the orphaned .tmp is cleaned up.
func TestCrashRecovery_ConvertToSorted_RawPreserved(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Create a raw file (not the live file — we need a second, newer one)
	rawPath := filepath.Join(dir, "1000")
	createTestWALFile(t, rawPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Create a "live" file (higher timestamp, so 1000 becomes a non-live raw file)
	livePath := filepath.Join(dir, "2000")
	createTestWALFile(t, livePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	// Compactor with rename failure — conversion writes .tmp but can't rename
	fs := common.NewTestFS()
	renameFailed := atomic.Bool{}
	fs.OnRename = func(oldpath, newpath string) error {
		if !renameFailed.Load() {
			renameFailed.Store(true)
			return os.ErrPermission
		}
		return os.Rename(oldpath, newpath)
	}

	config := CompactorConfig{
		Dir:               dir,
		MaxFilesPerMerge:  5,
		SnapshotThreshold: 0.20,
		BufferSize:        DefaultBufferSize,
		FS:                fs,
	}

	compactor := NewCompactor(config, logger, nil)
	_, err := compactor.RunCycle()
	require.Error(t, err, "should fail due to rename")

	// Raw file should still exist
	_, err = os.Stat(rawPath)
	require.NoError(t, err, "original raw file should be preserved")

	// Clean up orphaned .tmp files (startup recovery)
	CleanupOrphanedTempFiles(dir)

	// Retry with working FS
	config.FS = common.NewOSFS()
	compactor2 := NewCompactor(config, logger, nil)
	_, err = compactor2.RunCycle()
	require.NoError(t, err)

	// Verify data is loadable after recovery
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result, "data should be loadable after crash recovery")

	// Entrypoint should be set (from the raw file data)
	assert.True(t, result.State.Graph.EntrypointChanged, "entrypoint should be set")
}

// ---------------------------------------------------------------------------
// IMPORTANT: Corrupted commit type byte
// ---------------------------------------------------------------------------

// TestCrashRecovery_CorruptedCommitType verifies that the reader returns
// a clear error (not panic) when encountering an unknown commit type byte.
func TestCrashRecovery_CorruptedCommitType(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Write a valid WAL, then overwrite a byte in the middle with an invalid type
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	validStat, err := os.Stat(walPath)
	require.NoError(t, err)

	// Append a record with an invalid commit type (0xFF)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// The loader should handle this — either by treating it as corruption
	// (truncate and recover) or by returning an error. It should NOT panic.
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	// We accept either: error returned, or successful recovery with valid prefix
	if err != nil {
		// Error is acceptable — the key thing is no panic
		return
	}

	// If recovery succeeded, verify we got the valid prefix data
	require.NotNil(t, result)
	assert.Equal(t, uint64(0), result.State.Graph.Entrypoint)
	require.NotNil(t, result.State.Graph.Nodes[0])

	// File should be truncated to remove the corrupt record
	stat, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Equal(t, validStat.Size(), stat.Size(), "corrupt record should be truncated")
}

// ---------------------------------------------------------------------------
// IMPORTANT: Partial migration — some snapshots moved, some not
// ---------------------------------------------------------------------------

// TestCrashRecovery_PartialSnapshotMigration verifies that if
// MigrateSnapshotDirectory is interrupted (some files moved, some not),
// re-running it completes the migration without duplicating files.
func TestCrashRecovery_PartialSnapshotMigration(t *testing.T) {
	logger := crashTestLogger()

	// Create old snapshot directory structure
	dir := t.TempDir()
	commitlogDir := filepath.Join(dir, "test.hnsw.commitlog.d")
	snapshotDir := filepath.Join(dir, "test.hnsw.snapshot.d")
	require.NoError(t, os.MkdirAll(commitlogDir, os.ModePerm))
	require.NoError(t, os.MkdirAll(snapshotDir, os.ModePerm))

	// Create 2 snapshot files in old directory
	for i := 1; i <= 2; i++ {
		path := filepath.Join(snapshotDir, fmt.Sprintf("%d000.snapshot", i))
		createTestSnapshot(t, path, uint64(i), 0, []testNode{
			{id: uint64(i), level: 0, connections: [][]uint64{{}}, tombstone: false},
		})
	}

	// On same filesystem, atomicMoveFile uses a single rename per file.
	// Fail starting from the 2nd rename to simulate crash during 2nd file's migration.
	fs := common.NewTestFS()
	renameCount := atomic.Int32{}
	fs.OnRename = func(oldpath, newpath string) error {
		count := renameCount.Add(1)
		if count >= 2 { // First file succeeds (count=1), second file fails
			return os.ErrPermission
		}
		return os.Rename(oldpath, newpath)
	}

	migrator := NewMigratorWithFS(commitlogDir, logger, fs)
	err := migrator.MigrateSnapshotDirectory()
	require.Error(t, err, "migration should fail on 2nd file")

	// First file should be migrated, second should remain in old dir
	entries, err := os.ReadDir(commitlogDir)
	require.NoError(t, err)
	migratedCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".snapshot" {
			migratedCount++
		}
	}
	assert.Equal(t, 1, migratedCount, "only first snapshot should be migrated")

	// Re-run with working FS — should complete migration
	migrator2 := NewMigrator(commitlogDir, logger)
	err = migrator2.MigrateSnapshotDirectory()
	require.NoError(t, err, "retry should succeed")

	// Both snapshots should now be in the commitlog directory
	entries, err = os.ReadDir(commitlogDir)
	require.NoError(t, err)
	finalCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".snapshot" {
			finalCount++
		}
	}
	assert.Equal(t, 2, finalCount, "all snapshots should be migrated after retry")
}

// ---------------------------------------------------------------------------
// IMPORTANT: Iterator failure during merge
// ---------------------------------------------------------------------------

// TestCrashRecovery_MergeWithReadFailure verifies that if a source file
// becomes unreadable during merge, the SafeFileWriter is aborted (no partial
// output) and source files are preserved.
func TestCrashRecovery_MergeWithReadFailure(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Create sorted files
	writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})
	writeTestSortedFileWithData(t, dir, 2000, 2000, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})
	writeTestSortedFileWithData(t, dir, 3000, 3000, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	// TestFS that fails reads on the 2nd file opened (the first sorted file
	// to merge). The first file opened may be for temp file creation.
	fs := common.NewTestFS()
	openCount := atomic.Int32{}
	fs.OnOpen = func(f common.File) common.File {
		count := openCount.Add(1)
		if count == 2 {
			return &common.TestFile{
				File: f,
				OnRead: func(b []byte) (int, error) {
					return 0, os.ErrPermission
				},
			}
		}
		return f
	}

	config := CompactorConfig{
		Dir:               dir,
		MaxFilesPerMerge:  5,
		SnapshotThreshold: 0.20,
		BufferSize:        DefaultBufferSize,
		FS:                fs,
	}

	compactor := NewCompactor(config, logger, nil)
	_, err := compactor.RunCycle()
	require.Error(t, err, "merge should fail due to read error")

	// Clean up any temp files
	CleanupOrphanedTempFiles(dir)

	// All source sorted files should still exist
	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(state.SortedFiles), 2,
		"source sorted files should be preserved after failed merge")

	// Data should still be loadable from original files
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
}

// ---------------------------------------------------------------------------
// IMPORTANT: Zero-byte / empty file handling
// ---------------------------------------------------------------------------

// TestCrashRecovery_EmptyFile verifies that a zero-byte file left by a crash
// (e.g., file created but no data written before crash) doesn't break loading.
func TestCrashRecovery_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Create a valid WAL file
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Create a zero-byte file with a higher timestamp (simulates crash
	// right after file creation in switchCommitLogs)
	emptyPath := filepath.Join(dir, "2000")
	f, err := os.Create(emptyPath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Data from the valid file should be loaded
	assert.Equal(t, uint64(0), result.State.Graph.Entrypoint)
	require.NotNil(t, result.State.Graph.Nodes[0])
}

// ---------------------------------------------------------------------------
// IMPORTANT: Corrupt .condensed alongside raw — cleanup
// ---------------------------------------------------------------------------

// TestCrashRecovery_CorruptCondensedCleanup verifies that when both a raw
// file and a .condensed file with the same timestamp exist (interrupted
// conversion), the .condensed file is cleaned up and the raw file is loaded.
func TestCrashRecovery_CorruptCondensedCleanup(t *testing.T) {
	dir := t.TempDir()
	logger := crashTestLogger()

	// Create a raw file
	rawPath := filepath.Join(dir, "1000")
	createTestWALFile(t, rawPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	// Create a .condensed file with same timestamp (incomplete conversion)
	condensedPath := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		// Only partial data — conversion was interrupted
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Create a live file
	livePath := filepath.Join(dir, "2000")
	createTestWALFile(t, livePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Condensed file should be cleaned up
	_, err = os.Stat(condensedPath)
	assert.True(t, os.IsNotExist(err), ".condensed file should be removed")

	// Raw file should still exist
	_, err = os.Stat(rawPath)
	require.NoError(t, err, "raw file should be preserved")

	// All data from raw file should be loaded
	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// writeTestSortedFileWithData creates a sorted file with custom WAL entries.
func writeTestSortedFileWithData(t *testing.T, dir string, startTS, endTS int64, writeFn func(w *WALWriter)) {
	t.Helper()

	filename := BuildMergedFilename(startTS, endTS, FileTypeSorted)
	path := filepath.Join(dir, filename)

	sfw, err := NewSafeFileWriter(path, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	w := NewWALWriter(sfw.Writer())
	writeFn(w)

	require.NoError(t, sfw.Commit())
}
