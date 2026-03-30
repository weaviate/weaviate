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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loaderTestLogger() logrus.FieldLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

func TestLoader_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	assert.Nil(t, result, "empty directory should return nil state")
}

func TestLoader_NonExistentDirectory(t *testing.T) {
	loader := NewLoader(LoaderConfig{
		Dir:    "/non/existent/path",
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	assert.Nil(t, result, "non-existent directory should return nil state")
}

func TestLoader_SnapshotOnly(t *testing.T) {
	dir := t.TempDir()

	// Create a snapshot file with test data
	snapshotPath := filepath.Join(dir, "1000.snapshot")
	createTestSnapshot(t, snapshotPath, 42, 2, []testNode{
		{id: 0, level: 1, connections: [][]uint64{{1, 2}}, tombstone: false},
		{id: 1, level: 0, connections: [][]uint64{{0, 2}}, tombstone: false},
		{id: 2, level: 0, connections: [][]uint64{{0, 1}}, tombstone: false},
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, uint64(42), result.State.Graph.Entrypoint)
	assert.Equal(t, uint16(2), result.State.Graph.Level)
	assert.True(t, result.State.Graph.EntrypointChanged)

	// Verify nodes were loaded
	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
}

func TestLoader_WALFilesOnly(t *testing.T) {
	dir := t.TempDir()

	// Create a WAL file with test commits
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 1))
		require.NoError(t, w.WriteAddNode(0, 1))
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 1, 1))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, uint64(0), result.State.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.State.Graph.Level)
	assert.True(t, result.State.Graph.EntrypointChanged)

	// Verify nodes were loaded
	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
}

func TestLoader_SnapshotPlusWALFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a snapshot file
	snapshotPath := filepath.Join(dir, "1000.snapshot")
	createTestSnapshot(t, snapshotPath, 0, 1, []testNode{
		{id: 0, level: 1, connections: [][]uint64{{1}, {}}, tombstone: false},
		{id: 1, level: 0, connections: [][]uint64{{0}}, tombstone: false},
	})

	// Create a WAL file with additional data (timestamp > snapshot)
	walPath := filepath.Join(dir, "2000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(2, 0, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 2))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify snapshot + WAL data combined
	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
}

func TestLoader_MultipleWALFiles(t *testing.T) {
	dir := t.TempDir()

	// Create multiple WAL files with sequential timestamps
	walPath1 := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath1, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	walPath2 := filepath.Join(dir, "2000")
	createTestWALFile(t, walPath2, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 0))
	})

	walPath3 := filepath.Join(dir, "3000") // This is LiveFile
	createTestWALFile(t, walPath3, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 1))
		require.NoError(t, w.WriteSetEntryPointMaxLevel(2, 1))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify all data combined, including LiveFile
	assert.Equal(t, uint64(2), result.State.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.State.Graph.Level)

	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
}

func TestLoader_LiveFileIncluded(t *testing.T) {
	dir := t.TempDir()

	// Create a regular WAL file
	walPath1 := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath1, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Create a live file (highest timestamp raw file)
	liveFilePath := filepath.Join(dir, "2000") // This is LiveFile
	createTestWALFile(t, liveFilePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 1))
		require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 1))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify LiveFile was loaded (node 1 with level 1)
	assert.Equal(t, uint64(1), result.State.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.State.Graph.Level)
	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[1])
	assert.Equal(t, 1, result.State.Graph.Nodes[1].Level)
}

func TestLoader_OverlapFiltering(t *testing.T) {
	dir := t.TempDir()

	// Create a merged file (7_9.sorted) that contains timestamp 8
	mergedPath := filepath.Join(dir, "7_9.sorted")
	createTestWALFile(t, mergedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	// Create a file that would be overlapped (8.sorted)
	overlappedPath := filepath.Join(dir, "8.sorted")
	createTestWALFile(t, overlappedPath, func(w *WALWriter) {
		// This adds a different node that should NOT appear if filtered correctly
		require.NoError(t, w.WriteAddNode(99, 0))
	})

	// Create a live file
	livePath := filepath.Join(dir, "10")
	createTestWALFile(t, livePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the overlapped file's data was excluded
	// Node 99 should NOT exist if overlap filtering works
	if len(result.State.Graph.Nodes) > 99 {
		assert.Nil(t, result.State.Graph.Nodes[99], "overlapped file should be excluded")
	}

	// But nodes 0, 1, 2 should exist
	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
}

func TestLoader_FilesProcessedInOrder(t *testing.T) {
	dir := t.TempDir()

	// Create files in non-chronological order but they should be processed by timestamp
	// Earlier file sets entrypoint to 0
	walPath1 := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath1, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
	})

	// Later file changes entrypoint to 1
	walPath2 := filepath.Join(dir, "2000")
	createTestWALFile(t, walPath2, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 1))
		require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 1))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Final entrypoint should be 1 (from the later file)
	assert.Equal(t, uint64(1), result.State.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.State.Graph.Level)
}

func TestLoader_TombstonesHandled(t *testing.T) {
	dir := t.TempDir()

	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddTombstone(1))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify tombstone was recorded
	_, hasTombstone := result.State.Graph.Tombstones[1]
	assert.True(t, hasTombstone, "node 1 should have tombstone")
}

func TestLoader_OrphanedTmpFilesCleanedUp(t *testing.T) {
	dir := t.TempDir()

	// Create orphaned temp file
	tmpPath := filepath.Join(dir, "1000.snapshot.tmp")
	err := os.WriteFile(tmpPath, []byte("orphaned data"), 0o644)
	require.NoError(t, err)

	// Create a valid WAL file
	walPath := filepath.Join(dir, "1000")
	createTestWALFile(t, walPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify temp file was cleaned up
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "orphaned temp file should be removed")
}

func TestLoader_DefaultBufferSize(t *testing.T) {
	dir := t.TempDir()

	// Test with zero buffer size (should use default)
	loader := NewLoader(LoaderConfig{
		Dir:        dir,
		Logger:     loaderTestLogger(),
		BufferSize: 0,
	})
	assert.Equal(t, DefaultLoaderBufferSize, loader.config.BufferSize)

	// Test with negative buffer size (should use default)
	loader = NewLoader(LoaderConfig{
		Dir:        dir,
		Logger:     loaderTestLogger(),
		BufferSize: -1,
	})
	assert.Equal(t, DefaultLoaderBufferSize, loader.config.BufferSize)

	// Test with custom buffer size
	loader = NewLoader(LoaderConfig{
		Dir:        dir,
		Logger:     loaderTestLogger(),
		BufferSize: 1024,
	})
	assert.Equal(t, 1024, loader.config.BufferSize)
}

func TestLoader_CondensedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a condensed file
	condensedPath := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	// Create a live file (higher timestamp)
	livePath := filepath.Join(dir, "2000")
	createTestWALFile(t, livePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify all nodes loaded
	require.True(t, len(result.State.Graph.Nodes) > 2)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
}

func TestLoader_SortedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a sorted file
	sortedPath := filepath.Join(dir, "1000.sorted")
	createTestWALFile(t, sortedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Create a live file
	livePath := filepath.Join(dir, "2000")
	createTestWALFile(t, livePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
}

func TestLoader_WALFilesFilteredBySnapshot(t *testing.T) {
	dir := t.TempDir()

	// Create a snapshot with EndTS=1500
	snapshotPath := filepath.Join(dir, "1000_1500.snapshot")
	createTestSnapshot(t, snapshotPath, 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
	})

	// Create a WAL file with EndTS <= snapshot (should be filtered)
	oldWALPath := filepath.Join(dir, "1200.sorted")
	createTestWALFile(t, oldWALPath, func(w *WALWriter) {
		// This should be ignored because EndTS (1200) <= snapshot EndTS (1500)
		require.NoError(t, w.WriteAddNode(99, 0))
	})

	// Create a WAL file with EndTS > snapshot (should be included)
	newWALPath := filepath.Join(dir, "2000")
	createTestWALFile(t, newWALPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Node 99 should NOT exist if filtering works correctly
	if len(result.State.Graph.Nodes) > 99 {
		assert.Nil(t, result.State.Graph.Nodes[99], "old WAL file should be filtered")
	}

	// Node 1 from new WAL should exist
	require.True(t, len(result.State.Graph.Nodes) > 1)
	require.NotNil(t, result.State.Graph.Nodes[1])
}

func TestLoader_MixedFileTypes(t *testing.T) {
	dir := t.TempDir()

	// Create a snapshot
	snapshotPath := filepath.Join(dir, "1000.snapshot")
	createTestSnapshot(t, snapshotPath, 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
	})

	// Create a condensed file
	condensedPath := filepath.Join(dir, "2000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	// Create a sorted file
	sortedPath := filepath.Join(dir, "3000.sorted")
	createTestWALFile(t, sortedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	// Create a raw file
	rawPath := filepath.Join(dir, "4000")
	createTestWALFile(t, rawPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(3, 0))
	})

	// Create a live file
	livePath := filepath.Join(dir, "5000")
	createTestWALFile(t, livePath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(4, 0))
	})

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// All nodes should exist
	require.True(t, len(result.State.Graph.Nodes) > 4)
	require.NotNil(t, result.State.Graph.Nodes[0])
	require.NotNil(t, result.State.Graph.Nodes[1])
	require.NotNil(t, result.State.Graph.Nodes[2])
	require.NotNil(t, result.State.Graph.Nodes[3])
	require.NotNil(t, result.State.Graph.Nodes[4])
}

// TestLoader_CrashRecovery_TwoSnapshotsUsesNewest is a regression test for a
// bug in FileDiscovery.Scan where, when two snapshots coexist, the snapshot
// with the lowest StartTS was selected instead of the one with the highest EndTS.
//
// Crash scenario that triggers the bug:
//  1. Compactor merges old snapshot A (1000_3000) with sorted files 4000.sorted
//     and 5000.sorted into new snapshot B (1000_5000).
//  2. B is written atomically via SafeFileWriter rename. ✓
//  3. Compactor starts deleting sources. The sorted files are deleted first
//     (they come after A in StartTS order when there is a gap), or A's removal
//     fails transiently while the sorted file removals succeed.
//  4. Result on disk: A (1000_3000) and B (1000_5000) — no sorted files.
//
// On the next startup with the buggy selection:
//   - FileDiscovery picks A (alphabetically first: "1000_3000" < "1000_5000").
//   - No WAL files remain — the sorted files are gone.
//   - Only A is loaded → nodes 3 and 4 are missing. Data loss.
//
// With the fix (select highest EndTS):
//   - FileDiscovery picks B.
//   - B contains nodes 0-4. All data present.
func TestLoader_CrashRecovery_TwoSnapshotsUsesNewest(t *testing.T) {
	dir := t.TempDir()

	// Old snapshot A: contains nodes 0, 1, 2 — written in an earlier compaction.
	oldSnapshotPath := filepath.Join(dir, "1000_3000.snapshot")
	createTestSnapshot(t, oldSnapshotPath, 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 1, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 2, level: 0, connections: [][]uint64{{}}, tombstone: false},
	})

	// New snapshot B: contains nodes 0-4 — the compactor merged A + sorted files
	// into B. The sorted files were deleted but A's deletion failed.
	newSnapshotPath := filepath.Join(dir, "1000_5000.snapshot")
	createTestSnapshot(t, newSnapshotPath, 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 1, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 2, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 3, level: 0, connections: [][]uint64{{}}, tombstone: false},
		{id: 4, level: 0, connections: [][]uint64{{}}, tombstone: false},
	})

	// The source sorted files (4000.sorted, 5000.sorted) are intentionally absent:
	// they were already deleted before the crash / removal failure for A.
	// Without any WAL files, the loader must rely entirely on whichever snapshot
	// it selects — choosing A means nodes 3 and 4 are permanently lost.

	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: loaderTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	// All five nodes must be present. With the buggy snapshot selection the
	// loader picks A (nodes 0-2 only) and finds no WAL files to compensate,
	// so nodes 3 and 4 are permanently lost.
	require.True(t, len(result.State.Graph.Nodes) > 4,
		"expected nodes 0-4 to be present; loader may have selected the old snapshot")
	for i := uint64(0); i <= 4; i++ {
		require.NotNilf(t, result.State.Graph.Nodes[i],
			"node %d is missing — loader likely selected the old snapshot (1000_3000) "+
				"instead of the new one (1000_5000)", i)
	}
}

// Helper types and functions for creating test files

type testNode struct {
	id          uint64
	level       uint16
	connections [][]uint64
	tombstone   bool
}

func createTestWALFile(t *testing.T, path string, writeFunc func(w *WALWriter)) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	w := NewWALWriter(f)
	writeFunc(w)
}

func createTestSnapshot(t *testing.T, path string, entrypoint uint64, level uint16, nodes []testNode) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	sw := NewSnapshotWriterWithBlockSize(f, 1024)
	sw.SetEntrypoint(entrypoint, level)

	for _, n := range nodes {
		sw.AddNode(n.id, n.level, n.connections, n.tombstone)
	}

	err = sw.Flush()
	require.NoError(t, err)
}
