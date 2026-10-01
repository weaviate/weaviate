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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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

func TestCrashRecovery_CompleteCompactedWALFilesLoad(t *testing.T) {
	for _, tt := range []struct {
		name  string
		write func(*testing.T, string)
	}{
		{
			name: "sorted",
			write: func(t *testing.T, dir string) {
				writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
					require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
					require.NoError(t, w.WriteAddNode(0, 0))
					require.NoError(t, w.WriteAddNode(1, 0))
				})
			},
		},
		{
			name: "condensed",
			write: func(t *testing.T, dir string) {
				createTestWALFile(t, filepath.Join(dir, BuildMergedFilename(1000, 1000, FileTypeCondensed)), func(w *WALWriter) {
					require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
					require.NoError(t, w.WriteAddNode(0, 0))
					require.NoError(t, w.WriteAddNode(1, 0))
				})
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.write(t, dir)

			loader := NewLoader(LoaderConfig{Dir: dir, Logger: crashTestLogger()})
			result, err := loader.Load()
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.False(t, result.RecoveredFromCrash)
			require.GreaterOrEqual(t, len(result.State.Graph.Nodes), 2)
			require.NotNil(t, result.State.Graph.Nodes[0])
			require.NotNil(t, result.State.Graph.Nodes[1])
		})
	}
}

// TestCrashRecovery_TruncatedCompactedWALFilesRecover verifies that a corrupt
// .sorted/.condensed segment no longer fails the whole index load. Compacted
// files are written atomically, so an unreadable tail means on-disk corruption
// rather than a normal crash. Instead of erroring out (and taking the whole
// collection offline), the loader drops the bad segment, keeps the records read
// before the corruption, reports RecoveredFromCrash, and lets the caller
// recover from the snapshot + clean segments.
//
// Both corruption shapes are covered: a torn record (ErrUnexpectedEOF) and
// appended garbage that decodes to an unrecognized commit type — a non-EOF read
// error (see weaviate/0-weaviate-issues#195).
//
// Note: this only relaxes the *load* path. Compaction still fails closed on a
// corrupt merge input — see TestCrashRecovery_TruncatedSortedMergeFailsClosedAndKeepsSources.
func TestCrashRecovery_TruncatedCompactedWALFilesRecover(t *testing.T) {
	fileTypes := []struct {
		name  string
		path  func(string) string
		write func(*testing.T, string)
	}{
		{
			name: "sorted",
			path: func(dir string) string {
				return filepath.Join(dir, BuildMergedFilename(1000, 1000, FileTypeSorted))
			},
			write: func(t *testing.T, dir string) {
				writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
					require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
					require.NoError(t, w.WriteAddNode(0, 0))
					require.NoError(t, w.WriteAddNode(1, 0))
				})
			},
		},
		{
			name: "condensed",
			path: func(dir string) string {
				return filepath.Join(dir, BuildMergedFilename(1000, 1000, FileTypeCondensed))
			},
			write: func(t *testing.T, dir string) {
				createTestWALFile(t, filepath.Join(dir, BuildMergedFilename(1000, 1000, FileTypeCondensed)), func(w *WALWriter) {
					require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
					require.NoError(t, w.WriteAddNode(0, 0))
					require.NoError(t, w.WriteAddNode(1, 0))
				})
			},
		},
	}

	corruptions := []struct {
		name string
		tail []byte
	}{
		// Type byte followed by a partial body → io.ReadFull → ErrUnexpectedEOF.
		{name: "torn record", tail: []byte{byte(AddNode), 0x01, 0x02, 0x03}},
		// Bytes that decode to an unrecognized commit type → non-EOF read error.
		{name: "appended garbage", tail: []byte{0xFF, 0xFF, 0xFF, 0xFF}},
	}

	for _, ft := range fileTypes {
		for _, corrupt := range corruptions {
			t.Run(ft.name+"/"+corrupt.name, func(t *testing.T) {
				dir := t.TempDir()
				ft.write(t, dir)

				f, err := os.OpenFile(ft.path(dir), os.O_WRONLY|os.O_APPEND, 0o666)
				require.NoError(t, err)
				_, err = f.Write(corrupt.tail)
				require.NoError(t, err)
				require.NoError(t, f.Close())

				loader := NewLoader(LoaderConfig{Dir: dir, Logger: crashTestLogger()})
				result, err := loader.Load()
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.True(t, result.RecoveredFromCrash, "corrupt compacted segment should trigger crash recovery")

				// The complete records written before the corruption survive.
				require.GreaterOrEqual(t, len(result.State.Graph.Nodes), 2)
				require.NotNil(t, result.State.Graph.Nodes[0])
				require.NotNil(t, result.State.Graph.Nodes[1])

				// Recovery truncates the segment back to its last valid record,
				// so the file is now clean: a second load reads it without any
				// further recovery, and still sees the same surviving records.
				// This is what keeps the next compaction from tripping on it.
				result2, err := NewLoader(LoaderConfig{Dir: dir, Logger: crashTestLogger()}).Load()
				require.NoError(t, err)
				require.NotNil(t, result2)
				assert.False(t, result2.RecoveredFromCrash, "second load should be clean after truncation")
				require.GreaterOrEqual(t, len(result2.State.Graph.Nodes), 2)
				require.NotNil(t, result2.State.Graph.Nodes[0])
				require.NotNil(t, result2.State.Graph.Nodes[1])
			})
		}
	}
}

// TestCrashRecovery_TruncatedSortedMergeFailsClosedAndKeepsSources pins that both
// compaction paths reading .sorted inputs (merge and snapshot creation) fail
// without output and keep every source when an input has a torn or garbage
// tail, so the next load can repair it.
func TestCrashRecovery_TruncatedSortedMergeFailsClosedAndKeepsSources(t *testing.T) {
	paths := []struct {
		name  string
		files int64
	}{
		{name: "merge", files: 6},
		{name: "snapshot", files: 2},
	}
	tails := []struct {
		name  string
		tail  []byte
		errIs error
	}{
		{name: "torn record", tail: []byte{byte(AddNode), 0x01, 0x02, 0x03}, errIs: io.ErrUnexpectedEOF},
		{name: "reset after nodes", tail: []byte{byte(ResetIndex)}},
		{name: "compression after nodes", tail: walBytes(t, func(w *WALWriter) {
			require.NoError(t, w.WriteAddSQ(&compression.SQData{A: 1, B: 2, Dimensions: 4}))
		})},
	}

	for _, p := range paths {
		for _, tc := range tails {
			t.Run(p.name+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				sources := make([]string, 0, p.files)
				for i := int64(0); i < p.files; i++ {
					ts := 1000 + i
					writeTestSortedFileWithData(t, dir, ts, ts, func(w *WALWriter) {
						require.NoError(t, w.WriteSetEntryPointMaxLevel(uint64(i), 0))
						require.NoError(t, w.WriteAddNode(uint64(i), 0))
					})
					sources = append(sources, filepath.Join(dir, BuildMergedFilename(ts, ts, FileTypeSorted)))
				}
				appendToFile(t, sources[0], tc.tail)

				before, err := os.ReadDir(dir)
				require.NoError(t, err)

				action, err := NewCompactor(DefaultCompactorConfig(dir), crashTestLogger()).RunCycle(nil)
				require.Error(t, err)
				if tc.errIs != nil {
					require.ErrorIs(t, err, tc.errIs)
				}
				assert.Equal(t, ActionNone, action)

				// No source removed, no output or temp file left behind.
				after, err := os.ReadDir(dir)
				require.NoError(t, err)
				names := func(entries []os.DirEntry) []string {
					out := make([]string, 0, len(entries))
					for _, e := range entries {
						out = append(out, e.Name())
					}
					return out
				}
				assert.ElementsMatch(t, names(before), names(after))
			})
		}
	}
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

	// Create 3 sorted files with distinct data. Within a .sorted file every
	// commit is grouped under its source node in ascending node-ID order (see
	// SortedWriter.WriteAll), so a backlink from node 0 is written before
	// node 1's AddNode, and a backlink from node 1 before node 2's AddNode.
	writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})
	writeTestSortedFileWithData(t, dir, 2000, 2000, func(w *WALWriter) {
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
		require.NoError(t, w.WriteAddNode(1, 0))
	})
	writeTestSortedFileWithData(t, dir, 3000, 3000, func(w *WALWriter) {
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 2))
		require.NoError(t, w.WriteAddNode(2, 0))
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
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle(nil)
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
	compactor2 := NewCompactor(config, logger)

	_, err = compactor2.RunCycle(nil)
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

	compactor := NewCompactor(config, logger)
	_, err := compactor.RunCycle(nil)
	require.Error(t, err, "should fail due to rename")

	// Raw file should still exist
	_, err = os.Stat(rawPath)
	require.NoError(t, err, "original raw file should be preserved")

	// Clean up orphaned .tmp files (startup recovery)
	CleanupOrphanedTempFiles(dir)

	// Retry with working FS
	config.FS = common.NewOSFS()
	compactor2 := NewCompactor(config, logger)
	_, err = compactor2.RunCycle(nil)
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

	compactor := NewCompactor(config, logger)
	_, err := compactor.RunCycle(nil)
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

// TestCrashRecovery_TruncatedWALFile_TornAtFieldBoundary covers a crash that
// cuts the last commit exactly between two of its fields. The loader must treat
// it like any other torn tail: truncate the fragment, so that commits appended
// to the file later start on a commit boundary.
func TestCrashRecovery_TruncatedWALFile_TornAtFieldBoundary(t *testing.T) {
	cases := []struct {
		name string
		keep int // bytes of the torn AddLinkAtLevel(2, 0, 1) that reached disk
	}{
		{name: "after commit type", keep: 1},
		{name: "after source", keep: 9},
		{name: "after level", keep: 11},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			walPath := filepath.Join(dir, "1000")
			createTestWALFile(t, walPath, func(w *WALWriter) {
				require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 0))
				require.NoError(t, w.WriteAddNode(1, 0))
				require.NoError(t, w.WriteAddNode(2, 0))
			})
			info, err := os.Stat(walPath)
			require.NoError(t, err)
			validSize := info.Size()

			var torn bytes.Buffer
			require.NoError(t, NewWALWriter(&torn).WriteAddLinkAtLevel(2, 0, 1))
			appendToFile(t, walPath, torn.Bytes()[:tc.keep])

			result, err := NewLoader(LoaderConfig{Dir: dir, Logger: crashTestLogger()}).Load()
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.RecoveredFromCrash)

			info, err = os.Stat(walPath)
			require.NoError(t, err)
			require.Equal(t, validSize, info.Size(), "torn fragment must be truncated")

			// a later writer appends to the repaired file; its commits must read back intact
			var next bytes.Buffer
			require.NoError(t, NewWALWriter(&next).WriteAddNode(4703, 0))
			require.NoError(t, NewWALWriter(&next).WriteAddLinkAtLevel(4703, 0, 1))
			appendToFile(t, walPath, next.Bytes())

			result, err = NewLoader(LoaderConfig{Dir: dir, Logger: crashTestLogger()}).Load()
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.False(t, result.RecoveredFromCrash)
			nodes := result.State.Graph.Nodes
			require.Less(t, len(nodes), 10_000, "a misaligned read decodes huge node IDs")
			require.NotNil(t, nodes[4703])
			assert.Equal(t, []uint64{1}, nodes[4703].Connections.GetLayer(0))
		})
	}
}

func appendToFile(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	_, err = f.Write(b)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// ---------------------------------------------------------------------------
// Garbage appended to compacted segments (weaviate/0-weaviate-issues#666)
// ---------------------------------------------------------------------------

func corruptTailRQ(seed float32) *compression.RQData {
	return &compression.RQData{
		InputDim: 4,
		Bits:     8,
		Rotation: compression.FastRotation{
			OutputDim: 4,
			Rounds:    1,
			Swaps:     [][]compression.Swap{{{I: 0, J: 1}, {I: 2, J: 3}}},
			Signs:     [][]float32{{seed, -1, 1, -1}},
		},
	}
}

func corruptTailMuvera() *multivector.MuveraData {
	return &multivector.MuveraData{
		KSim: 1, NumClusters: 2, Dimensions: 2, DProjections: 1, Repetitions: 1,
		Gaussians: [][][]float32{{{0.1, 0.2}}},
		S:         [][][]float32{{{0.3, 0.4}}},
	}
}

// writeCorruptTailFixture writes a valid compacted segment holding an RQ
// compressor, an entrypoint, three linked nodes and a tombstone, in the record
// layout the respective writer produces: SortedWriter for .sorted, the legacy
// MemoryCondensor (nodes descending, entrypoint and tombstones last) for
// .condensed.
func writeCorruptTailFixture(t *testing.T, path string, fileType FileType, withMuvera bool) {
	t.Helper()
	createTestWALFile(t, path, func(w *WALWriter) {
		require.NoError(t, w.WriteAddRQ(corruptTailRQ(1)))
		if withMuvera {
			require.NoError(t, w.WriteAddMuvera(corruptTailMuvera()))
		}
		switch fileType {
		case FileTypeSorted:
			require.NoError(t, w.WriteSetEntryPointMaxLevel(2, 1))
			require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1, 2}))
			require.NoError(t, w.WriteAddTombstone(1))
			require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{0, 2}))
			require.NoError(t, w.WriteAddNode(2, 1))
			require.NoError(t, w.WriteAddLinksAtLevel(2, 0, []uint64{0, 1}))
			require.NoError(t, w.WriteAddLinksAtLevel(2, 1, []uint64{}))
		case FileTypeCondensed:
			require.NoError(t, w.WriteAddNode(2, 1))
			require.NoError(t, w.WriteAddLinksAtLevel(2, 0, []uint64{0, 1}))
			require.NoError(t, w.WriteAddLinksAtLevel(2, 1, []uint64{}))
			require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{0, 2}))
			require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1, 2}))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(2, 1))
			require.NoError(t, w.WriteAddTombstone(1))
		default:
			t.Fatalf("unexpected file type %s", fileType)
		}
	})
}

func walBytes(t *testing.T, fn func(w *WALWriter)) []byte {
	t.Helper()
	var buf bytes.Buffer
	fn(NewWALWriter(&buf))
	return buf.Bytes()
}

func fileSizeOf(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	require.NoError(t, err)
	return st.Size()
}

func assertMuveraEqual(t *testing.T, want, got *ent.DeserializationResult) {
	t.Helper()
	require.Equal(t, want.MuveraEnabled(), got.MuveraEnabled(), "muvera enabled")
	require.Equal(t, want.EncoderMuvera(), got.EncoderMuvera(), "muvera encoder")
}

// assertCorruptTailRecovered loads the fixture with tail appended and checks
// that both that load and the next one match the clean fixture exactly.
func assertCorruptTailRecovered(t *testing.T, fileType FileType, withMuvera bool, tail []byte) {
	t.Helper()
	name := BuildMergedFilename(1000, 1000, fileType)

	cleanDir := t.TempDir()
	writeCorruptTailFixture(t, filepath.Join(cleanDir, name), fileType, withMuvera)
	clean, err := NewLoader(LoaderConfig{Dir: cleanDir, Logger: quietLogger()}).Load()
	require.NoError(t, err)
	require.False(t, clean.RecoveredFromCrash)
	cleanSize := fileSizeOf(t, filepath.Join(cleanDir, name))

	dir := t.TempDir()
	path := filepath.Join(dir, name)
	writeCorruptTailFixture(t, path, fileType, withMuvera)
	appendToFile(t, path, tail)

	first, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger()}).Load()
	require.NoError(t, err)
	assert.True(t, first.RecoveredFromCrash, "garbage tail must be detected as corruption")
	assertGraphEqual(t, clean.State, first.State)
	assertMuveraEqual(t, clean.State, first.State)
	require.Equal(t, cleanSize, fileSizeOf(t, path), "file must be truncated to the end of the valid records")

	second, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger()}).Load()
	require.NoError(t, err)
	assert.False(t, second.RecoveredFromCrash, "second load must be clean after truncation")
	assertGraphEqual(t, clean.State, second.State)
	assertMuveraEqual(t, clean.State, second.State)
}

// TestCrashRecovery_CompactedGarbageTailIsNeverApplied pins that records decoded
// from garbage appended to a compacted segment are never applied to the graph
// and never kept by the truncation. A ResetIndex byte used to wipe the whole
// shard graph (and the truncation persisted it), and a compression record used
// to install a bogus quantizer next to the real one.
func TestCrashRecovery_CompactedGarbageTailIsNeverApplied(t *testing.T) {
	sqRecord := []byte{byte(AddSQ)}
	sqRecord = binary.LittleEndian.AppendUint32(sqRecord, 0xABABABAB)
	sqRecord = binary.LittleEndian.AppendUint32(sqRecord, 0xABABABAB)
	sqRecord = binary.LittleEndian.AppendUint16(sqRecord, 0xABAB)

	// Tile-encoded PQ with one segment: 10-byte header + 51-byte encoder.
	pqRecord := []byte{byte(AddPQ)}
	pqRecord = binary.LittleEndian.AppendUint16(pqRecord, 4)      // dims
	pqRecord = append(pqRecord, byte(compression.UseTileEncoder)) // encoder
	pqRecord = binary.LittleEndian.AppendUint16(pqRecord, 256)    // ks
	pqRecord = binary.LittleEndian.AppendUint16(pqRecord, 1)      // m
	pqRecord = append(pqRecord, 0, 0)                             // distribution, bits encoding
	pqRecord = append(pqRecord, bytes.Repeat([]byte{0x3F}, 6*8+2+1)...)

	brqRecord := walBytes(t, func(w *WALWriter) {
		require.NoError(t, w.WriteAddBRQ(&compression.BRQData{
			InputDim: 4,
			Rotation: compression.FastRotation{
				OutputDim: 4, Rounds: 1,
				Swaps: [][]compression.Swap{{{I: 0, J: 1}, {I: 2, J: 3}}},
				Signs: [][]float32{{1, 1, 1, 1}},
			},
			Rounding: []float32{0, 0, 0, 0},
		}))
	})

	tails := []struct {
		name string
		tail []byte
		// sortedOnly marks tails that are only detectable in .sorted files: the
		// legacy condensed layout writes nodes in descending order and the
		// entrypoint after them.
		sortedOnly bool
	}{
		{name: "reset then unknown type", tail: []byte{byte(ResetIndex), 0xFF}},
		{name: "reset at end of file", tail: []byte{byte(ResetIndex)}},
		{name: "reset in 512 garbage bytes", tail: append([]byte{byte(ResetIndex)}, bytes.Repeat([]byte{0xF7}, 511)...)},
		{name: "SQ record", tail: sqRecord},
		{name: "PQ record", tail: pqRecord},
		{name: "second RQ record", tail: walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddRQ(corruptTailRQ(-1))) })},
		{name: "BRQ record", tail: brqRecord},
		{name: "muvera record", tail: walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddMuvera(corruptTailMuvera())) })},
		{name: "entrypoint record", tail: walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0)) }), sortedOnly: true},
		// Zeroed blocks decode as AddNode(0, 0) records.
		{name: "zero-filled block", tail: make([]byte, 512), sortedOnly: true},
		{name: "lower node ID record", tail: walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(0, 3)) }), sortedOnly: true},
	}

	for _, fileType := range []FileType{FileTypeSorted, FileTypeCondensed} {
		for _, withMuvera := range []bool{false, true} {
			for _, tc := range tails {
				if tc.sortedOnly && fileType != FileTypeSorted {
					continue
				}
				t.Run(fmt.Sprintf("%s/muvera=%v/%s", fileType, withMuvera, tc.name), func(t *testing.T) {
					assertCorruptTailRecovered(t, fileType, withMuvera, tc.tail)
				})
			}
		}
	}
}

// TestCrashRecovery_CorruptCompressionHeaderAllocationIsBounded pins that a
// garbage compression record cannot make the decoder allocate memory out of
// proportion to the bytes it actually read: slice sizes come from unchecked
// uint32 header fields, so a 17-byte tail used to request up to ~100 GiB.
func TestCrashRecovery_CorruptCompressionHeaderAllocationIsBounded(t *testing.T) {
	const huge = 1 << 22

	rqHeader := func(typ HnswCommitType, outputDim, rounds uint32) []byte {
		b := []byte{byte(typ)}
		if typ == AddRQCentered {
			b = append(b, 0)
		}
		b = binary.LittleEndian.AppendUint32(b, 4) // inputDim
		b = binary.LittleEndian.AppendUint32(b, 8) // bits
		b = binary.LittleEndian.AppendUint32(b, outputDim)
		return binary.LittleEndian.AppendUint32(b, rounds)
	}
	// The mean length must equal the input dimension, so both are garbage.
	centeredRQMean := func(dim uint32) []byte {
		b := []byte{byte(AddRQCentered), 0}
		b = binary.LittleEndian.AppendUint32(b, dim) // inputDim
		b = binary.LittleEndian.AppendUint32(b, 8)   // bits
		b = binary.LittleEndian.AppendUint32(b, 2)   // outputDim
		b = binary.LittleEndian.AppendUint32(b, 0)   // rounds
		return binary.LittleEndian.AppendUint32(b, dim)
	}
	brqHeader := func(outputDim, rounds uint32) []byte {
		b := []byte{byte(AddBRQ)}
		b = binary.LittleEndian.AppendUint32(b, 4) // inputDim
		b = binary.LittleEndian.AppendUint32(b, outputDim)
		return binary.LittleEndian.AppendUint32(b, rounds)
	}
	muveraHeader := func(kSim, dims, dProjections, repetitions uint32) []byte {
		b := []byte{byte(AddMuvera)}
		b = binary.LittleEndian.AppendUint32(b, kSim)
		b = binary.LittleEndian.AppendUint32(b, 1<<kSim) // numClusters
		b = binary.LittleEndian.AppendUint32(b, dims)
		b = binary.LittleEndian.AppendUint32(b, dProjections)
		return binary.LittleEndian.AppendUint32(b, repetitions)
	}
	pqKMeansHeader := func(dims, ks, m uint16) []byte {
		b := []byte{byte(AddPQ)}
		b = binary.LittleEndian.AppendUint16(b, dims)
		b = append(b, byte(compression.UseKMeansEncoder))
		b = binary.LittleEndian.AppendUint16(b, ks)
		b = binary.LittleEndian.AppendUint16(b, m)
		return append(b, 0, 0)
	}

	tails := []struct {
		name string
		tail []byte
	}{
		{name: "RQ rounds", tail: rqHeader(AddRQ, 2, huge)},
		{name: "RQ zero-payload rounds", tail: rqHeader(AddRQ, 0, huge)},
		{name: "centered RQ rounds", tail: rqHeader(AddRQCentered, 2, huge)},
		{name: "centered RQ mean", tail: centeredRQMean(1 << 24)},
		{name: "BRQ rounds", tail: brqHeader(2, huge)},
		{name: "BRQ zero-payload rounds", tail: brqHeader(0, huge)},
		{name: "muvera repetitions", tail: muveraHeader(1, 1, 1, huge)},
		{name: "muvera zero-payload repetitions", tail: muveraHeader(0, 0, 0, huge)},
		{name: "PQ k-means zero-width segments", tail: pqKMeansHeader(1, 2048, 2048)},
	}

	for _, tc := range tails {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			assertCorruptTailRecovered(t, FileTypeSorted, false, tc.tail)
			runtime.ReadMemStats(&after)

			allocated := after.TotalAlloc - before.TotalAlloc
			assert.Less(t, allocated, uint64(16<<20),
				"a %d-byte garbage record allocated %d MiB", len(tc.tail), allocated>>20)
		})
	}
}

// TestCrashRecovery_ValidCompactedLayoutsAreNotTruncated guards the other side
// of garbage detection: every record layout a compaction writer produces must
// load without being treated as corrupt.
func TestCrashRecovery_ValidCompactedLayoutsAreNotTruncated(t *testing.T) {
	tests := []struct {
		name     string
		fileType FileType
		write    func(w *WALWriter)
	}{
		{
			name:     "sorted: compression, muvera, entrypoint, nodes",
			fileType: FileTypeSorted,
			write: func(w *WALWriter) {
				require.NoError(t, w.WriteAddRQ(corruptTailRQ(1)))
				require.NoError(t, w.WriteAddMuvera(corruptTailMuvera()))
				require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 0))
				require.NoError(t, w.WriteAddTombstone(0))
				require.NoError(t, w.WriteDeleteNode(0))
				require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{2}))
				require.NoError(t, w.WriteRemoveTombstone(2))
				require.NoError(t, w.WriteReplaceLinksAtLevel(2, 0, []uint64{1}))
				require.NoError(t, w.WriteClearLinksAtLevel(3, 0))
			},
		},
		{
			// The n-way merger emits every compression type it saw, in this order.
			name:     "sorted: merged globals from several compression types",
			fileType: FileTypeSorted,
			write: func(w *WALWriter) {
				require.NoError(t, w.WriteAddSQ(&compression.SQData{A: 1, B: 2, Dimensions: 4}))
				require.NoError(t, w.WriteAddRQ(corruptTailRQ(1)))
				require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
				require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{}))
			},
		},
		{
			name:     "sorted: globals only",
			fileType: FileTypeSorted,
			write: func(w *WALWriter) {
				require.NoError(t, w.WriteAddRQ(corruptTailRQ(1)))
				require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
			},
		},
		{
			name:     "sorted: nodes only",
			fileType: FileTypeSorted,
			write: func(w *WALWriter) {
				require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1}))
				require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{0}))
			},
		},
		{
			name:     "condensed: legacy layout with every record kind",
			fileType: FileTypeCondensed,
			write: func(w *WALWriter) {
				require.NoError(t, w.WriteAddRQ(corruptTailRQ(1)))
				require.NoError(t, w.WriteAddMuvera(corruptTailMuvera()))
				require.NoError(t, w.WriteAddNode(3, 1))
				require.NoError(t, w.WriteAddLinksAtLevel(3, 0, []uint64{1}))
				require.NoError(t, w.WriteReplaceLinksAtLevel(1, 0, []uint64{3}))
				require.NoError(t, w.WriteSetEntryPointMaxLevel(3, 1))
				require.NoError(t, w.WriteAddTombstone(1))
				require.NoError(t, w.WriteRemoveTombstone(2))
				require.NoError(t, w.WriteDeleteNode(0))
			},
		},
		{
			name:     "condensed: written before compression existed",
			fileType: FileTypeCondensed,
			write: func(w *WALWriter) {
				require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{0}))
				require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1}))
				require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 0))
				require.NoError(t, w.WriteAddTombstone(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, BuildMergedFilename(1000, 1000, tc.fileType))
			createTestWALFile(t, path, tc.write)
			size := fileSizeOf(t, path)

			res, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger()}).Load()
			require.NoError(t, err)
			assert.False(t, res.RecoveredFromCrash, "valid layout must not be treated as corrupt")
			assert.Equal(t, size, fileSizeOf(t, path), "valid file must not be truncated")
		})
	}
}

// TestCrashRecovery_CorruptCondensedResetKeepsOlderFiles pins the compaction
// side of #666: converting a .condensed file whose garbage tail decodes as a
// ResetIndex must not discard every older commit log as if a real reset had
// happened.
func TestCrashRecovery_CorruptCondensedResetKeepsOlderFiles(t *testing.T) {
	dir := t.TempDir()
	writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 0))
		require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{2}))
		require.NoError(t, w.WriteAddLinksAtLevel(2, 0, []uint64{1}))
	})
	condensedPath := filepath.Join(dir, BuildMergedFilename(2000, 2000, FileTypeCondensed))
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddLinksAtLevel(3, 0, []uint64{1}))
	})
	appendToFile(t, condensedPath, []byte{byte(ResetIndex)})
	createTestWALFile(t, filepath.Join(dir, "3000"), func(w *WALWriter) {
		require.NoError(t, w.WriteAddLinksAtLevel(4, 0, []uint64{3}))
	})

	// The corruption can appear after the startup load, so compaction may be
	// the first to read it. Its outcome may be an error, but never data loss.
	_, _ = NewCompactor(DefaultCompactorConfig(dir), quietLogger()).RunCycle(nil)

	assertNodes := func(label string) {
		t.Helper()
		state := loadGraph(t, dir)
		for id := 1; id <= 4; id++ {
			require.False(t, effectivelyAbsent(nodeAt(state, id)), "%s: node %d lost", label, id)
		}
	}
	assertNodes("after compaction cycle")

	compactToFixedPoint(t, dir)
	assertNodes("after compacting to fixed point")
}

// TestCrashRecovery_RecordTornAtFieldBoundary pins that a record cut off
// exactly between two of its fields is detected as torn. io.ReadFull reports a
// zero-byte read as io.EOF, which used to pass for a clean end of file, so the
// fragment was neither reported nor truncated.
func TestCrashRecovery_RecordTornAtFieldBoundary(t *testing.T) {
	tails := []struct {
		name string
		tail []byte
	}{
		{name: "type byte only", tail: []byte{byte(AddNode)}},
		{name: "type byte and node id", tail: binary.LittleEndian.AppendUint64([]byte{byte(AddNode)}, 7)},
		{name: "links header without targets", tail: binary.LittleEndian.AppendUint16(
			binary.LittleEndian.AppendUint16(
				binary.LittleEndian.AppendUint64([]byte{byte(AddLinksAtLevel)}, 1), 0), 2)},
	}

	for _, fileType := range []FileType{FileTypeRaw, FileTypeSorted, FileTypeCondensed} {
		for _, tc := range tails {
			t.Run(fmt.Sprintf("%s/%s", fileType, tc.name), func(t *testing.T) {
				dir := t.TempDir()
				name := "1000"
				if fileType != FileTypeRaw {
					name = BuildMergedFilename(1000, 1000, fileType)
				}
				path := filepath.Join(dir, name)
				createTestWALFile(t, path, func(w *WALWriter) {
					require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
					require.NoError(t, w.WriteAddLinksAtLevel(0, 0, []uint64{1}))
					require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{0}))
				})
				validSize := fileSizeOf(t, path)
				appendToFile(t, path, tc.tail)

				res, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger()}).Load()
				require.NoError(t, err)
				assert.True(t, res.RecoveredFromCrash, "torn record must be reported")
				assert.Equal(t, validSize, fileSizeOf(t, path), "torn record must be truncated")
				require.NotNil(t, nodeAt(res.State, 0))
				require.NotNil(t, nodeAt(res.State, 1))
			})
		}
	}
}

// TestCompactedLayout_ClassifiesEveryRecordType pins that the layout check and
// the merge iterator agree on which records are global. The check treats any
// record it does not list as a node record, so a global type it missed would
// make it reject the valid records after it and truncate them.
func TestCompactedLayout_ClassifiesEveryRecordType(t *testing.T) {
	pqRecord := []byte{byte(AddPQ)}
	pqRecord = binary.LittleEndian.AppendUint16(pqRecord, 4)
	pqRecord = append(pqRecord, byte(compression.UseTileEncoder))
	pqRecord = binary.LittleEndian.AppendUint16(pqRecord, 256)
	pqRecord = binary.LittleEndian.AppendUint16(pqRecord, 1)
	pqRecord = append(pqRecord, 0, 0)
	pqRecord = append(pqRecord, bytes.Repeat([]byte{0x3F}, 6*8+2+1)...)

	centered := corruptTailRQ(1)
	centered.Mean = []float32{0, 0, 0, 0}

	records := map[HnswCommitType][]byte{
		AddNode:               walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(1, 0)) }),
		SetEntryPointMaxLevel: walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 0)) }),
		AddLinkAtLevel:        walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 2)) }),
		ReplaceLinksAtLevel:   walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteReplaceLinksAtLevel(1, 0, []uint64{2})) }),
		AddTombstone:          walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddTombstone(1)) }),
		RemoveTombstone:       walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteRemoveTombstone(1)) }),
		ClearLinks:            walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteClearLinks(1)) }),
		DeleteNode:            walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteDeleteNode(1)) }),
		ResetIndex:            walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteResetIndex()) }),
		ClearLinksAtLevel:     walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteClearLinksAtLevel(1, 0)) }),
		AddLinksAtLevel:       walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{2})) }),
		AddPQ:                 pqRecord,
		AddSQ:                 walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddSQ(&compression.SQData{A: 1, B: 2, Dimensions: 4})) }),
		AddMuvera:             walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddMuvera(corruptTailMuvera())) }),
		AddRQ:                 walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddRQ(corruptTailRQ(1))) }),
		AddBRQ: walBytes(t, func(w *WALWriter) {
			require.NoError(t, w.WriteAddBRQ(&compression.BRQData{
				InputDim: 4,
				Rotation: compression.FastRotation{
					OutputDim: 4, Rounds: 1,
					Swaps: [][]compression.Swap{{{I: 0, J: 1}, {I: 2, J: 3}}},
					Signs: [][]float32{{1, 1, 1, 1}},
				},
				Rounding: []float32{0, 0, 0, 0},
			}))
		}),
		AddRQCentered: walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddRQ(centered)) }),
	}

	// Every type byte the decoder recognizes must be covered above.
	for b := 0; b < 256; b++ {
		ct := HnswCommitType(b)
		_, err := NewWALCommitReader(bytes.NewReader([]byte{byte(ct)}), quietLogger()).ReadNextCommit()
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			continue // unrecognized type
		}
		_, covered := records[ct]
		require.True(t, covered, "record type %d (%s) is decodable but not covered", b, ct)
	}

	for ct, record := range records {
		t.Run(ct.String(), func(t *testing.T) {
			c, err := NewWALCommitReader(bytes.NewReader(record), quietLogger()).ReadNextCommit()
			require.NoError(t, err)

			global := isGlobalCommit(c)
			_, hasNodeID := extractNodeID(c)
			require.NotEqual(t, global, hasNodeID, "a record is either global or belongs to a node")

			for _, fileType := range []FileType{FileTypeSorted, FileTypeCondensed} {
				layout := compactedLayout{fileType: fileType}
				err := layout.check(c)
				if ct == ResetIndex {
					require.Error(t, err, "compacted segments never contain a reset")
					continue
				}
				require.NoError(t, err)
				require.Equal(t, !global, layout.nodesStarted,
					"%s: layout check and merge iterator disagree on whether the record is global", fileType)
			}
		})
	}
}

// TestCrashRecovery_ImpossibleCompressionRecordInEmptySegment pins that a PQ
// record no quantizer writes is rejected even where the layout allows a
// compression record: at the head of an empty .sorted segment, which forced
// rotations leave behind. Such records crash or fail startup, and a k-means
// record with zero centroids or segments carries no payload at all.
func TestCrashRecovery_ImpossibleCompressionRecordInEmptySegment(t *testing.T) {
	pqHeader := func(encoder compression.Encoder, dims, ks, m uint16) []byte {
		b := []byte{byte(AddPQ)}
		b = binary.LittleEndian.AppendUint16(b, dims)
		b = append(b, byte(encoder))
		b = binary.LittleEndian.AppendUint16(b, ks)
		b = binary.LittleEndian.AppendUint16(b, m)
		b = append(b, 0, 0)
		if encoder == compression.UseTileEncoder {
			// One tile encoder per segment, at least one so the record is followed by bytes.
			return append(b, bytes.Repeat([]byte{0x3F}, max(int(m), 1)*(6*8+2+1))...)
		}
		// A k-means record with zero centroids carries no payload.
		return b
	}

	tails := []struct {
		name string
		tail []byte
	}{
		{name: "PQ with zero segments", tail: pqHeader(compression.UseTileEncoder, 4, 256, 0)},
		{name: "PQ with zero segments at end of file", tail: pqHeader(compression.UseTileEncoder, 4, 256, 0)[:10]},
		{name: "k-means PQ with zero centroids", tail: pqHeader(compression.UseKMeansEncoder, 4, 0, 1)},
		{name: "PQ with segments not dividing dimensions", tail: pqHeader(compression.UseTileEncoder, 5, 256, 2)},
	}

	for _, tc := range tails {
		t.Run(tc.name, func(t *testing.T) {
			write := func(dir string) string {
				writeCorruptTailFixture(t, filepath.Join(dir, BuildMergedFilename(1000, 1000, FileTypeSorted)), FileTypeSorted, false)
				empty := filepath.Join(dir, BuildMergedFilename(2000, 2000, FileTypeSorted))
				createTestWALFile(t, empty, func(w *WALWriter) {})
				return empty
			}

			cleanDir := t.TempDir()
			write(cleanDir)
			clean, err := NewLoader(LoaderConfig{Dir: cleanDir, Logger: quietLogger()}).Load()
			require.NoError(t, err)

			dir := t.TempDir()
			empty := write(dir)
			appendToFile(t, empty, tc.tail)

			res, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger()}).Load()
			require.NoError(t, err)
			assert.True(t, res.RecoveredFromCrash, "impossible record must be detected as corruption")
			assertGraphEqual(t, clean.State, res.State)
			assert.Equal(t, int64(0), fileSizeOf(t, empty), "segment must be truncated back to empty")
		})
	}
}

// ---------------------------------------------------------------------------
// Node IDs beyond the index's limit (weaviate/0-weaviate-issues#649)
// ---------------------------------------------------------------------------

const (
	nodeIDLimitTestCounter = 10
	nodeIDLimitTestMax     = nodeIDLimitTestCounter + docIDCounterSlack
	nodeIDLimitTestGarbage = nodeIDLimitTestMax + 1000
)

// writeNodeIDLimitFixture writes ten linked nodes, the entrypoint and a
// tombstone in the record layout each writer produces.
func writeNodeIDLimitFixture(t *testing.T, path string, fileType FileType) {
	t.Helper()
	createTestWALFile(t, path, func(w *WALWriter) {
		writeNode := func(id uint64) {
			require.NoError(t, w.WriteAddNode(id, 0))
			require.NoError(t, w.WriteAddLinksAtLevel(id, 0, []uint64{(id + 1) % 10}))
		}
		switch fileType {
		case FileTypeSorted:
			require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
			for id := uint64(0); id < 10; id++ {
				writeNode(id)
			}
		case FileTypeCondensed:
			for id := uint64(9); ; id-- {
				writeNode(id)
				if id == 0 {
					break
				}
			}
			require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
			require.NoError(t, w.WriteAddTombstone(3))
		default:
			require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
			for id := uint64(0); id < 10; id++ {
				writeNode(id)
			}
			require.NoError(t, w.WriteAddTombstone(3))
		}
	})
}

// nodeIDLimitTestDir returns a commit log directory inside a shard directory
// whose document-ID counter is nodeIDLimitTestCounter.
func nodeIDLimitTestDir(t *testing.T) string {
	t.Helper()
	shardDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"),
		binary.LittleEndian.AppendUint64(nil, nodeIDLimitTestCounter), 0o644))
	dir := filepath.Join(shardDir, "main.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return dir
}

func nodeIDLimitTestName(fileType FileType) string {
	if fileType == FileTypeRaw {
		return "1000"
	}
	return BuildMergedFilename(1000, 1000, fileType)
}

// TestCrashRecovery_NodeIDBeyondLimitIsNeverApplied pins that a record naming a
// node ID above the index's limit is treated as corruption: it is neither
// applied nor kept, and the node index is never sized to it. A misaligned read
// of a torn log decodes such IDs, and sizing the index to one ran the node out
// of memory on every startup.
func TestCrashRecovery_NodeIDBeyondLimitIsNeverApplied(t *testing.T) {
	const g = nodeIDLimitTestGarbage

	records := []struct {
		name  string
		write func(w *WALWriter) error
		// rawOnly marks records the compacted-segment layout check already
		// rejects, since the fixtures hold an entrypoint.
		rawOnly bool
	}{
		{name: "add node", write: func(w *WALWriter) error { return w.WriteAddNode(g, 0) }},
		{name: "entrypoint", write: func(w *WALWriter) error { return w.WriteSetEntryPointMaxLevel(g, 0) }, rawOnly: true},
		{name: "link source", write: func(w *WALWriter) error { return w.WriteAddLinkAtLevel(g, 0, 1) }},
		{name: "link target", write: func(w *WALWriter) error { return w.WriteAddLinkAtLevel(9, 0, g) }},
		{name: "links source", write: func(w *WALWriter) error { return w.WriteAddLinksAtLevel(g, 0, []uint64{1, 2}) }},
		{name: "links target", write: func(w *WALWriter) error { return w.WriteAddLinksAtLevel(9, 0, []uint64{1, g}) }},
		{name: "replace links target", write: func(w *WALWriter) error { return w.WriteReplaceLinksAtLevel(9, 0, []uint64{g}) }},
		{name: "tombstone", write: func(w *WALWriter) error { return w.WriteAddTombstone(g) }},
		{name: "remove tombstone", write: func(w *WALWriter) error { return w.WriteRemoveTombstone(g) }},
		{name: "clear links", write: func(w *WALWriter) error { return w.WriteClearLinks(g) }},
		{name: "clear links at level", write: func(w *WALWriter) error { return w.WriteClearLinksAtLevel(g, 0) }},
		{name: "delete node", write: func(w *WALWriter) error { return w.WriteDeleteNode(g) }},
	}

	for _, fileType := range []FileType{FileTypeRaw, FileTypeSorted, FileTypeCondensed} {
		for _, rec := range records {
			if rec.rawOnly && fileType != FileTypeRaw {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s", fileType, rec.name), func(t *testing.T) {
				load := func(dir string) *LoadResult {
					t.Helper()
					res, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger(), NodeIDsAreDocIDs: true}).Load()
					require.NoError(t, err)
					require.NotNil(t, res)
					return res
				}

				cleanDir := nodeIDLimitTestDir(t)
				writeNodeIDLimitFixture(t, filepath.Join(cleanDir, nodeIDLimitTestName(fileType)), fileType)
				clean := load(cleanDir)
				cleanSize := fileSizeOf(t, filepath.Join(cleanDir, nodeIDLimitTestName(fileType)))

				dir := nodeIDLimitTestDir(t)
				path := filepath.Join(dir, nodeIDLimitTestName(fileType))
				writeNodeIDLimitFixture(t, path, fileType)
				appendToFile(t, path, walBytes(t, func(w *WALWriter) { require.NoError(t, rec.write(w)) }))

				first := load(dir)
				assert.True(t, first.RecoveredFromCrash, "a node ID beyond the limit must be detected as corruption")
				assert.Less(t, len(first.State.Graph.Nodes), g, "node index sized to the garbage ID")
				assertGraphEqual(t, clean.State, first.State)
				require.Equal(t, cleanSize, fileSizeOf(t, path), "file must be truncated before the record")

				second := load(dir)
				assert.False(t, second.RecoveredFromCrash, "second load must be clean after truncation")
				assertGraphEqual(t, clean.State, second.State)
			})
		}
	}
}

// TestCrashRecovery_NodeIDLimitControls pins the cases the limit must not
// change: an ID at the limit, and no limit at all, which keeps today's
// behavior for indexes whose node IDs have no known bound.
func TestCrashRecovery_NodeIDLimitControls(t *testing.T) {
	tests := []struct {
		name   string
		docIDs bool
		id     uint64
	}{
		{name: "ID at the limit", docIDs: true, id: nodeIDLimitTestMax},
		{name: "node IDs are not document IDs", docIDs: false, id: nodeIDLimitTestGarbage},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := nodeIDLimitTestDir(t)
			path := filepath.Join(dir, "1000")
			writeNodeIDLimitFixture(t, path, FileTypeRaw)
			appendToFile(t, path, walBytes(t, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(tc.id, 0)) }))
			size := fileSizeOf(t, path)

			res, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger(), NodeIDsAreDocIDs: tc.docIDs}).Load()
			require.NoError(t, err)
			assert.False(t, res.RecoveredFromCrash)
			assert.Equal(t, size, fileSizeOf(t, path), "valid file must not be truncated")
			require.NotNil(t, nodeAt(res.State, int(tc.id)), "node %d must be loaded", tc.id)
		})
	}
}

// TestCrashRecovery_NodeIDBeyondLimitDoesNotPresizeSnapshot pins the startup
// pre-scan: it sizes the snapshot's node slice to the highest ID in the raw
// logs before any record is applied, which is the crash-loop frame of #649.
func TestCrashRecovery_NodeIDBeyondLimitDoesNotPresizeSnapshot(t *testing.T) {
	dir := nodeIDLimitTestDir(t)
	createTestSnapshot(t, filepath.Join(dir, "1000.snapshot"), 0, 0, []testNode{
		{id: 0, level: 0, connections: [][]uint64{{5}}},
		{id: 5, level: 0, connections: [][]uint64{{0}}},
	})
	rawPath := filepath.Join(dir, "2000")
	createTestWALFile(t, rawPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(7, 0))
	})
	validSize := fileSizeOf(t, rawPath)
	appendToFile(t, rawPath, walBytes(t, func(w *WALWriter) {
		require.NoError(t, w.WriteAddLinkAtLevel(nodeIDLimitTestGarbage, 0, 0))
	}))

	res, err := NewLoader(LoaderConfig{Dir: dir, Logger: quietLogger(), NodeIDsAreDocIDs: true}).Load()
	require.NoError(t, err)
	assert.True(t, res.RecoveredFromCrash)
	assert.Less(t, len(res.State.Graph.Nodes), nodeIDLimitTestGarbage, "snapshot pre-sized to the garbage ID")
	require.NotNil(t, nodeAt(res.State, 7), "records before the corruption must survive")
	assert.Equal(t, validSize, fileSizeOf(t, rawPath), "raw log must be truncated before the record")
}
