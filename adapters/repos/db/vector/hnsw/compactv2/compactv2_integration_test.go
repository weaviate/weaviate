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

package compactv2

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompactV2EndToEnd tests the complete compactv2 pipeline:
// 1. Write WAL commits to a raw file
// 2. Read raw file and convert to sorted format
// 3. Read multiple sorted files and merge using N-Way merger
// 4. Write merged result to a snapshot
// 5. Read snapshot back and verify all data matches
func TestCompactV2EndToEnd(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Create test data: simulate two separate commit logs
	// Log 1: Initial nodes and connections
	// Log 2: Updates, deletions, and tombstones

	// =========================================================================
	// Step 1: Write two raw WAL files
	// =========================================================================
	rawPath1 := filepath.Join(dir, "log1.raw")
	rawPath2 := filepath.Join(dir, "log2.raw")

	// Write first log: initial nodes
	writeRawLog(t, rawPath1, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 2))

		// Add nodes with various levels
		require.NoError(t, w.WriteAddNode(0, 2)) // entrypoint, level 2
		require.NoError(t, w.WriteAddNode(1, 1))
		require.NoError(t, w.WriteAddNode(2, 0))
		require.NoError(t, w.WriteAddNode(3, 1))
		require.NoError(t, w.WriteAddNode(4, 0))

		// Add connections at various levels
		// Node 0 (level 2): connects to 1,3 at level 0; 1 at level 1; nothing at level 2
		require.NoError(t, w.WriteReplaceLinksAtLevel(0, 0, []uint64{1, 3}))
		require.NoError(t, w.WriteReplaceLinksAtLevel(0, 1, []uint64{1}))

		// Node 1 (level 1): connects to 0,2,4 at level 0; 0 at level 1
		require.NoError(t, w.WriteReplaceLinksAtLevel(1, 0, []uint64{0, 2, 4}))
		require.NoError(t, w.WriteReplaceLinksAtLevel(1, 1, []uint64{0}))

		// Node 2 (level 0): connects to 1,4
		require.NoError(t, w.WriteReplaceLinksAtLevel(2, 0, []uint64{1, 4}))

		// Node 3 (level 1): connects to 0,4 at level 0; 0 at level 1
		require.NoError(t, w.WriteReplaceLinksAtLevel(3, 0, []uint64{0, 4}))
		require.NoError(t, w.WriteReplaceLinksAtLevel(3, 1, []uint64{0}))

		// Node 4 (level 0): connects to 1,2,3
		require.NoError(t, w.WriteReplaceLinksAtLevel(4, 0, []uint64{1, 2, 3}))
	})

	// Write second log: updates and deletions
	writeRawLog(t, rawPath2, func(w *WALWriter) {
		// Add more nodes
		require.NoError(t, w.WriteAddNode(5, 0))
		require.NoError(t, w.WriteAddNode(6, 1))

		// Update connections for node 1
		require.NoError(t, w.WriteReplaceLinksAtLevel(1, 0, []uint64{0, 2, 5, 6}))

		// Add connections for new nodes
		require.NoError(t, w.WriteReplaceLinksAtLevel(5, 0, []uint64{1, 2}))
		require.NoError(t, w.WriteReplaceLinksAtLevel(6, 0, []uint64{1, 3}))
		require.NoError(t, w.WriteReplaceLinksAtLevel(6, 1, []uint64{1, 3}))

		// Add tombstone for node 4 (soft delete)
		require.NoError(t, w.WriteAddTombstone(4))

		// Delete node 2 completely
		require.NoError(t, w.WriteDeleteNode(2))

		// Update entrypoint
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 2))
	})

	// =========================================================================
	// Step 2: Read raw files and convert to sorted format
	// =========================================================================
	sortedPath1 := filepath.Join(dir, "log1.sorted")
	sortedPath2 := filepath.Join(dir, "log2.sorted")

	convertRawToSorted(t, rawPath1, sortedPath1, logger)
	convertRawToSorted(t, rawPath2, sortedPath2, logger)

	// =========================================================================
	// Step 3: Read sorted files sequentially and verify against raw
	// =========================================================================
	// Read raw files sequentially
	rawResult := readFilesSequentially(t, []string{rawPath1, rawPath2}, logger)

	// Read sorted files sequentially
	sortedResult := readFilesSequentially(t, []string{sortedPath1, sortedPath2}, logger)

	// Verify they match
	assertResultsEqual(t, "raw vs sorted", rawResult, sortedResult)

	// =========================================================================
	// Step 4: Merge sorted files using N-Way merger
	// =========================================================================
	mergedSortedPath := filepath.Join(dir, "merged.sorted")
	mergeSortedFiles(t, []string{sortedPath1, sortedPath2}, mergedSortedPath, logger)

	// Read back merged result
	mergedResult := readFilesSequentially(t, []string{mergedSortedPath}, logger)

	// Verify merged result matches sequential read
	assertResultsEqual(t, "sequential vs merged", sortedResult, mergedResult)

	// =========================================================================
	// Step 5: Write snapshot from merged sorted files
	// =========================================================================
	snapshotPath := filepath.Join(dir, "final.snapshot")
	writeSnapshotFromSortedFiles(t, []string{sortedPath1, sortedPath2}, snapshotPath, logger)

	// =========================================================================
	// Step 6: Read snapshot back and verify
	// =========================================================================
	snapshotReader := NewSnapshotReader()
	snapshotResult, err := snapshotReader.ReadFromFile(snapshotPath)
	require.NoError(t, err)

	// Verify snapshot against expected final state
	assertSnapshotMatchesResult(t, sortedResult, snapshotResult)

	// =========================================================================
	// Step 7: Verify specific expected state
	// =========================================================================
	// Entrypoint should be node 0, level 2
	assert.Equal(t, uint64(0), snapshotResult.Entrypoint)
	assert.Equal(t, uint16(2), snapshotResult.Level)

	// Node 4 should have a tombstone
	_, hasTombstone := snapshotResult.Tombstones[4]
	assert.True(t, hasTombstone, "node 4 should have tombstone")

	// Node 2 should be nil (deleted)
	assert.Nil(t, snapshotResult.Nodes[2], "node 2 should be deleted")

	// Node 5 and 6 should exist (added in log2)
	assert.NotNil(t, snapshotResult.Nodes[5], "node 5 should exist")
	assert.NotNil(t, snapshotResult.Nodes[6], "node 6 should exist")

	// Verify node 1's updated connections (should have 0,2,5,6 from log2)
	node1 := snapshotResult.Nodes[1]
	require.NotNil(t, node1)
	if node1.Connections != nil {
		iter := node1.Connections.Iterator()
		if iter.Next() {
			level, links := iter.Current()
			assert.Equal(t, uint8(0), level)
			assert.ElementsMatch(t, []uint64{0, 2, 5, 6}, links, "node 1 level 0 connections")
		}
	}
}

// TestCompactV2WithSafeFileWriter tests that SafeFileWriter integrates correctly
// with all compactv2 writers (WAL, Sorted, Snapshot).
func TestCompactV2WithSafeFileWriter(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Test WAL writer with SafeFileWriter
	t.Run("WALWriter", func(t *testing.T) {
		walPath := filepath.Join(dir, "test.wal")
		sfw, err := NewSafeFileWriter(walPath, DefaultBufferSize)
		require.NoError(t, err)
		defer sfw.Abort()

		w := NewWALWriter(sfw.Writer())
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 1))
		require.NoError(t, w.WriteAddNode(0, 1))
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteReplaceLinksAtLevel(0, 0, []uint64{1}))

		require.NoError(t, sfw.Commit())

		// Verify file exists and is readable
		result := readFilesSequentially(t, []string{walPath}, logger)
		assert.Equal(t, uint64(0), result.Entrypoint)
		assert.Equal(t, 2, countNonNilNodes(result.Nodes))
	})

	// Test Sorted writer with SafeFileWriter
	t.Run("SortedWriter", func(t *testing.T) {
		sortedPath := filepath.Join(dir, "test.sorted")

		// Create test data
		testResult := &DeserializationResult{
			Entrypoint:        0,
			Level:             1,
			Nodes:             make([]*Vertex, 3),
			Tombstones:        make(map[uint64]struct{}),
			TombstonesDeleted: make(map[uint64]struct{}),
			NodesDeleted:      make(map[uint64]struct{}),
			LinksReplaced:     make(map[uint64]map[uint16]struct{}),
		}
		testResult.Nodes[0] = &Vertex{ID: 0, Level: 1}
		testResult.Nodes[1] = &Vertex{ID: 1, Level: 0}
		testResult.Tombstones[2] = struct{}{}

		sfw, err := NewSafeFileWriter(sortedPath, DefaultBufferSize)
		require.NoError(t, err)
		defer sfw.Abort()

		w := NewSortedWriter(sfw.Writer(), logger)
		require.NoError(t, w.WriteAll(testResult))
		require.NoError(t, sfw.Commit())

		// Verify file exists and is readable
		result := readFilesSequentially(t, []string{sortedPath}, logger)
		assert.Equal(t, uint64(0), result.Entrypoint)
		_, hasTombstone := result.Tombstones[2]
		assert.True(t, hasTombstone)
	})

	// Test Snapshot writer with SafeFileWriter
	t.Run("SnapshotWriter", func(t *testing.T) {
		snapshotPath := filepath.Join(dir, "test.snapshot")

		sfw, err := NewSafeFileWriter(snapshotPath, DefaultBufferSize)
		require.NoError(t, err)
		defer sfw.Abort()

		sw := NewSnapshotWriter(sfw.Writer())
		sw.SetEntrypoint(5, 2)
		sw.AddNode(0, 1, [][]uint64{{1, 2}}, false)
		sw.AddNode(1, 0, [][]uint64{{0}}, true) // has tombstone
		sw.AddNode(5, 2, [][]uint64{{0}, {1}, {}}, false)

		require.NoError(t, sw.Flush())
		require.NoError(t, sfw.Commit())

		// Verify file exists and is readable
		reader := NewSnapshotReader()
		result, err := reader.ReadFromFile(snapshotPath)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), result.Entrypoint)
		assert.Equal(t, uint16(2), result.Level)
		_, hasTombstone := result.Tombstones[1]
		assert.True(t, hasTombstone)
	})
}

// TestCleanupOrphanedTempFilesIntegration tests cleanup of orphaned temp files.
func TestCleanupOrphanedTempFilesIntegration(t *testing.T) {
	dir := t.TempDir()

	// Create some regular files
	require.NoError(t, os.WriteFile(filepath.Join(dir, "valid.sorted"), []byte("data"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "valid.snapshot"), []byte("data"), 0o666))

	// Create orphaned temp files (as if a previous crash happened)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "orphan1.sorted.tmp"), []byte("orphan"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "orphan2.snapshot.tmp"), []byte("orphan"), 0o666))

	// Run cleanup
	require.NoError(t, CleanupOrphanedTempFiles(dir))

	// Verify regular files still exist
	_, err := os.Stat(filepath.Join(dir, "valid.sorted"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "valid.snapshot"))
	require.NoError(t, err)

	// Verify orphaned files were removed
	_, err = os.Stat(filepath.Join(dir, "orphan1.sorted.tmp"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "orphan2.snapshot.tmp"))
	assert.True(t, os.IsNotExist(err))
}

// Helper functions

func countNonNilNodes(nodes []*Vertex) int {
	count := 0
	for _, n := range nodes {
		if n != nil {
			count++
		}
	}
	return count
}

func writeRawLog(t *testing.T, path string, writeFn func(*WALWriter)) {
	t.Helper()

	sfw, err := NewSafeFileWriter(path, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	w := NewWALWriter(sfw.Writer())
	writeFn(w)

	require.NoError(t, sfw.Commit())
}

func convertRawToSorted(t *testing.T, rawPath, sortedPath string, logger *logrus.Logger) {
	t.Helper()

	// Read raw file
	result := readFilesSequentially(t, []string{rawPath}, logger)

	// Write sorted file
	sfw, err := NewSafeFileWriter(sortedPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	w := NewSortedWriter(sfw.Writer(), logger)
	require.NoError(t, w.WriteAll(result))
	require.NoError(t, sfw.Commit())
}

func readFilesSequentially(t *testing.T, paths []string, logger *logrus.Logger) *DeserializationResult {
	t.Helper()

	var result *DeserializationResult
	for _, path := range paths {
		f, err := os.Open(path)
		require.NoError(t, err)

		reader := bufio.NewReaderSize(f, 256*1024)
		walReader := NewWALCommitReader(reader, logger)
		memReader := NewInMemoryReader(walReader, logger)
		result, err = memReader.Do(result, true)
		f.Close()
		require.NoError(t, err)
	}
	return result
}

func mergeSortedFiles(t *testing.T, sortedPaths []string, outputPath string, logger *logrus.Logger) {
	t.Helper()

	// Create iterators
	iterators := make([]IteratorLike, len(sortedPaths))
	files := make([]*os.File, len(sortedPaths))

	for i, path := range sortedPaths {
		f, err := os.Open(path)
		require.NoError(t, err)
		files[i] = f

		reader := bufio.NewReaderSize(f, 256*1024)
		walReader := NewWALCommitReader(reader, logger)
		iterator, err := NewIterator(walReader, i, logger)
		require.NoError(t, err)
		iterators[i] = iterator
	}

	// Create merger
	merger, err := NewNWayMerger(iterators, logger)
	require.NoError(t, err)

	// Write merged output
	sfw, err := NewSafeFileWriter(outputPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	w := NewWALWriter(sfw.Writer())

	// Write global commits
	for _, commit := range merger.GlobalCommits() {
		require.NoError(t, writeCommitToWAL(w, commit))
	}

	// Write node commits
	for {
		nodeCommits, err := merger.Next()
		require.NoError(t, err)
		if nodeCommits == nil {
			break
		}
		for _, commit := range nodeCommits.Commits {
			require.NoError(t, writeCommitToWAL(w, commit))
		}
	}

	require.NoError(t, sfw.Commit())

	// Close files
	for _, f := range files {
		f.Close()
	}
}

func writeSnapshotFromSortedFiles(t *testing.T, sortedPaths []string, snapshotPath string, logger *logrus.Logger) {
	t.Helper()

	// Create iterators
	iterators := make([]IteratorLike, len(sortedPaths))
	files := make([]*os.File, len(sortedPaths))

	for i, path := range sortedPaths {
		f, err := os.Open(path)
		require.NoError(t, err)
		files[i] = f

		reader := bufio.NewReaderSize(f, 256*1024)
		walReader := NewWALCommitReader(reader, logger)
		iterator, err := NewIterator(walReader, i, logger)
		require.NoError(t, err)
		iterators[i] = iterator
	}

	// Create merger
	merger, err := NewNWayMerger(iterators, logger)
	require.NoError(t, err)

	// Write snapshot
	sfw, err := NewSafeFileWriter(snapshotPath, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	sw := NewSnapshotWriter(sfw.Writer())
	require.NoError(t, sw.WriteFromMerger(merger))
	require.NoError(t, sfw.Commit())

	// Close files
	for _, f := range files {
		f.Close()
	}
}

func writeCommitToWAL(w *WALWriter, c Commit) error {
	switch commit := c.(type) {
	case *AddNodeCommit:
		return w.WriteAddNode(commit.ID, commit.Level)
	case *SetEntryPointMaxLevelCommit:
		return w.WriteSetEntryPointMaxLevel(commit.Entrypoint, commit.Level)
	case *AddLinkAtLevelCommit:
		return w.WriteAddLinkAtLevel(commit.Source, commit.Level, commit.Target)
	case *AddLinksAtLevelCommit:
		return w.WriteAddLinksAtLevel(commit.Source, commit.Level, commit.Targets)
	case *ReplaceLinksAtLevelCommit:
		return w.WriteReplaceLinksAtLevel(commit.Source, commit.Level, commit.Targets)
	case *ClearLinksCommit:
		return w.WriteClearLinks(commit.ID)
	case *ClearLinksAtLevelCommit:
		return w.WriteClearLinksAtLevel(commit.ID, commit.Level)
	case *AddTombstoneCommit:
		return w.WriteAddTombstone(commit.ID)
	case *RemoveTombstoneCommit:
		return w.WriteRemoveTombstone(commit.ID)
	case *DeleteNodeCommit:
		return w.WriteDeleteNode(commit.ID)
	case *ResetIndexCommit:
		return w.WriteResetIndex()
	case *AddPQCommit:
		return w.WriteAddPQ(commit.Data)
	case *AddSQCommit:
		return w.WriteAddSQ(commit.Data)
	case *AddRQCommit:
		return w.WriteAddRQ(commit.Data)
	case *AddBRQCommit:
		return w.WriteAddBRQ(commit.Data)
	case *AddMuveraCommit:
		return w.WriteAddMuvera(commit.Data)
	default:
		return nil
	}
}

func assertResultsEqual(t *testing.T, label string, a, b *DeserializationResult) {
	t.Helper()

	assert.Equal(t, a.Entrypoint, b.Entrypoint, "%s: entrypoint mismatch", label)
	assert.Equal(t, a.Level, b.Level, "%s: level mismatch", label)
	assert.Equal(t, len(a.Tombstones), len(b.Tombstones), "%s: tombstone count mismatch", label)
	assert.Equal(t, len(a.TombstonesDeleted), len(b.TombstonesDeleted), "%s: tombstones deleted count mismatch", label)
	assert.Equal(t, len(a.NodesDeleted), len(b.NodesDeleted), "%s: nodes deleted count mismatch", label)

	// Compare nodes
	maxLen := len(a.Nodes)
	if len(b.Nodes) > maxLen {
		maxLen = len(b.Nodes)
	}

	for i := 0; i < maxLen; i++ {
		var nodeA, nodeB *Vertex
		if i < len(a.Nodes) {
			nodeA = a.Nodes[i]
		}
		if i < len(b.Nodes) {
			nodeB = b.Nodes[i]
		}

		if nodeA == nil && nodeB == nil {
			continue
		}

		if (nodeA == nil) != (nodeB == nil) {
			t.Errorf("%s: node %d nil mismatch (a=%v, b=%v)", label, i, nodeA == nil, nodeB == nil)
			continue
		}

		assert.Equal(t, nodeA.ID, nodeB.ID, "%s: node %d ID mismatch", label, i)
		assert.Equal(t, nodeA.Level, nodeB.Level, "%s: node %d level mismatch", label, i)

		// Compare connections
		assertConnectionsEqual(t, label, i, nodeA, nodeB)
	}
}

func assertConnectionsEqual(t *testing.T, label string, nodeID int, a, b *Vertex) {
	t.Helper()

	if a.Connections == nil && b.Connections == nil {
		return
	}

	if (a.Connections == nil) != (b.Connections == nil) {
		t.Errorf("%s: node %d connections nil mismatch", label, nodeID)
		return
	}

	iterA := a.Connections.Iterator()
	iterB := b.Connections.Iterator()

	for iterA.Next() && iterB.Next() {
		levelA, linksA := iterA.Current()
		levelB, linksB := iterB.Current()

		assert.Equal(t, levelA, levelB, "%s: node %d level mismatch in connections", label, nodeID)
		assert.ElementsMatch(t, linksA, linksB, "%s: node %d level %d links mismatch", label, nodeID, levelA)
	}
}

func assertSnapshotMatchesResult(t *testing.T, expected, snapshot *DeserializationResult) {
	t.Helper()

	assert.Equal(t, expected.Entrypoint, snapshot.Entrypoint, "entrypoint mismatch")
	assert.Equal(t, expected.Level, snapshot.Level, "level mismatch")

	// Compute expected active tombstones (excluding deleted ones)
	expectedTombstones := make(map[uint64]struct{})
	for id := range expected.Tombstones {
		if _, deleted := expected.TombstonesDeleted[id]; !deleted {
			expectedTombstones[id] = struct{}{}
		}
	}
	assert.Equal(t, len(expectedTombstones), len(snapshot.Tombstones), "active tombstone count mismatch")

	// Compare nodes (accounting for deleted nodes)
	maxLen := len(expected.Nodes)
	if len(snapshot.Nodes) > maxLen {
		maxLen = len(snapshot.Nodes)
	}

	for i := 0; i < maxLen; i++ {
		var expectedNode, snapshotNode *Vertex

		if i < len(expected.Nodes) {
			expectedNode = expected.Nodes[i]
		}
		if i < len(snapshot.Nodes) {
			snapshotNode = snapshot.Nodes[i]
		}

		// If node is deleted in expected, it should be nil in snapshot
		if expectedNode != nil {
			if _, deleted := expected.NodesDeleted[expectedNode.ID]; deleted {
				expectedNode = nil
			}
		}

		if expectedNode == nil && snapshotNode == nil {
			continue
		}

		if (expectedNode == nil) != (snapshotNode == nil) {
			t.Errorf("snapshot node %d nil mismatch (expected=%v, snapshot=%v)", i, expectedNode == nil, snapshotNode == nil)
			continue
		}

		assert.Equal(t, expectedNode.ID, snapshotNode.ID, "snapshot node %d ID mismatch", i)
		assert.Equal(t, expectedNode.Level, snapshotNode.Level, "snapshot node %d level mismatch", i)
	}
}

// TestCompactV2RoundTrip tests that data survives a complete round-trip through
// all compactv2 formats: WAL -> Sorted -> Merged -> Snapshot -> Read.
func TestCompactV2RoundTrip(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Create test data in memory
	var buf bytes.Buffer
	w := NewWALWriter(&buf)

	// Write some test commits
	require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 1))
	require.NoError(t, w.WriteAddNode(0, 1))
	require.NoError(t, w.WriteAddNode(1, 0))
	require.NoError(t, w.WriteAddNode(2, 0))
	require.NoError(t, w.WriteReplaceLinksAtLevel(0, 0, []uint64{1, 2}))
	require.NoError(t, w.WriteReplaceLinksAtLevel(0, 1, []uint64{}))
	require.NoError(t, w.WriteReplaceLinksAtLevel(1, 0, []uint64{0, 2}))
	require.NoError(t, w.WriteReplaceLinksAtLevel(2, 0, []uint64{0, 1}))
	require.NoError(t, w.WriteAddTombstone(2))

	// Read back via WAL reader
	reader := bufio.NewReader(&buf)
	walReader := NewWALCommitReader(reader, logger)
	memReader := NewInMemoryReader(walReader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	// Verify initial read
	assert.Equal(t, uint64(0), result.Entrypoint)
	assert.Equal(t, uint16(1), result.Level)
	assert.Equal(t, 3, countNonNilNodes(result.Nodes))
	_, hasTombstone := result.Tombstones[2]
	assert.True(t, hasTombstone)

	// Write to sorted format
	var sortedBuf bytes.Buffer
	sortedWriter := NewSortedWriter(&sortedBuf, logger)
	require.NoError(t, sortedWriter.WriteAll(result))

	// Read from sorted format
	sortedReader := bufio.NewReader(&sortedBuf)
	sortedWalReader := NewWALCommitReader(sortedReader, logger)
	sortedMemReader := NewInMemoryReader(sortedWalReader, logger)
	sortedResult, err := sortedMemReader.Do(nil, true)
	require.NoError(t, err)

	// Verify sorted read matches original
	assertResultsEqual(t, "original vs sorted", result, sortedResult)

	// Write to snapshot format (using a temp file since SnapshotReader needs io.ReadSeeker)
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "roundtrip.snapshot")

	sfw, err := NewSafeFileWriter(snapshotPath, DefaultBufferSize)
	require.NoError(t, err)

	snapshotWriter := NewSnapshotWriter(sfw.Writer())
	snapshotWriter.SetEntrypoint(result.Entrypoint, result.Level)

	for i, node := range result.Nodes {
		if node == nil {
			continue
		}

		var connections [][]uint64
		if node.Connections != nil {
			iter := node.Connections.Iterator()
			for iter.Next() {
				_, links := iter.Current()
				connections = append(connections, links)
			}
		}

		_, hasTombstone := result.Tombstones[uint64(i)]
		snapshotWriter.AddNode(uint64(i), uint16(node.Level), connections, hasTombstone)
	}

	require.NoError(t, snapshotWriter.Flush())
	require.NoError(t, sfw.Commit())

	// Read from snapshot format
	snapshotReaderObj := NewSnapshotReader()
	snapshotResult, err := snapshotReaderObj.ReadFromFile(snapshotPath)
	require.NoError(t, err)

	// Verify snapshot matches
	assert.Equal(t, result.Entrypoint, snapshotResult.Entrypoint)
	assert.Equal(t, result.Level, snapshotResult.Level)
	_, snapshotHasTombstone := snapshotResult.Tombstones[2]
	assert.True(t, snapshotHasTombstone)
}
