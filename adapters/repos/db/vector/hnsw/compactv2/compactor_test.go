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

func TestCompactor_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle()
	require.NoError(t, err)
	assert.Equal(t, ActionNone, action)
}

func TestCompactor_OnlyLiveFile(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create only a live file (highest timestamp raw file)
	createEmptyFile(t, dir, "1000")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle()
	require.NoError(t, err)
	assert.Equal(t, ActionNone, action)

	// Live file should still exist
	_, err = os.Stat(filepath.Join(dir, "1000"))
	require.NoError(t, err)
}

func TestCompactor_CleanupTempFiles(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create orphaned temp files
	createEmptyFile(t, dir, "1000.sorted.tmp")
	createEmptyFile(t, dir, "2000.snapshot.tmp")

	// Create a live file
	createEmptyFile(t, dir, "3000")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle()
	require.NoError(t, err)

	// Temp files should be cleaned up
	_, err = os.Stat(filepath.Join(dir, "1000.sorted.tmp"))
	assert.True(t, os.IsNotExist(err), "temp file should be deleted")

	_, err = os.Stat(filepath.Join(dir, "2000.snapshot.tmp"))
	assert.True(t, os.IsNotExist(err), "temp file should be deleted")
}

func TestCompactor_DecideAction_NoSnapshot(t *testing.T) {
	logger := logrus.New()
	config := DefaultCompactorConfig("/tmp")
	compactor := NewCompactor(config, logger)

	tests := []struct {
		name     string
		state    *DirectoryState
		expected Action
	}{
		{
			name: "no sorted files",
			state: &DirectoryState{
				SortedFiles: []FileInfo{},
			},
			expected: ActionNone,
		},
		{
			name: "one sorted file, create snapshot",
			state: &DirectoryState{
				SortedFiles: []FileInfo{
					{StartTS: 1000, Size: 1000},
				},
			},
			expected: ActionCreateSnapshot,
		},
		{
			name: "many sorted files, merge first",
			state: &DirectoryState{
				SortedFiles: []FileInfo{
					{StartTS: 1000, Size: 100},
					{StartTS: 2000, Size: 100},
					{StartTS: 3000, Size: 100},
					{StartTS: 4000, Size: 100},
					{StartTS: 5000, Size: 100},
					{StartTS: 6000, Size: 100}, // > MaxFilesPerMerge
				},
			},
			expected: ActionMergeSorted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := compactor.decideAction(tt.state)
			assert.Equal(t, tt.expected, action)
		})
	}
}

func TestCompactor_DecideAction_WithSnapshot(t *testing.T) {
	logger := logrus.New()
	config := DefaultCompactorConfig("/tmp")
	config.SnapshotThreshold = 0.20
	compactor := NewCompactor(config, logger)

	tests := []struct {
		name     string
		state    *DirectoryState
		expected Action
	}{
		{
			name: "sorted ratio below threshold, merge",
			state: &DirectoryState{
				Snapshot: &FileInfo{Size: 1000},
				SortedFiles: []FileInfo{
					{StartTS: 2000, Size: 100}, // 10% < 20%
					{StartTS: 3000, Size: 50},
				},
			},
			expected: ActionMergeSorted,
		},
		{
			name: "sorted ratio above threshold, create snapshot",
			state: &DirectoryState{
				Snapshot: &FileInfo{Size: 100},
				SortedFiles: []FileInfo{
					{StartTS: 2000, Size: 100}, // 50% > 20%
				},
			},
			expected: ActionCreateSnapshot,
		},
		{
			name: "no sorted files",
			state: &DirectoryState{
				Snapshot:    &FileInfo{Size: 1000},
				SortedFiles: []FileInfo{},
			},
			expected: ActionNone,
		},
		{
			name: "one sorted file below threshold",
			state: &DirectoryState{
				Snapshot: &FileInfo{Size: 1000},
				SortedFiles: []FileInfo{
					{StartTS: 2000, Size: 100}, // 10% < 20%, only 1 file
				},
			},
			expected: ActionNone, // Can't merge just one file
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := compactor.decideAction(tt.state)
			assert.Equal(t, tt.expected, action)
		})
	}
}

func TestCompactor_ResolveOverlaps(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create overlapping files
	createEmptyFile(t, dir, "7_9.sorted")
	createEmptyFile(t, dir, "8.sorted")

	// Create live file
	createEmptyFile(t, dir, "10")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle()
	require.NoError(t, err)

	// Contained file should be deleted
	_, err = os.Stat(filepath.Join(dir, "8.sorted"))
	assert.True(t, os.IsNotExist(err), "contained file should be deleted")

	// Merged file should still exist
	_, err = os.Stat(filepath.Join(dir, "7_9.sorted"))
	require.NoError(t, err)
}

func TestCompactor_ConvertRawToSorted(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create a raw file with valid WAL content
	rawPath := filepath.Join(dir, "1000")
	createWALFile(t, rawPath)

	// Create a live file (higher timestamp)
	createEmptyFile(t, dir, "2000")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle()
	require.NoError(t, err)

	// Original raw file should be converted and deleted
	_, err = os.Stat(rawPath)
	assert.True(t, os.IsNotExist(err), "raw file should be deleted after conversion")

	// The compactor converts raw → sorted → snapshot in one cycle
	// So we expect a snapshot file to be created
	assert.Equal(t, ActionCreateSnapshot, action)
	_, err = os.Stat(filepath.Join(dir, "1000.snapshot"))
	require.NoError(t, err, "snapshot file should be created")
}

func TestAction_String(t *testing.T) {
	assert.Equal(t, "none", ActionNone.String())
	assert.Equal(t, "merge_sorted", ActionMergeSorted.String())
	assert.Equal(t, "create_snapshot", ActionCreateSnapshot.String())
	assert.Equal(t, "unknown", Action(99).String())
}

func TestDefaultCompactorConfig(t *testing.T) {
	config := DefaultCompactorConfig("/some/path")

	assert.Equal(t, "/some/path", config.Dir)
	assert.Equal(t, 5, config.MaxFilesPerMerge)
	assert.Equal(t, 0.20, config.SnapshotThreshold)
	assert.Equal(t, DefaultBufferSize, config.BufferSize)
}

func TestNewCompactor_AppliesDefaults(t *testing.T) {
	logger := logrus.New()

	// Test with zero values
	config := CompactorConfig{Dir: "/tmp"}
	compactor := NewCompactor(config, logger)

	assert.Equal(t, 5, compactor.config.MaxFilesPerMerge)
	assert.Equal(t, 0.20, compactor.config.SnapshotThreshold)
	assert.Equal(t, DefaultBufferSize, compactor.config.BufferSize)
}

// createEmptyFile creates an empty file with the given name.
func createEmptyFile(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	require.NoError(t, err)
	f.Close()
}

// createWALFile creates a minimal valid WAL file.
func createWALFile(t *testing.T, path string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	walWriter := NewWALWriter(f)

	// Write a simple node
	err = walWriter.WriteAddNode(0, 0)
	require.NoError(t, err)

	// Write a link
	err = walWriter.WriteReplaceLinksAtLevel(0, 0, []uint64{1, 2})
	require.NoError(t, err)

	// Write entrypoint
	err = walWriter.WriteSetEntryPointMaxLevel(0, 0)
	require.NoError(t, err)
}
