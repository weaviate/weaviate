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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileDiscovery_Scan_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)
	assert.Nil(t, state.Snapshot)
	assert.Empty(t, state.SortedFiles)
	assert.Empty(t, state.RawFiles)
	assert.Empty(t, state.CondensedFiles)
	assert.Nil(t, state.LiveFile)
	assert.Empty(t, state.Overlaps)
}

func TestFileDiscovery_Scan_NonExistentDirectory(t *testing.T) {
	discovery := NewFileDiscovery("/non/existent/path")
	state, err := discovery.Scan()

	require.NoError(t, err)
	assert.Nil(t, state.Snapshot)
	assert.Empty(t, state.SortedFiles)
}

func TestFileDiscovery_Scan_RawFiles(t *testing.T) {
	dir := t.TempDir()

	// Create raw files (oldest and newest)
	createFile(t, dir, "1000")
	createFile(t, dir, "2000")
	createFile(t, dir, "3000")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	// Highest timestamp raw file is the live file
	require.NotNil(t, state.LiveFile)
	assert.Equal(t, int64(3000), state.LiveFile.StartTS)

	// Other raw files need conversion
	assert.Len(t, state.RawFiles, 2)
	assert.Equal(t, int64(1000), state.RawFiles[0].StartTS)
	assert.Equal(t, int64(2000), state.RawFiles[1].StartTS)
}

func TestFileDiscovery_Scan_SortedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create sorted files
	createFile(t, dir, "1000.sorted")
	createFile(t, dir, "2000.sorted")
	createFile(t, dir, "3000.sorted")

	// Create a live file so sorted files aren't mistaken for it
	createFile(t, dir, "4000")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	assert.Len(t, state.SortedFiles, 3)
	assert.Equal(t, int64(1000), state.SortedFiles[0].StartTS)
	assert.Equal(t, int64(2000), state.SortedFiles[1].StartTS)
	assert.Equal(t, int64(3000), state.SortedFiles[2].StartTS)

	for _, f := range state.SortedFiles {
		assert.Equal(t, FileTypeSorted, f.Type)
	}
}

func TestFileDiscovery_Scan_CondensedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create condensed files
	createFile(t, dir, "1000.condensed")
	createFile(t, dir, "2000.condensed")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	assert.Len(t, state.CondensedFiles, 2)
	for _, f := range state.CondensedFiles {
		assert.Equal(t, FileTypeCondensed, f.Type)
	}
}

func TestFileDiscovery_Scan_SnapshotFile(t *testing.T) {
	dir := t.TempDir()

	// Create a snapshot file
	createFile(t, dir, "1000.snapshot")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	require.NotNil(t, state.Snapshot)
	assert.Equal(t, int64(1000), state.Snapshot.StartTS)
	assert.Equal(t, FileTypeSnapshot, state.Snapshot.Type)
}

func TestFileDiscovery_Scan_MergedRangeFiles(t *testing.T) {
	dir := t.TempDir()

	// Create merged range files
	createFile(t, dir, "1000_2000.sorted")
	createFile(t, dir, "3000_5000.snapshot")

	// Create a live file
	createFile(t, dir, "6000")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	// Check sorted merged file
	require.Len(t, state.SortedFiles, 1)
	assert.Equal(t, int64(1000), state.SortedFiles[0].StartTS)
	assert.Equal(t, int64(2000), state.SortedFiles[0].EndTS)
	assert.True(t, state.SortedFiles[0].IsMergedRange())

	// Check snapshot merged file
	require.NotNil(t, state.Snapshot)
	assert.Equal(t, int64(3000), state.Snapshot.StartTS)
	assert.Equal(t, int64(5000), state.Snapshot.EndTS)
}

func TestFileDiscovery_Scan_DetectsOverlaps(t *testing.T) {
	dir := t.TempDir()

	// Create a merged file and a file it contains
	createFile(t, dir, "7_9.sorted")
	createFile(t, dir, "8.sorted")

	// Create a live file
	createFile(t, dir, "10")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	// Should detect the overlap
	require.Len(t, state.Overlaps, 1)
	assert.Equal(t, int64(7), state.Overlaps[0].MergedFile.StartTS)
	assert.Equal(t, int64(9), state.Overlaps[0].MergedFile.EndTS)
	assert.Equal(t, int64(8), state.Overlaps[0].ContainedFile.StartTS)
}

func TestFileDiscovery_Scan_SkipsTempFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a temp file that should be skipped
	createFile(t, dir, "1000.sorted.tmp")
	createFile(t, dir, "2000.tmp")

	// Create a normal file
	createFile(t, dir, "3000.sorted")
	createFile(t, dir, "4000")

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	// Only the non-temp files should be found
	assert.Len(t, state.SortedFiles, 1)
	assert.Equal(t, int64(3000), state.SortedFiles[0].StartTS)

	require.NotNil(t, state.LiveFile)
	assert.Equal(t, int64(4000), state.LiveFile.StartTS)
}

func TestFileDiscovery_Scan_MixedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create a mix of all file types
	createFile(t, dir, "1000.snapshot")
	createFile(t, dir, "2000.condensed")
	createFile(t, dir, "3000.sorted")
	createFile(t, dir, "4000")        // raw, non-live
	createFile(t, dir, "5000")        // raw, live (highest)
	createFile(t, dir, "ignored.txt") // should be ignored

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)

	require.NotNil(t, state.Snapshot)
	assert.Equal(t, int64(1000), state.Snapshot.StartTS)

	assert.Len(t, state.CondensedFiles, 1)
	assert.Equal(t, int64(2000), state.CondensedFiles[0].StartTS)

	assert.Len(t, state.SortedFiles, 1)
	assert.Equal(t, int64(3000), state.SortedFiles[0].StartTS)

	assert.Len(t, state.RawFiles, 1)
	assert.Equal(t, int64(4000), state.RawFiles[0].StartTS)

	require.NotNil(t, state.LiveFile)
	assert.Equal(t, int64(5000), state.LiveFile.StartTS)
}

func TestFileDiscovery_Scan_FileSize(t *testing.T) {
	dir := t.TempDir()

	// Create a file with known content
	path := filepath.Join(dir, "1000.sorted")
	content := []byte("hello world")
	err := os.WriteFile(path, content, 0o644)
	require.NoError(t, err)

	discovery := NewFileDiscovery(dir)
	state, err := discovery.Scan()

	require.NoError(t, err)
	require.Len(t, state.SortedFiles, 1)
	assert.Equal(t, int64(len(content)), state.SortedFiles[0].Size)
}

func TestFileInfo_Contains(t *testing.T) {
	tests := []struct {
		name     string
		fi       FileInfo
		other    FileInfo
		expected bool
	}{
		{
			name:     "merged contains single",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 8, EndTS: 8},
			expected: true,
		},
		{
			name:     "merged contains start edge",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 7, EndTS: 7},
			expected: true,
		},
		{
			name:     "merged contains end edge",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 9, EndTS: 9},
			expected: true,
		},
		{
			name:     "merged does not contain outside",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 10, EndTS: 10},
			expected: false,
		},
		{
			name:     "merged does not contain partial overlap",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 8, EndTS: 11},
			expected: false,
		},
		{
			name:     "single does not contain single different",
			fi:       FileInfo{StartTS: 7, EndTS: 7},
			other:    FileInfo{StartTS: 8, EndTS: 8},
			expected: false,
		},
		{
			name:     "single contains itself",
			fi:       FileInfo{StartTS: 7, EndTS: 7},
			other:    FileInfo{StartTS: 7, EndTS: 7},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.fi.Contains(tt.other))
		})
	}
}

func TestFileInfo_Overlaps(t *testing.T) {
	tests := []struct {
		name     string
		fi       FileInfo
		other    FileInfo
		expected bool
	}{
		{
			name:     "overlapping ranges",
			fi:       FileInfo{StartTS: 7, EndTS: 10},
			other:    FileInfo{StartTS: 9, EndTS: 12},
			expected: true,
		},
		{
			name:     "non-overlapping ranges",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 10, EndTS: 12},
			expected: false,
		},
		{
			name:     "touching at edge",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 9, EndTS: 12},
			expected: true,
		},
		{
			name:     "one contains the other",
			fi:       FileInfo{StartTS: 5, EndTS: 15},
			other:    FileInfo{StartTS: 7, EndTS: 12},
			expected: true,
		},
		{
			name:     "identical ranges",
			fi:       FileInfo{StartTS: 7, EndTS: 9},
			other:    FileInfo{StartTS: 7, EndTS: 9},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.fi.Overlaps(tt.other))
		})
	}
}

func TestBuildMergedFilename(t *testing.T) {
	tests := []struct {
		name     string
		startTS  int64
		endTS    int64
		fileType FileType
		expected string
	}{
		{
			name:     "single sorted",
			startTS:  1000,
			endTS:    1000,
			fileType: FileTypeSorted,
			expected: "1000.sorted",
		},
		{
			name:     "merged sorted",
			startTS:  1000,
			endTS:    2000,
			fileType: FileTypeSorted,
			expected: "1000_2000.sorted",
		},
		{
			name:     "single snapshot",
			startTS:  1000,
			endTS:    1000,
			fileType: FileTypeSnapshot,
			expected: "1000.snapshot",
		},
		{
			name:     "merged snapshot",
			startTS:  1000,
			endTS:    3000,
			fileType: FileTypeSnapshot,
			expected: "1000_3000.snapshot",
		},
		{
			name:     "raw file",
			startTS:  1000,
			endTS:    1000,
			fileType: FileTypeRaw,
			expected: "1000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildMergedFilename(tt.startTS, tt.endTS, tt.fileType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDirectoryState_TotalSizes(t *testing.T) {
	state := &DirectoryState{
		Snapshot: &FileInfo{Size: 1000},
		SortedFiles: []FileInfo{
			{Size: 100},
			{Size: 200},
			{Size: 300},
		},
	}

	assert.Equal(t, int64(1000), state.TotalSnapshotSize())
	assert.Equal(t, int64(600), state.TotalSortedSize())

	// Test with no snapshot
	stateNoSnapshot := &DirectoryState{
		SortedFiles: []FileInfo{{Size: 500}},
	}
	assert.Equal(t, int64(0), stateNoSnapshot.TotalSnapshotSize())
	assert.Equal(t, int64(500), stateNoSnapshot.TotalSortedSize())
}

func TestFileType_String(t *testing.T) {
	assert.Equal(t, "raw", FileTypeRaw.String())
	assert.Equal(t, "condensed", FileTypeCondensed.String())
	assert.Equal(t, "sorted", FileTypeSorted.String())
	assert.Equal(t, "snapshot", FileTypeSnapshot.String())
}

func TestFileType_Suffix(t *testing.T) {
	assert.Equal(t, "", FileTypeRaw.Suffix())
	assert.Equal(t, ".condensed", FileTypeCondensed.Suffix())
	assert.Equal(t, ".sorted", FileTypeSorted.Suffix())
	assert.Equal(t, ".snapshot", FileTypeSnapshot.Suffix())
}

// createFile is a helper to create an empty file with the given name.
func createFile(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	require.NoError(t, err)
	f.Close()
}
