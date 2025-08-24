//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func createTestCommitLoggerForSnapshots(t *testing.T, rootDir, id string) *hnswCommitLogger {
	opts := []CommitlogOption{
		WithCommitlogThreshold(1000),
		WithCommitlogThresholdForCombining(200),
		WithCondensor(&fakeCondensor{}),
		WithSnapshotDisabled(false),
		WithAllocChecker(fakeAllocChecker{}),
	}

	commitLogDir := commitLogDirectory(rootDir, id)
	cl, err := NewCommitLogger(rootDir, id, logrus.New(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)

	// commit logger always creates an empty file if there is no data, remove it first
	files, err := os.ReadDir(commitLogDir)
	require.NoError(t, err)
	for _, file := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", commitLogDir, file.Name()))
		require.NoError(t, err)
	}

	return cl
}

func createCommitlogTestData(t *testing.T, dir string, filenameSizes ...any) {
	// create the files with the specified sizes
	for i := 0; i < len(filenameSizes); i += 2 {
		filename := fmt.Sprintf("%s/%s", dir, filenameSizes[i])
		size := filenameSizes[i+1].(int)
		cl := commitlog.NewLogger(filename)

		generateFakeCommitLogData(t, cl, int64(size))

		err := cl.Close()
		require.NoError(t, err)
	}
}

func generateFakeCommitLogData(t *testing.T, cl *commitlog.Logger, size int64) {
	var err error

	i := 0
	for {
		if i > 0 && i%5 == 0 {
			err = cl.DeleteNode(uint64(i - 1))
		} else {
			err = cl.AddNode(uint64(i), levelForDummyVertex(i))
		}
		require.NoError(t, err)

		err = cl.Flush()
		require.NoError(t, err)

		fsize, err := cl.FileSize()
		require.NoError(t, err)

		if fsize >= size {
			break
		}

		i++
	}
}

func createCommitlogAndSnapshotTestData(t *testing.T, cl *hnswCommitLogger, commitlogNameSizes ...any) {
	require.GreaterOrEqual(t, len(commitlogNameSizes), 4, "at least 2 commitlog files are required to create snapshot")

	clDir := commitLogDirectory(cl.rootPath, cl.id)
	createCommitlogTestData(t, clDir, commitlogNameSizes...)

	created, _, err := cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
}

func readDir(t *testing.T, dir string) []string {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)

	var result []string
	for _, item := range files {
		if item.IsDir() {
			continue
		}
		result = append(result, item.Name())
	}
	return result
}

func TestCreateSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		setup    []any
		expected []string
		created  bool
	}{
		{
			name:  "empty directory",
			setup: []any{},
		},
		{
			name:  "single file",
			setup: []any{"1000", 1000},
		},
		{
			name:     "many non-condensed files",
			setup:    []any{"1000", 1000, "1001", 1000, "1002", 1000, "1003", 1000},
			expected: []string{"1002.snapshot", "1002.snapshot.checkpoints"},
			created:  true,
		},
		{
			name:     "small condensed files",
			setup:    []any{"1000.condensed", 100, "1001.condensed", 100, "1002.condensed", 100, "1003.condensed", 100},
			expected: []string{"1002.snapshot", "1002.snapshot.checkpoints"},
			created:  true,
		},
		{
			name:     "bigger condensed files",
			setup:    []any{"1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200},
			expected: []string{"1002.snapshot", "1002.snapshot.checkpoints"},
			created:  true,
		},
		{
			name:  "not enough condensed files",
			setup: []any{"1000.condensed", 1000},
		},
		{
			name:     "enough condensed files",
			setup:    []any{"1000.condensed", 1000, "1001.condensed", 1000},
			expected: []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			created:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			id := "main"
			cl := createTestCommitLoggerForSnapshots(t, dir, id)
			createCommitlogTestData(t, commitLogDirectory(dir, id), test.setup...)

			created, _, err := cl.CreateSnapshot()
			require.NoError(t, err)
			require.Equal(t, test.created, created)
			require.Equal(t, test.expected, readDir(t, snapshotDirectory(dir, id)))
		})
	}
}

func TestCreateSnapshotWithExistingState(t *testing.T) {
	dir := t.TempDir()
	id := "main"
	clDir := commitLogDirectory(dir, id)
	sDir := snapshotDirectory(dir, id)

	cl := createTestCommitLoggerForSnapshots(t, dir, id)
	createCommitlogTestData(t, clDir, "1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200)

	// create snapshot
	created, _, err := cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)

	files := readDir(t, sDir)
	require.Equal(t, []string{"1002.snapshot", "1002.snapshot.checkpoints"}, files)

	// add new files
	createCommitlogTestData(t, clDir, "1004", 1000, "1005", 5)

	// create snapshot, should create it
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1004.snapshot", "1004.snapshot.checkpoints"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1004"), filepath.Join(clDir, "1004.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it (no new commitlogs)
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.False(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1004.snapshot", "1004.snapshot.checkpoints"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1005"), filepath.Join(clDir, "1005.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it (no new commitlogs)
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.False(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1004.snapshot", "1004.snapshot.checkpoints"}, files)

	// add new files
	createCommitlogTestData(t, clDir, "1006", 5)

	// create snapshot, should create it
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1005.snapshot", "1005.snapshot.checkpoints"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1006"), filepath.Join(clDir, "1006.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it (no new files)
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.False(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1005.snapshot", "1005.snapshot.checkpoints"}, files)

	// add new files
	createCommitlogTestData(t, clDir, "1007", 5)

	// create snapshot, should create it
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1006.snapshot", "1006.snapshot.checkpoints"}, files)
}

func TestCreateSnapshotCrashRecovery(t *testing.T) {
	t.Run("crash before renaming from .tmp to .snapshot", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)
		os.MkdirAll(sDir, os.ModePerm)

		createCommitlogTestData(t, clDir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// simulate shutdown before snapshot renaming
		createCommitlogTestData(t, sDir, "1000.snapshot.tmp", 1000)

		// create snapshot
		created, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		files := readDir(t, sDir)
		require.Equal(t, []string{"1001.snapshot", "1001.snapshot.checkpoints"}, files)
	})

	t.Run("missing checkpoints", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)

		createCommitlogTestData(t, clDir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000,
			"1003", 1000)

		// missing checkpoints
		createCommitlogTestData(t, sDir, "1000.snapshot", 1000)

		// create snapshot should still work
		created, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		files := readDir(t, sDir)
		require.Equal(t, []string{"1002.snapshot", "1002.snapshot.checkpoints"}, files)
	})

	t.Run("corrupt snapshot", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)

		createCommitlogTestData(t, clDir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// create snapshot
		created, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.NotNil(t, created)
		files := readDir(t, sDir)
		require.Equal(t, []string{"1001.snapshot", "1001.snapshot.checkpoints"}, files)

		// corrupt the snapshot
		err = os.WriteFile(filepath.Join(sDir, "1001.snapshot"), []byte("corrupt"), 0o644)
		require.NoError(t, err)

		// add new files
		createCommitlogTestData(t, clDir, "1003.condensed", 1000, "1004.condensed", 1000, "1005.condensed", 1000)

		// create snapshot should still work
		created, _, err = cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		files = readDir(t, sDir)
		require.Equal(t, []string{"1004.snapshot", "1004.snapshot.checkpoints"}, files)
	})
}

func TestCreateAndLoadSnapshot(t *testing.T) {
	t.Run("create and load snapshot", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)

		createCommitlogTestData(t, clDir, "1000.condensed", 1000)

		// try to create a snapshot, should not create it
		// because there is not enough data
		state, createdAt, err := cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.Nil(t, state)
		require.Zero(t, createdAt)
		files := readDir(t, sDir)
		require.Empty(t, files)

		// add new files
		createCommitlogTestData(t, clDir, "1001.condensed", 1000)

		// create new snapshot
		state, createdAt, err = cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.ElementsMatch(t, []string{"1000.snapshot", "1000.snapshot.checkpoints"}, files)

		// add new files
		createCommitlogTestData(t, clDir, "1002.condensed", 1000)

		// create new snapshot
		state, createdAt, err = cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.ElementsMatch(t, []string{"1001.snapshot", "1001.snapshot.checkpoints"}, files)

		// try again, should not create a new snapshot
		// but should return the existing one
		state, createdAt, err = cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.ElementsMatch(t, []string{"1001.snapshot", "1001.snapshot.checkpoints"}, files)
	})

	t.Run("empty snapshot", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)

		createCommitlogTestData(t, clDir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// create snapshot
		created, createdAt, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		require.NotZero(t, createdAt)
		files := readDir(t, sDir)
		require.Equal(t, []string{"1001.snapshot", "1001.snapshot.checkpoints"}, files)

		// empty the snapshot
		err = os.WriteFile(filepath.Join(sDir, "1001.snapshot"), []byte(""), 0o644)
		require.NoError(t, err)

		// create snapshot again
		state, createdAt, err := cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.Equal(t, []string{"1001.snapshot", "1001.snapshot.checkpoints"}, files)
		// snapshot has content now
		info, err := os.Stat(filepath.Join(sDir, "1001.snapshot"))
		require.NoError(t, err)
		require.Less(t, int64(0), info.Size())
	})
}

func TestCreateSnapshot_NextOne(t *testing.T) {
	s2008 := 1200 // commitlog of size 1200 makes snapshot of size 2008

	tests := []struct {
		name                string
		setup               []any
		delta               []any
		deltaNumber         int
		deltaSizePercentage int
		allocCheckerOOM     bool
		expectedFiles       []string
		expectedCreated     bool
	}{
		// number of delta files
		{
			name:            "no new commitlogs (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{},
			deltaNumber:     1,
			expectedFiles:   []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated: false,
		},
		{
			name:            "1 new commitlog (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000, "1003", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1002.snapshot", "1002.snapshot.checkpoints"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated: false,
		},
		{
			name:            "3 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1003.snapshot", "1003.snapshot.checkpoints"},
			expectedCreated: true,
		},
		{
			name:            "4 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004.condensed", 1000, "1005", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1004.snapshot", "1004.snapshot.checkpoints"},
			expectedCreated: true,
		},

		// size % of delta files
		{
			name:                "too small delta size (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 90},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 30},
			delta:               []any{"1002.condensed", 30, "1003.condensed", 30, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 110},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 35},
			delta:               []any{"1002.condensed", 35, "1003.condensed", 35, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1003.snapshot", "1003.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "too small delta size (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 2490},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 820},
			delta:               []any{"1002.condensed", 820, "1003.condensed", 820, "1004", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 2510},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 830},
			delta:               []any{"1002.condensed", 830, "1003.condensed", 830, "1004", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1003.snapshot", "1003.snapshot.checkpoints"},
			expectedCreated:     true,
		},

		// number + size % of delta files + allocChecker
		// NOTE: data in commitlogs is duplicated, so final snapshot size made out of multiple commitlogs
		// will effectively be the same as size of snaptshot created just from biggest commitlog
		{
			name:                "too few delta commitlogs, too small delta size",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too few delta commitlogs",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 50,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "enough delta commit logs, enough delta size",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 50,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "enough delta commit logs, enough delta size, but oom",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 50,
			allocCheckerOOM:     true,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			id := "main"
			cl := createTestCommitLoggerForSnapshots(t, dir, id)
			createCommitlogAndSnapshotTestData(t, cl, test.setup...)

			if len(test.delta) > 0 {
				createCommitlogTestData(t, commitLogDirectory(dir, id), test.delta...)
			}

			// overwrite settings for next snapshot creation
			if test.allocCheckerOOM {
				cl.allocChecker = &fakeAllocChecker{shouldErr: true}
			}
			cl.snapshotMinDeltaCommitlogsNumber = test.deltaNumber
			cl.snapshotMinDeltaCommitlogsSizePercentage = test.deltaSizePercentage

			created, _, err := cl.CreateSnapshot()
			require.NoError(t, err)
			require.Equal(t, test.expectedCreated, created)
			require.Equal(t, test.expectedFiles, readDir(t, snapshotDirectory(dir, id)))
		})
	}
}

func TestCreateAndLoadSnapshot_NextOne(t *testing.T) {
	s2008 := 1200 // commitlog of size 1200 makes snapshot of size 2008

	tests := []struct {
		name                string
		setup               []any
		delta               []any
		deltaNumber         int
		deltaSizePercentage int
		allocCheckerOOM     bool
		expectedFiles       []string
		expectedCreated     bool
	}{
		// number of delta files
		{
			name:            "no new commitlogs (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{},
			deltaNumber:     1,
			expectedFiles:   []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated: false,
		},
		{
			name:            "1 new commitlog (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000, "1003", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1002.snapshot", "1002.snapshot.checkpoints"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated: false,
		},
		{
			name:            "3 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1003.snapshot", "1003.snapshot.checkpoints"},
			expectedCreated: true,
		},
		{
			name:            "4 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004.condensed", 1000, "1005", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1004.snapshot", "1004.snapshot.checkpoints"},
			expectedCreated: true,
		},

		// size % of delta files
		{
			name:                "too small delta size (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 90},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 30},
			delta:               []any{"1002.condensed", 30, "1003.condensed", 30, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 110},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s2008, "1001", 35},
			delta:               []any{"1002.condensed", 35, "1003.condensed", 35, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1003.snapshot", "1003.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "too small delta size (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 2490},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 820},
			delta:               []any{"1002.condensed", 820, "1003.condensed", 820, "1004", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 2510},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 125%)",
			setup:               []any{"1000.condensed", s2008, "1001", 830},
			delta:               []any{"1002.condensed", 830, "1003.condensed", 830, "1004", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1003.snapshot", "1003.snapshot.checkpoints"},
			expectedCreated:     true,
		},

		// number + size % of delta files + allocChecker
		// NOTE: data in commitlogs is duplicated, so final snapshot size made out of multiple commitlogs
		// will effectively be the same as size of snaptshot created just from biggest commitlog
		{
			name:                "too few delta commitlogs, too small delta size",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "too few delta commitlogs",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 50,
			expectedFiles:       []string{"1000.snapshot", "1000.snapshot.checkpoints"},
			expectedCreated:     false,
		},
		{
			name:                "enough delta commit logs, enough delta size",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 50,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
		{
			name:                "enough delta commit logs, enough delta size, oom is ignored",
			setup:               []any{"1000.condensed", s2008, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 50,
			allocCheckerOOM:     true,
			expectedFiles:       []string{"1001.snapshot", "1001.snapshot.checkpoints"},
			expectedCreated:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			id := "main"
			cl := createTestCommitLoggerForSnapshots(t, dir, id)
			createCommitlogAndSnapshotTestData(t, cl, test.setup...)

			if len(test.delta) > 0 {
				createCommitlogTestData(t, commitLogDirectory(dir, id), test.delta...)
			}

			// overwrite settings for next snapshot creation
			if test.allocCheckerOOM {
				cl.allocChecker = &fakeAllocChecker{shouldErr: true}
			}
			cl.snapshotMinDeltaCommitlogsNumber = test.deltaNumber
			cl.snapshotMinDeltaCommitlogsSizePercentage = test.deltaSizePercentage

			state, createdAt, err := cl.CreateAndLoadSnapshot()
			require.NoError(t, err)
			require.NotNil(t, state)
			require.Equal(t, test.expectedFiles, readDir(t, snapshotDirectory(dir, id)))
			// All examples have snapshot 1000 to start with.
			// If new one is created it would be named after newer commitlogs
			if test.expectedCreated {
				require.NotEqual(t, int64(1000), createdAt)
			} else {
				require.Equal(t, int64(1000), createdAt)
			}
		})
	}
}
