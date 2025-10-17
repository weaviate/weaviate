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

package hnsw

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/commitlog"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func createTestCommitLoggerForSnapshotsWithOpts(t *testing.T, rootDir, id string, opts ...CommitlogOption) *hnswCommitLogger {
	options := []CommitlogOption{
		WithCommitlogThreshold(1000),
		WithCommitlogThresholdForCombining(200),
		WithCondensor(&fakeCondensor{}),
		WithSnapshotDisabled(false),
	}
	options = append(options, opts...)

	commitLogDir := commitLogDirectory(rootDir, id)
	cl, err := NewCommitLogger(rootDir, id, logrus.New(), cyclemanager.NewCallbackGroupNoop(), options...)
	require.NoError(t, err)
	cl.snapshotBlockSize = 4096

	// commit logger always creates an empty file if there is no data, remove it first
	files, err := os.ReadDir(commitLogDir)
	require.NoError(t, err)
	for _, file := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", commitLogDir, file.Name()))
		require.NoError(t, err)
	}

	return cl
}

func createTestCommitLoggerForSnapshots(t *testing.T, rootDir, id string) *hnswCommitLogger {
	return createTestCommitLoggerForSnapshotsWithOpts(t, rootDir, id)
}

func createCommitlogTestData(t *testing.T, dir string, filenameSizes ...any) {
	// create the files with the specified sizes
	for i := 0; i < len(filenameSizes); i += 2 {
		filename := fmt.Sprintf("%s/%s", dir, filenameSizes[i])
		size := filenameSizes[i+1].(int)
		cl := commitlog.NewLogger(filename, common.NewOSFS())

		generateFakeCommitLogData(t, cl, int64(size))

		err := cl.Close()
		require.NoError(t, err)
	}
}

var generateFakeCommitLogData = func(t *testing.T, cl *commitlog.Logger, size int64) {
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
	t.Helper()

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
			expected: []string{"1002.snapshot"},
			created:  true,
		},
		{
			name:     "small condensed files",
			setup:    []any{"1000.condensed", 100, "1001.condensed", 100, "1002.condensed", 100, "1003.condensed", 100},
			expected: []string{"1002.snapshot"},
			created:  true,
		},
		{
			name:     "bigger condensed files",
			setup:    []any{"1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200},
			expected: []string{"1002.snapshot"},
			created:  true,
		},
		{
			name:  "not enough condensed files",
			setup: []any{"1000.condensed", 1000},
		},
		{
			name:     "enough condensed files",
			setup:    []any{"1000.condensed", 1000, "1001.condensed", 1000},
			expected: []string{"1000.snapshot"},
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
	require.Equal(t, []string{"1002.snapshot"}, files)

	// add new files
	createCommitlogTestData(t, clDir, "1004", 1000, "1005", 5)

	// create snapshot, should create it
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1004.snapshot"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1004"), filepath.Join(clDir, "1004.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it (no new commitlogs)
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.False(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1004.snapshot"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1005"), filepath.Join(clDir, "1005.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it (no new commitlogs)
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.False(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1004.snapshot"}, files)

	// add new files
	createCommitlogTestData(t, clDir, "1006", 5)

	// create snapshot, should create it
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1005.snapshot"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1006"), filepath.Join(clDir, "1006.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it (no new files)
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.False(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1005.snapshot"}, files)

	// add new files
	createCommitlogTestData(t, clDir, "1007", 5)

	// create snapshot, should create it
	created, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.True(t, created)
	files = readDir(t, sDir)
	require.Equal(t, []string{"1006.snapshot"}, files)
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
		require.Equal(t, []string{"1001.snapshot"}, files)
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
		require.Equal(t, []string{"1002.snapshot"}, files)
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
		require.Equal(t, []string{"1001.snapshot"}, files)

		// corrupt the snapshot
		content, err := os.ReadFile(filepath.Join(sDir, "1001.snapshot"))
		require.NoError(t, err)

		copy(content[125:], []byte{0xDE, 0xAD, 0xBE, 0xEF})

		err = os.WriteFile(filepath.Join(sDir, "1001.snapshot"), content, 0o644)
		require.NoError(t, err)

		// add new files
		createCommitlogTestData(t, clDir, "1003.condensed", 1000, "1004.condensed", 1000, "1005.condensed", 1000)

		// create snapshot should still work
		created, _, err = cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		files = readDir(t, sDir)
		require.Equal(t, []string{"1004.snapshot"}, files)
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
		require.ElementsMatch(t, []string{"1000.snapshot"}, files)

		// add new files
		createCommitlogTestData(t, clDir, "1002.condensed", 1000)

		// create new snapshot
		state, createdAt, err = cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.ElementsMatch(t, []string{"1001.snapshot"}, files)

		// try again, should not create a new snapshot
		// but should return the existing one
		state, createdAt, err = cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.ElementsMatch(t, []string{"1001.snapshot"}, files)
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
		require.Equal(t, []string{"1001.snapshot"}, files)

		// empty the snapshot
		err = os.WriteFile(filepath.Join(sDir, "1001.snapshot"), []byte(""), 0o644)
		require.NoError(t, err)

		// create snapshot again
		state, createdAt, err := cl.CreateAndLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		require.NotZero(t, createdAt)
		files = readDir(t, sDir)
		require.Equal(t, []string{"1001.snapshot"}, files)
		// snapshot has content now
		info, err := os.Stat(filepath.Join(sDir, "1001.snapshot"))
		require.NoError(t, err)
		require.Less(t, int64(0), info.Size())
	})
}

func TestCreateSnapshot_NextOne(t *testing.T) {
	// blockSize for testing: 4096
	// commitlog of size 1200 makes snapshot of size 4096 bytes
	s4K := 1200

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
			expectedFiles:   []string{"1000.snapshot"},
			expectedCreated: false,
		},
		{
			name:            "1 new commitlog (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1001.snapshot"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000, "1003", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1002.snapshot"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1000.snapshot"},
			expectedCreated: false,
		},
		{
			name:            "3 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1003.snapshot"},
			expectedCreated: true,
		},
		{
			name:            "4 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004.condensed", 1000, "1005", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1004.snapshot"},
			expectedCreated: true,
		},

		// size % of delta files
		{
			name:                "too small delta size (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 90},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 30},
			delta:               []any{"1002.condensed", 30, "1003.condensed", 30, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 205},
			delta:               []any{"1002", 100},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1001.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 69},
			delta:               []any{"1002.condensed", 69, "1003.condensed", 69, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1003.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "too small delta size (required 125%)",
			setup:               []any{"1000.condensed", s4K, "1001", 1500},
			delta:               []any{"1002", 1100},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 125%)",
			setup:               []any{"1000.condensed", s4K, "1001", 820},
			delta:               []any{"1002.condensed", 820, "1003.condensed", 750, "1004", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 110%)",
			setup:               []any{"1000.condensed", s4K, "1001", 4600},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 110,
			expectedFiles:       []string{"1001.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 110%)",
			setup:               []any{"1000.condensed", s4K, "1001", 1550},
			delta:               []any{"1002.condensed", 1550, "1003.condensed", 1550, "1004", 1200},
			deltaSizePercentage: 110,
			expectedFiles:       []string{"1003.snapshot"},
			expectedCreated:     true,
		},

		// number + size % of delta files + allocChecker
		// NOTE: data in commitlogs is duplicated, so final snapshot size made out of multiple commitlogs
		// will effectively be the same as size of snaptshot created just from biggest commitlog
		{
			name:                "too few delta commitlogs, too small delta size",
			setup:               []any{"1000.condensed", s4K, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size",
			setup:               []any{"1000.condensed", s4K, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too few delta commitlogs",
			setup:               []any{"1000.condensed", s4K, "1001", 1700},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 50,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "enough delta commit logs, enough delta size",
			setup:               []any{"1000.condensed", s4K, "1001", 1700},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 40,
			expectedFiles:       []string{"1001.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "enough delta commit logs, enough delta size, but oom",
			setup:               []any{"1000.condensed", s4K, "1001", 1700},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 40,
			allocCheckerOOM:     true,
			expectedFiles:       []string{"1000.snapshot"},
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
	// blockSize for testing: 4096
	// commitlog of size 1200 makes snapshot of size 4096 bytes
	s4K := 1200

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
			expectedFiles:   []string{"1000.snapshot"},
			expectedCreated: false,
		},
		{
			name:            "1 new commitlog (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1001.snapshot"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (1 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002", 1000, "1003", 1000},
			deltaNumber:     1,
			expectedFiles:   []string{"1002.snapshot"},
			expectedCreated: true,
		},
		{
			name:            "2 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1000.snapshot"},
			expectedCreated: false,
		},
		{
			name:            "3 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1003.snapshot"},
			expectedCreated: true,
		},
		{
			name:            "4 new commitlogs (3 required)",
			setup:           []any{"1000.condensed", 1000, "1001", 1000},
			delta:           []any{"1002.condensed", 1000, "1003.condensed", 1000, "1004.condensed", 1000, "1005", 1000},
			deltaNumber:     3,
			expectedFiles:   []string{"1004.snapshot"},
			expectedCreated: true,
		},

		// size % of delta files
		{
			name:                "too small delta size (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 90},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 30},
			delta:               []any{"1002.condensed", 30, "1003.condensed", 30, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 205},
			delta:               []any{"1002", 100},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1001.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 5%)",
			setup:               []any{"1000.condensed", s4K, "1001", 69},
			delta:               []any{"1002.condensed", 69, "1003.condensed", 69, "1004", 1200},
			deltaSizePercentage: 5,
			expectedFiles:       []string{"1003.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "too small delta size (required 125%)",
			setup:               []any{"1000.condensed", s4K, "1001", 2450},
			delta:               []any{"1002", 1100},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size, multiple files (required 125%)",
			setup:               []any{"1000.condensed", s4K, "1001", 750},
			delta:               []any{"1002.condensed", 750, "1003.condensed", 750, "1004", 1200},
			deltaSizePercentage: 125,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "big enough delta size (required 110%)",
			setup:               []any{"1000.condensed", s4K, "1001", 4600},
			delta:               []any{"1002", 1200},
			deltaSizePercentage: 110,
			expectedFiles:       []string{"1001.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "big enough delta size, multiple files (required 110%)",
			setup:               []any{"1000.condensed", s4K, "1001", 1550},
			delta:               []any{"1002.condensed", 1550, "1003.condensed", 1550, "1004", 1200},
			deltaSizePercentage: 110,
			expectedFiles:       []string{"1003.snapshot"},
			expectedCreated:     true,
		},

		// number + size % of delta files + allocChecker
		// NOTE: data in commitlogs is duplicated, so final snapshot size made out of multiple commitlogs
		// will effectively be the same as size of snaptshot created just from biggest commitlog
		{
			name:                "too few delta commitlogs, too small delta size",
			setup:               []any{"1000.condensed", s4K, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too small delta size",
			setup:               []any{"1000.condensed", s4K, "1001", 1010},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 75,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "too few delta commitlogs",
			setup:               []any{"1000.condensed", s4K, "1001", 1700},
			delta:               []any{"1002", 1000},
			deltaNumber:         2,
			deltaSizePercentage: 50,
			expectedFiles:       []string{"1000.snapshot"},
			expectedCreated:     false,
		},
		{
			name:                "enough delta commit logs, enough delta size",
			setup:               []any{"1000.condensed", s4K, "1001", 1700},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 40,
			expectedFiles:       []string{"1001.snapshot"},
			expectedCreated:     true,
		},
		{
			name:                "enough delta commit logs, enough delta size, oom is ignored",
			setup:               []any{"1000.condensed", s4K, "1001", 1700},
			delta:               []any{"1002", 1000},
			deltaNumber:         1,
			deltaSizePercentage: 40,
			allocCheckerOOM:     true,
			expectedFiles:       []string{"1001.snapshot"},
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

func TestMetadataWriteAndRestore(t *testing.T) {
	t.Run("v3 metadata - basic fields only", func(t *testing.T) {
		// Create a basic state with no compression/encoding
		state := &DeserializationResult{
			Entrypoint: 23,
			Level:      42,
			Compressed: false,
			Nodes:      make([]*vertex, 100),
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.Compressed, restoredState.Compressed)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.False(t, restoredState.MuveraEnabled)
		require.Nil(t, restoredState.CompressionPQData)
		require.Nil(t, restoredState.CompressionSQData)
		require.Nil(t, restoredState.CompressionRQData)
		require.Nil(t, restoredState.CompressionBRQData)
		require.Nil(t, restoredState.EncoderMuvera)
	})

	t.Run("v3 metadata - basic fields only", func(t *testing.T) {
		// Create a basic state with no compression/encoding
		state := &DeserializationResult{
			Entrypoint: 43,
			Level:      15,
			Compressed: false,
			Nodes:      make([]*vertex, 200),
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.Compressed, restoredState.Compressed)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.False(t, restoredState.MuveraEnabled)
		require.Nil(t, restoredState.CompressionPQData)
		require.Nil(t, restoredState.CompressionSQData)
		require.Nil(t, restoredState.CompressionRQData)
		require.Nil(t, restoredState.CompressionBRQData)
		require.Nil(t, restoredState.EncoderMuvera)
	})

	t.Run("v3 metadata - with PQ compression", func(t *testing.T) {
		// Create state with PQ compression
		state := &DeserializationResult{
			Entrypoint: 99,
			Level:      7,
			Compressed: true,
			Nodes:      make([]*vertex, 150),
			CompressionPQData: &compressionhelpers.PQData{
				Dimensions:          128,
				Ks:                  256,
				M:                   8,
				EncoderType:         compressionhelpers.UseTileEncoder,
				EncoderDistribution: 1,
				UseBitsEncoding:     true,
				Encoders:            make([]compressionhelpers.PQEncoder, 8),
			},
		}

		// Create actual encoders for testing
		for i := 0; i < 8; i++ {
			state.CompressionPQData.Encoders[i] = compressionhelpers.NewTileEncoder(8, i, compressionhelpers.EncoderDistribution(1))
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.Compressed, restoredState.Compressed)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.NotNil(t, restoredState.CompressionPQData)
		require.Equal(t, state.CompressionPQData.Dimensions, restoredState.CompressionPQData.Dimensions)
		require.Equal(t, state.CompressionPQData.Ks, restoredState.CompressionPQData.Ks)
		require.Equal(t, state.CompressionPQData.M, restoredState.CompressionPQData.M)
		require.Equal(t, state.CompressionPQData.EncoderType, restoredState.CompressionPQData.EncoderType)
		require.Equal(t, state.CompressionPQData.EncoderDistribution, restoredState.CompressionPQData.EncoderDistribution)
		require.Equal(t, state.CompressionPQData.UseBitsEncoding, restoredState.CompressionPQData.UseBitsEncoding)
		require.Equal(t, len(state.CompressionPQData.Encoders), len(restoredState.CompressionPQData.Encoders))
	})

	t.Run("v3 metadata - with SQ compression", func(t *testing.T) {
		// Create state with SQ compression
		state := &DeserializationResult{
			Entrypoint: 120,
			Level:      12,
			Compressed: true,
			Nodes:      make([]*vertex, 300),
			CompressionSQData: &compressionhelpers.SQData{
				Dimensions: 64,
				A:          1.5,
				B:          2.7,
			},
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.Compressed, restoredState.Compressed)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.NotNil(t, restoredState.CompressionSQData)
		require.Equal(t, state.CompressionSQData.Dimensions, restoredState.CompressionSQData.Dimensions)
		require.Equal(t, state.CompressionSQData.A, restoredState.CompressionSQData.A)
		require.Equal(t, state.CompressionSQData.B, restoredState.CompressionSQData.B)
	})

	t.Run("v3 metadata - with RQ compression", func(t *testing.T) {
		// Create state with RQ compression
		state := &DeserializationResult{
			Entrypoint: 212,
			Level:      5,
			Compressed: true,
			Nodes:      make([]*vertex, 250),
			CompressionRQData: &compressionhelpers.RQData{
				InputDim: 8,
				Bits:     8,
				Rotation: compressionhelpers.FastRotation{
					OutputDim: 8,
					Rounds:    1,
					Swaps: [][]compressionhelpers.Swap{
						{
							{I: 0, J: 1},
							{I: 2, J: 3},
							{I: 4, J: 5},
							{I: 6, J: 7},
						},
					},
					Signs: [][]float32{
						{1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0},
					},
				},
			},
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Compressed, true)
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.Compressed, restoredState.Compressed)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.NotNil(t, restoredState.CompressionRQData)
		require.Equal(t, state.CompressionRQData.InputDim, restoredState.CompressionRQData.InputDim)
		require.Equal(t, state.CompressionRQData.Bits, restoredState.CompressionRQData.Bits)
		require.Equal(t, state.CompressionRQData.Rotation.OutputDim, restoredState.CompressionRQData.Rotation.OutputDim)
		require.Equal(t, state.CompressionRQData.Rotation.Rounds, restoredState.CompressionRQData.Rotation.Rounds)
		require.Equal(t, len(state.CompressionRQData.Rotation.Swaps), len(restoredState.CompressionRQData.Rotation.Swaps))
		require.Equal(t, len(state.CompressionRQData.Rotation.Signs), len(restoredState.CompressionRQData.Rotation.Signs))
		require.Equal(t, state.CompressionRQData.Rotation.Swaps[0][0].I, restoredState.CompressionRQData.Rotation.Swaps[0][0].I)
		require.Equal(t, state.CompressionRQData.Rotation.Swaps[0][0].J, restoredState.CompressionRQData.Rotation.Swaps[0][0].J)
		require.Equal(t, state.CompressionRQData.Rotation.Signs[0][0], restoredState.CompressionRQData.Rotation.Signs[0][0])
	})

	t.Run("v3 metadata - with Muvera encoding", func(t *testing.T) {
		// Create state with Muvera encoding
		state := &DeserializationResult{
			Entrypoint:    172,
			Level:         8,
			MuveraEnabled: true,
			Nodes:         make([]*vertex, 180),
			EncoderMuvera: &multivector.MuveraData{
				Dimensions:   8,
				KSim:         2,
				NumClusters:  4,
				DProjections: 1,
				Repetitions:  1,
				Gaussians: [][][]float32{
					{
						make([]float32, 8),
						make([]float32, 8),
					},
				},
				S: [][][]float32{
					{
						make([]float32, 8),
					},
				},
			},
		}

		// Initialize with some values
		for i := 0; i < 8; i++ {
			state.EncoderMuvera.Gaussians[0][0][i] = float32(i) * 0.1
			state.EncoderMuvera.S[0][0][i] = float32(i) * 0.2
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Compressed, false)
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.MuveraEnabled, restoredState.MuveraEnabled)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.NotNil(t, restoredState.EncoderMuvera)
		require.Equal(t, state.EncoderMuvera.Dimensions, restoredState.EncoderMuvera.Dimensions)
		require.Equal(t, state.EncoderMuvera.KSim, restoredState.EncoderMuvera.KSim)
		require.Equal(t, state.EncoderMuvera.NumClusters, restoredState.EncoderMuvera.NumClusters)
		require.Equal(t, state.EncoderMuvera.DProjections, restoredState.EncoderMuvera.DProjections)
		require.Equal(t, state.EncoderMuvera.Repetitions, restoredState.EncoderMuvera.Repetitions)
		require.Equal(t, len(state.EncoderMuvera.Gaussians), len(restoredState.EncoderMuvera.Gaussians))
		require.Equal(t, len(state.EncoderMuvera.S), len(restoredState.EncoderMuvera.S))
		require.Equal(t, state.EncoderMuvera.Gaussians[0][0][0], restoredState.EncoderMuvera.Gaussians[0][0][0])
		require.Equal(t, state.EncoderMuvera.S[0][0][0], restoredState.EncoderMuvera.S[0][0][0])
	})

	t.Run("v3 metadata - with BRQ compression", func(t *testing.T) {
		// Create state with BRQ compression
		state := &DeserializationResult{
			Entrypoint: 212,
			Level:      5,
			Compressed: true,
			Nodes:      make([]*vertex, 250),
			CompressionBRQData: &compressionhelpers.BRQData{
				InputDim: 8,
				Rotation: compressionhelpers.FastRotation{
					OutputDim: 8,
					Rounds:    1,
					Swaps: [][]compressionhelpers.Swap{
						{
							{I: 0, J: 1},
							{I: 2, J: 3},
							{I: 4, J: 5},
							{I: 6, J: 7},
						},
					},
					Signs: [][]float32{
						{1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0},
					},
				},
				Rounding: []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
			},
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Compressed, true)
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.Compressed, restoredState.Compressed)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.NotNil(t, restoredState.CompressionBRQData)
		require.Equal(t, state.CompressionBRQData.InputDim, restoredState.CompressionBRQData.InputDim)
		require.Equal(t, state.CompressionBRQData.Rotation.OutputDim, restoredState.CompressionBRQData.Rotation.OutputDim)
		require.Equal(t, state.CompressionBRQData.Rotation.Rounds, restoredState.CompressionBRQData.Rotation.Rounds)
		require.Equal(t, len(state.CompressionBRQData.Rotation.Swaps), len(restoredState.CompressionBRQData.Rotation.Swaps))
		require.Equal(t, len(state.CompressionBRQData.Rotation.Signs), len(restoredState.CompressionBRQData.Rotation.Signs))
		require.Equal(t, state.CompressionBRQData.Rotation.Swaps[0][0].I, restoredState.CompressionBRQData.Rotation.Swaps[0][0].I)
		require.Equal(t, state.CompressionBRQData.Rotation.Swaps[0][0].J, restoredState.CompressionBRQData.Rotation.Swaps[0][0].J)
		require.Equal(t, state.CompressionBRQData.Rotation.Signs[0][0], restoredState.CompressionBRQData.Rotation.Signs[0][0])
		require.Equal(t, state.CompressionBRQData.Rounding[0], restoredState.CompressionBRQData.Rounding[0])
	})

	t.Run("v3 metadata - with Muvera encoding and SQ compression", func(t *testing.T) {
		// Create state with Muvera encoding
		state := &DeserializationResult{
			Entrypoint:    172,
			Level:         8,
			Compressed:    true,
			MuveraEnabled: true,
			Nodes:         make([]*vertex, 180),
			CompressionSQData: &compressionhelpers.SQData{
				Dimensions: 64,
				A:          1.5,
				B:          2.7,
			},
			EncoderMuvera: &multivector.MuveraData{
				Dimensions:   8,
				KSim:         2,
				NumClusters:  4,
				DProjections: 1,
				Repetitions:  1,
				Gaussians: [][][]float32{
					{
						make([]float32, 8),
						make([]float32, 8),
					},
				},
				S: [][][]float32{
					{
						make([]float32, 8),
					},
				},
			},
		}

		// Initialize with some values
		for i := 0; i < 8; i++ {
			state.EncoderMuvera.Gaussians[0][0][i] = float32(i) * 0.1
			state.EncoderMuvera.S[0][0][i] = float32(i) * 0.2
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		// Verify all fields match
		require.Equal(t, state.Compressed, true)
		require.Equal(t, state.Entrypoint, restoredState.Entrypoint)
		require.Equal(t, state.Level, restoredState.Level)
		require.Equal(t, state.CompressionSQData.Dimensions, restoredState.CompressionSQData.Dimensions)
		require.Equal(t, state.CompressionSQData.A, restoredState.CompressionSQData.A)
		require.Equal(t, state.CompressionSQData.B, restoredState.CompressionSQData.B)
		require.Equal(t, state.MuveraEnabled, restoredState.MuveraEnabled)
		require.Equal(t, len(state.Nodes), len(restoredState.Nodes))
		require.NotNil(t, restoredState.EncoderMuvera)
		require.Equal(t, state.EncoderMuvera.Dimensions, restoredState.EncoderMuvera.Dimensions)
		require.Equal(t, state.EncoderMuvera.KSim, restoredState.EncoderMuvera.KSim)
		require.Equal(t, state.EncoderMuvera.NumClusters, restoredState.EncoderMuvera.NumClusters)
		require.Equal(t, state.EncoderMuvera.DProjections, restoredState.EncoderMuvera.DProjections)
		require.Equal(t, state.EncoderMuvera.Repetitions, restoredState.EncoderMuvera.Repetitions)
		require.Equal(t, len(state.EncoderMuvera.Gaussians), len(restoredState.EncoderMuvera.Gaussians))
		require.Equal(t, len(state.EncoderMuvera.S), len(restoredState.EncoderMuvera.S))
		require.Equal(t, state.EncoderMuvera.Gaussians[0][0][0], restoredState.EncoderMuvera.Gaussians[0][0][0])
		require.Equal(t, state.EncoderMuvera.S[0][0][0], restoredState.EncoderMuvera.S[0][0][0])
	})

	t.Run("v3 metadata - compression is supported", func(t *testing.T) {
		// Create state with compression (v3 supports compression)
		state := &DeserializationResult{
			Entrypoint: 33,
			Level:      10,
			Compressed: false,
			Nodes:      make([]*vertex, 100),
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)
		require.Equal(t, state.Compressed, restoredState.Compressed)
	})

	t.Run("invalid version should fail", func(t *testing.T) {
		state := &DeserializationResult{
			Entrypoint: 29,
			Level:      3,
			Compressed: false,
			Nodes:      make([]*vertex, 50),
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Corrupt the version byte to make it invalid
		data, err := os.ReadFile(snapshotPath)
		require.NoError(t, err)
		data[0] = 99 // invalid version

		// Write corrupted data back
		err = os.WriteFile(snapshotPath, data, 0o644)
		require.NoError(t, err)

		// Read should fail due to invalid version
		_, err = cl.readSnapshot(snapshotPath)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported snapshot version 99")
	})

	t.Run("checksum validation", func(t *testing.T) {
		state := &DeserializationResult{
			Entrypoint: 28,
			Level:      6,
			Compressed: false,
			Nodes:      make([]*vertex, 75),
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err := cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Corrupt the data by changing a byte
		data, err := os.ReadFile(snapshotPath)
		require.NoError(t, err)
		data[5] = data[5] ^ 0xFF // flip bits

		// Write corrupted data back
		err = os.WriteFile(snapshotPath, data, 0o644)
		require.NoError(t, err)

		// Read should fail due to checksum mismatch
		_, err = cl.readSnapshot(snapshotPath)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid metadata checksum")
	})

	t.Run("v3 - multi block", func(t *testing.T) {
		size := 30_000
		state := &DeserializationResult{
			Entrypoint: 28,
			Level:      6,
			Compressed: false,
			Nodes:      make([]*vertex, size),
			Tombstones: make(map[uint64]struct{}),
		}

		c, err := packedconn.NewWithMaxLayer(2)
		require.Nil(t, err)
		c.ReplaceLayer(0, connsSlice1)

		for i := 0; i < size; i++ {
			if i%30 == 0 {
				continue // nil node
			}

			if i%5 == 0 {
				state.Tombstones[uint64(i)] = struct{}{}
			}

			state.Nodes[i] = &vertex{
				id:          uint64(i),
				level:       i % 6,
				connections: c,
			}
		}

		dir := t.TempDir()
		id := "test"
		cl := createTestCommitLoggerForSnapshots(t, dir, id)

		// Write snapshot to a temporary file
		snapshotPath := filepath.Join(snapshotDirectory(dir, id), "test.snapshot")
		err = cl.writeSnapshot(state, snapshotPath)
		require.NoError(t, err)

		// Read snapshot back
		restoredState, err := cl.readSnapshot(snapshotPath)
		require.NoError(t, err)

		require.NotEqual(t, state.Nodes, restoredState.Nodes)
	})
}

var connsSlice1 = []uint64{
	4477, 83, 6777, 13118, 12903, 12873, 14397, 15034, 15127, 15162, 15219, 15599, 17627,
	18624, 18844, 19359, 22981, 23099, 36188, 37400, 39724, 39810, 47254, 58047, 59647, 61746,
	64635, 66528, 70470, 73936, 86283, 86697, 120033, 129098, 131345, 137609, 140937, 186468,
	191226, 199803, 206818, 223456, 271063, 278598, 288539, 395876, 396785, 452103, 487237,
	506431, 507230, 554813, 572566, 595572, 660562, 694477, 728865, 730031, 746368, 809331,
	949338,
}

func TestCreateCondensedSnapshot(t *testing.T) {
	prevFn := generateFakeCommitLogData
	t.Cleanup(func() {
		generateFakeCommitLogData = prevFn
	})

	dir := t.TempDir()
	id := "main"

	var nodes int
	var once sync.Once
	// this creates a commit log file with half of the nodes being tombstones
	generateFakeCommitLogData = func(t *testing.T, cl *commitlog.Logger, size int64) {
		var err error

		i := 0
		for {
			if i > 0 && i%2 == 0 {
				err = cl.AddTombstone(uint64(i - 1)) // tombstone
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

		once.Do(func() {
			nodes = i
		})
	}

	cl := createTestCommitLoggerForSnapshotsWithOpts(t, dir, id,
		WithCommitlogThresholdForCombining(1200),
		WithSnapshotDisabled(false),
		WithSnapshotCreateInterval(time.Microsecond),
	)

	// create two condensed commit log files with half of the nodes being tombstones and generate a snapshot
	createCommitlogAndSnapshotTestData(t, cl, "1000.condensed", 800, "1001.condensed", 200)

	require.Equal(t, []string{"1000.snapshot"}, readDir(t, snapshotDirectory(dir, id)))
	files, err := os.ReadDir(commitLogDirectory(dir, id))
	require.NoError(t, err)
	for _, item := range files {
		if item.IsDir() {
			continue
		}
		info, err := item.Info()
		require.NoError(t, err)
		fmt.Println(info.Name(), info.Size(), info.ModTime())
	}

	// this creates a commit log file with only deletions
	generateFakeCommitLogData = func(t *testing.T, cl *commitlog.Logger, size int64) {
		var err error

		for i := nodes / 2; i < nodes; i++ {
			err = cl.DeleteNode(uint64(i)) // hard delete
			require.NoError(t, err)

			err = cl.RemoveTombstone(uint64(i)) // remove tombstone
			require.NoError(t, err)

			err = cl.Flush()
			require.NoError(t, err)
		}
	}

	// create one commit log file with the second half of the nodes deleted
	createCommitlogTestData(t, commitLogDirectory(dir, id), "1002.condensed", 1000)

	shouldAbort := func() bool {
		return false
	}

	// manually trigger maintenance
	executed := cl.startCommitLogsMaintenance(shouldAbort)
	require.True(t, executed)

	require.Equal(t, []string{"1000.condensed", "1002"}, readDir(t, commitLogDirectory(dir, id)))
	require.Equal(t, []string{"1000.snapshot"}, readDir(t, snapshotDirectory(dir, id)))

	files, err = os.ReadDir(commitLogDirectory(dir, id))
	require.NoError(t, err)
	for _, item := range files {
		if item.IsDir() {
			continue
		}
		info, err := item.Info()
		require.NoError(t, err)
		fmt.Println(info.Name(), info.Size(), info.ModTime())
	}

	// create some delta commit log
	generateFakeCommitLogData = prevFn
	createCommitlogTestData(t, commitLogDirectory(dir, id), "1003", 700)

	// manually trigger maintenance
	executed = cl.startCommitLogsMaintenance(shouldAbort)
	require.True(t, executed)

	require.Equal(t, []string{"1000.condensed", "1002.condensed", "1003"}, readDir(t, commitLogDirectory(dir, id)))
	require.Equal(t, []string{"1002.snapshot"}, readDir(t, snapshotDirectory(dir, id)))
}

func TestCommitLogger_Snapshot_Race(t *testing.T) {
	dir := t.TempDir()
	id := "main"
	cl := createTestCommitLoggerForSnapshots(t, dir, id)
	clDir := commitLogDirectory(dir, id)
	sDir := snapshotDirectory(dir, id)
	os.MkdirAll(sDir, os.ModePerm)

	createCommitlogTestData(t, clDir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// create snapshot
			_, _, err := cl.CreateSnapshot()
			require.NoError(t, err)
			files := readDir(t, sDir)
			require.Equal(t, []string{"1001.snapshot"}, files)
		}()
	}

	wg.Wait()
}

func TestCreateSnapshotCrashSafety(t *testing.T) {
	t.Run("fail on Write", func(t *testing.T) {
		var stop bool
		var i int
		for i = 0; !stop; i++ {
			t.Run(fmt.Sprintf("fails on write number %d", i+1), func(t *testing.T) {
				dir := t.TempDir()
				id := "main"
				clDir := commitLogDirectory(dir, id)
				sDir := snapshotDirectory(dir, id)

				fs := common.NewTestFS()

				var counter int

				fs.OnOpenFile = func(f common.File) common.File {
					return &common.TestFile{
						File: f,
						OnWrite: func(b []byte) (n int, err error) {
							counter++
							if counter == i+1 {
								return 0, errors.Errorf("fake temp error: %d writes", counter)
							}
							return f.Write(b)
						},
					}
				}

				cl := createTestCommitLoggerForSnapshotsWithOpts(t, dir, id, WithFS(fs))
				createCommitlogTestData(t, clDir, "1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200)

				// create snapshot once: should fail
				created, _, err := cl.CreateSnapshot()
				if created || err == nil {
					stop = true
					t.SkipNow()
				}

				require.False(t, created)
				got := readDir(t, sDir)
				require.Equal(t, []string{"1002.snapshot.tmp"}, got)

				// create snapshot again: should succeed
				created, _, err = cl.CreateSnapshot()
				require.NoError(t, err)
				require.True(t, created)
				got = readDir(t, sDir)
				require.Equal(t, []string{"1002.snapshot"}, got)
			})
		}
		// ensure it failed at least once
		require.Greater(t, i, 1, "should have failed at least once")
	})

	t.Run("fail on Rename", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)

		fs := common.NewTestFS()

		var counter int
		fs.OnRename = func(oldpath string, newpath string) error {
			counter++
			if counter > 1 {
				return os.Rename(oldpath, newpath)
			}
			return errors.Errorf("fake temp error on rename")
		}

		cl := createTestCommitLoggerForSnapshotsWithOpts(t, dir, id, WithFS(fs))
		createCommitlogTestData(t, clDir, "1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200)

		// create snapshot once: should fail
		created, _, err := cl.CreateSnapshot()
		require.Error(t, err)
		require.False(t, created)

		got := readDir(t, sDir)
		require.Equal(t, []string{"1002.snapshot.tmp"}, got)

		// create snapshot again: should succeed
		created, _, err = cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		got = readDir(t, sDir)
		require.Equal(t, []string{"1002.snapshot"}, got)
	})

	t.Run("calls Fsync", func(t *testing.T) {
		dir := t.TempDir()
		id := "main"
		clDir := commitLogDirectory(dir, id)
		sDir := snapshotDirectory(dir, id)

		fs := common.NewTestFS()

		var fsyncCalled bool
		fs.OnOpenFile = func(f common.File) common.File {
			return &common.TestFile{
				File: f,
				OnSync: func() error {
					fsyncCalled = true
					return f.Sync()
				},
			}
		}

		cl := createTestCommitLoggerForSnapshotsWithOpts(t, dir, id, WithFS(fs))
		createCommitlogTestData(t, clDir, "1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200)

		// create snapshot once: should succeed
		created, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.True(t, created)
		require.True(t, fsyncCalled)

		got := readDir(t, sDir)
		require.Equal(t, []string{"1002.snapshot"}, got)
	})
}
