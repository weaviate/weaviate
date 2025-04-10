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

func createTestCommitLoggerForSnapshots(t *testing.T, dir string) *hnswCommitLogger {
	opts := []CommitlogOption{
		WithCommitlogThreshold(1000),
		WithCommitlogThresholdForCombining(200),
		WithCondensor(&fakeCondensor{}),
	}

	commitLogDir := commitLogDirectory(dir, "main")
	cl, err := NewCommitLogger(dir, "main", logrus.New(), cyclemanager.NewCallbackGroupNoop(), opts...)
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

func createSnapshotTestData(t *testing.T, dir string, filenameSizes ...any) {
	commitLogDir := commitLogDirectory(dir, "main")
	// create the files with the specified sizes
	for i := 0; i < len(filenameSizes); i += 2 {
		filename := fmt.Sprintf("%s/%s", commitLogDir, filenameSizes[i])
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

func readDir(t *testing.T, dir string) []string {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)

	var result []string
	for _, item := range files {
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
			name:     "single file",
			setup:    []any{"1000", 1000},
			expected: []string{"1000"},
		},
		{
			name:     "many non-condensed files",
			setup:    []any{"1000", 1000, "1001", 1000, "1002", 1000, "1003", 1000},
			expected: []string{"1000", "1001", "1002", "1003"},
		},
		{
			name:     "mutable condensed files",
			setup:    []any{"1000.condensed", 100, "1001.condensed", 100, "1002.condensed", 100, "1003.condensed", 100},
			expected: []string{"1000.condensed", "1001.condensed", "1002.condensed", "1003.condensed"},
		},
		{
			name:     "immutable condensed files",
			setup:    []any{"1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200},
			expected: []string{"1000.condensed", "1001.condensed", "1002.condensed", "1002.snapshot", "1002.snapshot.checkpoints", "1003.condensed"},
			created:  true,
		},
		{
			name:     "not enough immutable condensed files",
			setup:    []any{"1000.condensed", 1000},
			expected: []string{"1000.condensed"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			cl := createTestCommitLoggerForSnapshots(t, dir)
			createSnapshotTestData(t, dir, test.setup...)

			state, _, err := cl.CreateSnapshot()
			require.NoError(t, err)
			require.Equal(t, state != nil, test.created)
			require.Equal(t, test.expected, readDir(t, commitLogDirectory(dir, "main")))
		})
	}
}

func TestCreateSnapshotWithExistingState(t *testing.T) {
	dir := t.TempDir()
	cl := createTestCommitLoggerForSnapshots(t, dir)
	createSnapshotTestData(t, dir, "1000.condensed", 200, "1001.condensed", 200, "1002.condensed", 200, "1003.condensed", 200)
	clDir := commitLogDirectory(dir, "main")

	// create snapshot
	state, _, err := cl.CreateSnapshot()
	require.NoError(t, err)
	require.NotNil(t, state)

	files := readDir(t, clDir)
	require.Equal(t, []string{"1000.condensed", "1001.condensed", "1002.condensed", "1002.snapshot", "1002.snapshot.checkpoints", "1003.condensed"}, files)

	// add new files
	createSnapshotTestData(t, dir, "1004", 1000, "1005", 5)

	// create snapshot, should not create it
	state, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.Nil(t, state)
	files = readDir(t, clDir)
	require.Equal(t, []string{"1000.condensed", "1001.condensed", "1002.condensed", "1002.snapshot", "1002.snapshot.checkpoints", "1003.condensed", "1004", "1005"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1004"), filepath.Join(clDir, "1004.condensed"))
	require.NoError(t, err)

	// create snapshot, should create it
	state, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.NotNil(t, state)
	files = readDir(t, clDir)
	require.Equal(t, []string{"1000.condensed", "1001.condensed", "1002.condensed", "1003.condensed", "1003.snapshot", "1003.snapshot.checkpoints", "1004.condensed", "1005"}, files)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1005"), filepath.Join(clDir, "1005.condensed"))
	require.NoError(t, err)

	// create snapshot, should create it, because 1005.condensed is the last condensed file
	state, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.NotNil(t, state)
	files = readDir(t, clDir)
	require.Equal(t, []string{"1000.condensed", "1001.condensed", "1002.condensed", "1003.condensed", "1004.condensed", "1004.snapshot", "1004.snapshot.checkpoints", "1005.condensed"}, files)

	// add new files
	createSnapshotTestData(t, dir, "1006", 5)

	// create snapshot, should not create it, because 1005.condensed is the last condensed file
	state, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.Nil(t, state)

	// simulate file condensation
	err = os.Rename(filepath.Join(clDir, "1006"), filepath.Join(clDir, "1006.condensed"))
	require.NoError(t, err)

	// create snapshot, should not create it, because 1005.condensed can be combined with 1006.condensed
	// as they are both below the threshold
	state, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.Nil(t, state)

	// increase the size of 1005.condensed
	err = os.Remove(filepath.Join(clDir, "1005.condensed"))
	require.NoError(t, err)
	createSnapshotTestData(t, dir, "1005.condensed", 1000)

	// create snapshot, should create it, because 1005.condensed is above the threshold
	state, _, err = cl.CreateSnapshot()
	require.NoError(t, err)
	require.NotNil(t, state)
	files = readDir(t, clDir)
	require.Equal(t, []string{"1000.condensed", "1001.condensed", "1002.condensed", "1003.condensed", "1004.condensed", "1005.condensed", "1005.snapshot", "1005.snapshot.checkpoints", "1006.condensed"}, files)
}

func TestCreateSnapshotCrashRecovery(t *testing.T) {
	t.Run("crash before renaming from .tmp to .snapshot", func(t *testing.T) {
		dir := t.TempDir()
		cl := createTestCommitLoggerForSnapshots(t, dir)
		clDir := commitLogDirectory(dir, "main")

		createSnapshotTestData(t, dir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// simulate shutdown before snapshot renaming
		createSnapshotTestData(t, dir, "1000.snapshot.tmp", 1000)

		// create snapshot
		state, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files := readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1001.condensed", "1001.snapshot", "1001.snapshot.checkpoints", "1002.condensed"}, files)
	})

	t.Run("missing checkpoints", func(t *testing.T) {
		dir := t.TempDir()
		cl := createTestCommitLoggerForSnapshots(t, dir)
		clDir := commitLogDirectory(dir, "main")

		// missing checkpoints
		createSnapshotTestData(t, dir, "1000.condensed", 1000, "1000.snapshot", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// create snapshot should still work
		state, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files := readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1001.condensed", "1001.snapshot", "1001.snapshot.checkpoints", "1002.condensed"}, files)
	})

	t.Run("corrupt snapshot", func(t *testing.T) {
		dir := t.TempDir()
		cl := createTestCommitLoggerForSnapshots(t, dir)
		clDir := commitLogDirectory(dir, "main")

		createSnapshotTestData(t, dir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// create snapshot
		state, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files := readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1001.condensed", "1001.snapshot", "1001.snapshot.checkpoints", "1002.condensed"}, files)

		// corrupt the snapshot
		err = os.WriteFile(filepath.Join(clDir, "1001.snapshot"), []byte("corrupt"), 0o644)
		require.NoError(t, err)

		// add new files
		createSnapshotTestData(t, dir, "1003.condensed", 1000, "1004.condensed", 1000, "1005.condensed", 1000)

		// create snapshot should still work
		state, _, err = cl.CreateSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files = readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1001.condensed", "1002.condensed", "1003.condensed", "1004.condensed", "1004.snapshot", "1004.snapshot.checkpoints", "1005.condensed"}, files)
	})
}

func TestCreateOrLoadSnapshot(t *testing.T) {
	t.Run("create and load snapshot", func(t *testing.T) {
		dir := t.TempDir()
		cl := createTestCommitLoggerForSnapshots(t, dir)
		clDir := commitLogDirectory(dir, "main")

		createSnapshotTestData(t, dir, "1000.condensed", 1000)

		// try to create a snapshot, should not create it
		// because there is not enough data
		state, _, err := cl.CreateOrLoadSnapshot()
		require.NoError(t, err)
		require.Nil(t, state)
		files := readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed"}, files)

		// add new files
		createSnapshotTestData(t, dir, "1001.condensed", 1000)

		// create snapshot
		state, _, err = cl.CreateOrLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files = readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1000.snapshot", "1000.snapshot.checkpoints", "1001.condensed"}, files)

		// try again, should not create a new snapshot
		// but should return the existing one
		state, _, err = cl.CreateOrLoadSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files = readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1000.snapshot", "1000.snapshot.checkpoints", "1001.condensed"}, files)
	})

	t.Run("empty snapshot", func(t *testing.T) {
		dir := t.TempDir()
		cl := createTestCommitLoggerForSnapshots(t, dir)
		clDir := commitLogDirectory(dir, "main")

		createSnapshotTestData(t, dir, "1000.condensed", 1000, "1001.condensed", 1000, "1002.condensed", 1000)

		// create snapshot
		state, _, err := cl.CreateSnapshot()
		require.NoError(t, err)
		require.NotNil(t, state)
		files := readDir(t, clDir)
		require.Equal(t, []string{"1000.condensed", "1001.condensed", "1001.snapshot", "1001.snapshot.checkpoints", "1002.condensed"}, files)

		// empty the snapshot
		err = os.WriteFile(filepath.Join(clDir, "1001.snapshot"), []byte(""), 0o644)
		require.NoError(t, err)

		// create snapshot should still work
		state, from, err := cl.CreateOrLoadSnapshot()
		require.NoError(t, err)
		require.Nil(t, state)
		require.Zero(t, from)
	})
}
