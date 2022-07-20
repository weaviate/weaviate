package modstgfs

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/stretchr/testify/assert"
)

func TestSnapshotStorage_StoreSnapshot(t *testing.T) {
	testdataMainDir := "./testData"
	snapshotsMainDir := "./snapshots"
	snapshotsRelativePath := filepath.Join(snapshotsMainDir, "some", "nested", "dir") // ./snapshots/some/nested/dir
	snapshotsAbsolutePath, _ := filepath.Abs(snapshotsRelativePath)

	ctx := context.Background()
	removeDir(t, snapshotsMainDir) // just in case

	t.Run("fails init storage module with empty snapshots path", func(t *testing.T) {
		module := New()
		err := module.initSnapshotStorage(ctx, "")

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "empty snapshots path provided")
	})

	t.Run("fails init storage module with relative snapshots path", func(t *testing.T) {
		module := New()
		err := module.initSnapshotStorage(ctx, snapshotsRelativePath)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "relative snapshots path provided")
	})

	t.Run("inits storage module with absolute snapshots path if dir does not exist", func(t *testing.T) {
		defer removeDir(t, snapshotsMainDir)

		module := New()
		err := module.initSnapshotStorage(ctx, snapshotsAbsolutePath)

		assert.Nil(t, err)

		_, err = os.Stat(snapshotsAbsolutePath)
		assert.Nil(t, err) // dir exists
	})

	t.Run("inits storage module with absolute snapshots path if dir already exists", func(t *testing.T) {
		makeDir(t, snapshotsRelativePath)
		defer removeDir(t, snapshotsMainDir)

		module := New()
		err := module.initSnapshotStorage(ctx, snapshotsAbsolutePath)

		assert.Nil(t, err)

		_, err = os.Stat(snapshotsAbsolutePath)
		assert.Nil(t, err) // dir exists
	})

	t.Run("copies snapshot data", func(t *testing.T) {
		testDir := makeTestDir(t, testdataMainDir)
		defer removeDir(t, testdataMainDir)
		defer removeDir(t, snapshotsMainDir)

		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		module := New()
		module.initSnapshotStorage(ctx, snapshotsAbsolutePath)
		err := module.StoreSnapshot(ctxSnapshot, snapshot)

		assert.Nil(t, err)

		var expectedFilePath string
		var info os.FileInfo
		for _, filePath := range snapshot.Files {
			expectedFilePath = filepath.Join(snapshotsAbsolutePath, snapshot.ID, filePath)
			info, err = os.Stat(expectedFilePath)
			orgInfo, _ := os.Stat(filePath)

			assert.Nil(t, err) // file exists
			assert.Equal(t, orgInfo.Size(), info.Size())
		}

		expectedFilePath = filepath.Join(snapshotsAbsolutePath, snapshot.ID, "snapshot.json")
		info, err = os.Stat(expectedFilePath)
		assert.Nil(t, err) // file exists
		assert.Greater(t, info.Size(), int64(0))
	})
}

func makeTestDir(t *testing.T, basePath string) string {
	rand.Seed(time.Now().UnixNano())
	dirPath := filepath.Join(basePath, strconv.Itoa(rand.Intn(10000000)))
	makeDir(t, dirPath)
	return dirPath
}

func makeDir(t *testing.T, dirPath string) {
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		t.Fatalf("failed to make test dir '%s': %s", dirPath, err)
	}
}

func removeDir(t *testing.T, dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		t.Errorf("failed to remove test dir '%s': %s", dirPath, err)
	}
}

func createSnapshotInstance(t *testing.T, dirPath string) snapshots.Snapshot {
	startedAt := time.Now()
	basePath, _ := os.Getwd()
	filePaths := createTestFiles(t, dirPath)

	return snapshots.Snapshot{
		ID:          "snapshot_id",
		StartedAt:   startedAt,
		CompletedAt: time.Now(),
		Files:       filePaths,
		BasePath:    basePath,
	}
}

func createTestFiles(t *testing.T, dirPath string) []string {
	count := 5
	filePaths := make([]string, count)
	var fileName string

	for i := 0; i < count; i += 1 {
		fileName = fmt.Sprintf("file_%d.db", i)
		filePaths[i] = filepath.Join(dirPath, fileName)
		file, err := os.Create(filePaths[i])
		if err != nil {
			t.Fatalf("failed to create test file '%s': %s", fileName, err)
		}
		fmt.Fprintf(file, "This is content of db file named %s", fileName)
		file.Close()
	}
	return filePaths
}
