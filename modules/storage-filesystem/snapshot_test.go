//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

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
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

var (
	testdataMainDir  = "./testData"
	snapshotsMainDir = "./snapshots"
)

func TestSnapshotStorage_StoreSnapshot(t *testing.T) {
	snapshotsRelativePath := filepath.Join(snapshotsMainDir, "some", "nested", "dir") // ./snapshots/some/nested/dir
	snapshotsAbsolutePath, _ := filepath.Abs(snapshotsRelativePath)
	testDir := makeTestDir(t, testdataMainDir)
	defer removeDir(t, testdataMainDir)
	defer removeDir(t, snapshotsMainDir)

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
		snapshot := createSnapshotInstance(t, testDir)
		ctxSnapshot := context.Background()

		module := New()
		module.initSnapshotStorage(ctx, snapshotsAbsolutePath)
		module.logger, _ = test.NewNullLogger()
		module.dataPath, _ = os.Getwd()
		err := module.StoreSnapshot(ctxSnapshot, snapshot)

		assert.Nil(t, err)

		var expectedFilePath string
		var info os.FileInfo
		for _, filePath := range snapshot.Files {
			expectedFilePath = module.makeSnapshotFilePath(snapshot.ClassName, snapshot.ID, filePath)
			info, err = os.Stat(expectedFilePath)
			assert.Nil(t, err) // file exists
			orgInfo, err := os.Stat(filePath)
			assert.Nil(t, err) // file exists

			assert.Equal(t, orgInfo.Size(), info.Size())
		}

		expectedFilePath = module.makeMetaFilePath(snapshot.ClassName, snapshot.ID)
		info, err = os.Stat(expectedFilePath)
		assert.Nil(t, err) // file exists
		assert.Greater(t, info.Size(), int64(0))
	})

	t.Run("restores snapshot data", func(t *testing.T) {
		ctxSnapshot := context.Background()
		module := New()
		module.initSnapshotStorage(ctx, snapshotsAbsolutePath)
		module.logger, _ = test.NewNullLogger()
		module.dataPath, _ = os.Getwd()

		// List all files in testDir
		files, _ := os.ReadDir(testDir)

		// Remove the files, ready for restore
		for _, f := range files {
			os.Remove(filepath.Join(testDir, f.Name()))
			assert.NoFileExists(t, filepath.Join(testDir, f.Name()))
		}

		// Use the previous test snapshot to test the restore function

		err := module.RestoreSnapshot(ctxSnapshot, "classname", "snapshot_id")
		assert.Nil(t, err)

		assert.DirExists(t, module.dataPath)

		// Check that every file in the snapshot exists in testDir
		for _, filePath := range files {
			expectedFilePath := filepath.Join(testDir, filePath.Name())
			assert.FileExists(t, expectedFilePath)
		}
	})
}

func TestSnapshotStorage_MetaStatus(t *testing.T) {
	var testClass string
	var testId string
	testDir := makeTestDir(t, testdataMainDir)
	snapshotsRelativePath := filepath.Join(snapshotsMainDir, "some", "nested", "dir") // ./snapshots/some/nested/dir
	snapshotsAbsolutePath, _ := filepath.Abs(snapshotsRelativePath)
	defer removeDir(t, testdataMainDir)
	defer removeDir(t, snapshotsMainDir)

	t.Run("store snapshot", func(t *testing.T) {
		snapshot := createSnapshotInstance(t, testDir)
		testClass = snapshot.ClassName
		testId = snapshot.ID
		ctxSnapshot := context.Background()

		module := New()
		module.initSnapshotStorage(context.Background(), snapshotsAbsolutePath)
		module.logger, _ = test.NewNullLogger()
		module.dataPath, _ = os.Getwd()
		err := module.StoreSnapshot(ctxSnapshot, snapshot)
		assert.Nil(t, err)
	})

	t.Run("set snapshot status", func(t *testing.T) {
		module := New()
		module.snapshotsPath = snapshotsAbsolutePath

		err := module.SetMetaStatus(context.Background(), testClass, testId, string(snapshots.StatusStarted))
		assert.Nil(t, err)
	})

	t.Run("get snapshot status", func(t *testing.T) {
		module := New()
		module.snapshotsPath = snapshotsAbsolutePath

		status, err := module.GetMetaStatus(context.Background(), testClass, testId)
		assert.Nil(t, err)
		assert.Equal(t, string(snapshots.StatusStarted), status)
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

func createSnapshotInstance(t *testing.T, dirPath string) *snapshots.Snapshot {
	startedAt := time.Now()

	filePaths := createTestFiles(t, dirPath)

	snap := snapshots.New("classname", "snapshot_id", startedAt)
	snap.Files = filePaths
	snap.CompletedAt = time.Now()
	return snap
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
