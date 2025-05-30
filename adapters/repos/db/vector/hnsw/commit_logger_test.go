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
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type MockDirEntry struct {
	name  string
	isDir bool
}

func (d MockDirEntry) Name() string {
	return d.name
}

func (d MockDirEntry) IsDir() bool {
	return d.isDir
}

func (d MockDirEntry) Type() os.FileMode {
	return os.ModePerm
}

func (d MockDirEntry) Info() (os.FileInfo, error) {
	return nil, nil
}

func TestRemoveTmpScratchOrHiddenFiles(t *testing.T) {
	entries := []os.DirEntry{
		MockDirEntry{name: "1682473161", isDir: false},
		MockDirEntry{name: ".nfs6b46801cd962afbc00000005", isDir: false},
		MockDirEntry{name: ".mystery-folder", isDir: false},
		MockDirEntry{name: "1682473161.condensed", isDir: false},
		MockDirEntry{name: "1682473161.scratch.tmp", isDir: false},
	}

	expected := []os.DirEntry{
		MockDirEntry{name: "1682473161", isDir: false},
		MockDirEntry{name: "1682473161.condensed", isDir: false},
	}

	result := skipTmpScratchOrHiddenFiles(entries)

	if len(result) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(result))
	}

	for i, entry := range result {
		if entry.Name() != expected[i].Name() {
			t.Errorf("Expected entry %d to be %s, got %s", i, expected[i].Name(), entry.Name())
		}
	}
}

func TestCondenseLoop(t *testing.T) {
	scratchDir := t.TempDir()
	commitLogDir := createCondensorTestData(t, scratchDir)
	createTestCommitLoggerWithOptions(t, scratchDir, "main", WithCondensor(&fakeCondensor{}))

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		files, err := os.ReadDir(commitLogDir)
		assert.Nil(t, err)

		// all existing files should be condensed, but there is of course also an
		// active log, so we expect 2 files in total
		assert.Len(t, files, 2)

		fileNames := make([]string, 0, len(files))
		for _, file := range files {
			fileNames = append(fileNames, file.Name())
		}

		assert.ElementsMatch(t, []string{"1003.condensed", "1004"}, fileNames)
	}, 5*time.Second, 50*time.Millisecond, "Condense loop did not run")
}

func TestCondenseLoop_WithAllocChecker(t *testing.T) {
	scratchDir := t.TempDir()
	commitLogDir := createCondensorTestData(t, scratchDir)
	createTestCommitLoggerWithOptions(t, scratchDir, "main",
		WithCondensor(&fakeCondensor{}), WithAllocChecker(&fakeAllocChecker{}))

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		files, err := os.ReadDir(commitLogDir)
		assert.Nil(t, err)

		// all existing files should be condensed, but there is of course also an
		// active log, so we expect 2 files in total
		assert.Len(t, files, 2)

		fileNames := make([]string, 0, len(files))
		for _, file := range files {
			fileNames = append(fileNames, file.Name())
		}

		assert.ElementsMatch(t, []string{"1003.condensed", "1004"}, fileNames)
	}, 5*time.Second, 50*time.Millisecond, "Condense loop did not run")
}

func TestCondenseLoop_WithAllocChecker_OOM(t *testing.T) {
	scratchDir := t.TempDir()
	commitLogDir := createCondensorTestData(t, scratchDir)
	createTestCommitLoggerWithOptions(t, scratchDir, "main",
		WithCondensor(&fakeCondensor{}), WithAllocChecker(&fakeAllocChecker{shouldErr: true}))

	// Wait 6 commit log cycles (50 ms)
	time.Sleep(300 * time.Millisecond)

	// Ensure that files 1002, 1003, and 1004 still exist and have not been
	// condensed due to the OOM checker, we ignore 1001.condensed and 1002.condensed
	// as combining can still occur when OOM
	files, err := os.ReadDir(commitLogDir)
	assert.Nil(t, err)
	assert.Len(t, files, 4)

	fileNames := make([]string, len(files))
	for i, file := range files {
		fileNames[i] = file.Name()
	}

	for _, expected := range []string{"1002", "1003", "1004"} {
		assert.Contains(t, fileNames, expected)
	}
}

type fakeCondensor struct{}

func (f fakeCondensor) Do(fileName string) error {
	os.Rename(fileName, fmt.Sprintf("%s.condensed", fileName))
	return nil
}

func createCondensorTestData(t *testing.T, scratchDir string) string {
	commitLogDir := fmt.Sprintf("%s/main.hnsw.commitlog.d", scratchDir)

	os.MkdirAll(commitLogDir, os.ModePerm)

	// create dummy data
	_, err := os.Create(fmt.Sprintf("%s/1000.condensed", commitLogDir))
	require.Nil(t, err)
	_, err = os.Create(fmt.Sprintf("%s/1001.condensed", commitLogDir))
	require.Nil(t, err)
	_, err = os.Create(fmt.Sprintf("%s/1002", commitLogDir))
	require.Nil(t, err)
	_, err = os.Create(fmt.Sprintf("%s/1003", commitLogDir))
	require.Nil(t, err)
	_, err = os.Create(fmt.Sprintf("%s/1004", commitLogDir)) // active log
	require.Nil(t, err)

	return commitLogDir
}

func createTestCommitLoggerWithOptions(t *testing.T, scratchDir string, name string, options ...CommitlogOption) *hnswCommitLogger {
	logger, _ := test.NewNullLogger()
	cbg := cyclemanager.NewCallbackGroup("test", logger, 10)
	ticker := cyclemanager.NewLinearTicker(50*time.Millisecond, 60*time.Millisecond, 1)
	cm := cyclemanager.NewManager(ticker, cbg.CycleCallback, logger)
	cl, err := NewCommitLogger(scratchDir, name, logger, cbg, options...)
	require.Nil(t, err)
	cl.InitMaintenance()
	cm.Start()

	t.Cleanup(func() {
		cl.Shutdown(context.Background())
		cm.Stop(context.Background())
	})

	return cl
}

type fakeAllocChecker struct {
	shouldErr bool
}

func (f fakeAllocChecker) CheckAlloc(sizeInBytes int64) error {
	if f.shouldErr {
		return fmt.Errorf("can't allocate %d bytes", sizeInBytes)
	}
	return nil
}

func (f fakeAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}
func (f fakeAllocChecker) Refresh(updateMappings bool) {}
