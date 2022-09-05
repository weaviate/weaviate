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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testdataMainDir  = "./testData"
	snapshotsMainDir = "./snapshots"
)

func TestSnapshotStorage_StoreSnapshot(t *testing.T) {
	snapshotsRelativePath := filepath.Join(snapshotsMainDir, "some", "nested", "dir") // ./snapshots/some/nested/dir
	snapshotsAbsolutePath, _ := filepath.Abs(snapshotsRelativePath)
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
