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

package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// TestDropOrphanedClassRemovesDataWithNoLoadedIndex covers the state both orphan
// issues leave behind: data on disk with no *Index, because the class was gone
// from the schema by the time the reload ran.
func TestDropOrphanedClassRemovesDataWithNoLoadedIndex(t *testing.T) {
	tests := []struct {
		name       string
		onDisk     bool
		wantRemove bool
	}{
		{name: "unopened class directory is removed", onDisk: true, wantRemove: true},
		{name: "missing directory is a no-op", onDisk: false, wantRemove: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			logger, _ := test.NewNullLogger()
			db := &DB{config: Config{RootPath: root}, logger: logger, indices: map[string]*Index{}}

			const class = "OrphanClass"
			dir := filepath.Join(root, indexID(schema.ClassName(class)))
			if tt.onDisk {
				writeShardSignature(t, dir)
			}

			require.NoError(t, db.DropOrphanedClass(schema.ClassName(class)))

			if !tt.wantRemove {
				return
			}
			require.Eventually(t, func() bool {
				_, err := os.Stat(dir)
				return os.IsNotExist(err)
			}, 10*time.Second, 20*time.Millisecond, "class directory survived DeleteIndex")
		})
	}
}

// TestDeleteIndexNeverRemovesFilesWithoutAnIndex pins the blast radius of
// deleting a class that never existed: DeleteClass does not check, so the store
// is asked to delete whatever a caller names.
func TestDeleteIndexNeverRemovesFilesWithoutAnIndex(t *testing.T) {
	for _, class := range []string{"Raft", "Backups", "NeverExisted"} {
		t.Run(class, func(t *testing.T) {
			root := t.TempDir()
			logger, _ := test.NewNullLogger()
			db := &DB{config: Config{RootPath: root}, logger: logger, indices: map[string]*Index{}}

			dir := filepath.Join(root, indexID(schema.ClassName(class)))
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard1", "lsm"), 0o755))

			require.NoError(t, db.DeleteIndex(schema.ClassName(class)))

			require.Never(t, func() bool {
				_, err := os.Stat(dir)
				return os.IsNotExist(err)
			}, time.Second, 50*time.Millisecond,
				"DeleteIndex removed %s with no index loaded", dir)
		})
	}
}

// TestDropOrphanedClassReportsAFailedDrop pins the retry contract: a drop that
// leaves the files in place must not report success, or dropOrphanedClasses
// discards the pending entry for good.
func TestDropOrphanedClassReportsAFailedDrop(t *testing.T) {
	root := t.TempDir()
	logger, _ := test.NewNullLogger()
	db := &DB{config: Config{RootPath: root}, logger: logger, indices: map[string]*Index{}}

	const class = "OrphanClass"
	dir := filepath.Join(root, indexID(schema.ClassName(class)))
	writeShardSignature(t, dir)

	// A read-only data root makes the rename fail the way a full or
	// permission-denied volume would.
	require.NoError(t, os.Chmod(root, 0o555))
	t.Cleanup(func() { os.Chmod(root, 0o755) })

	err := db.DropOrphanedClass(schema.ClassName(class))
	require.Error(t, err, "a drop that left the files in place must not report success")
}

// TestDropOrphanedClassLeavesADirectoryThatIsNotAnIndex pins the one thing the
// caller cannot know: whether the path is only ours. The class really was in
// the schema and really was held here, so only the directory settles it.
func TestDropOrphanedClassLeavesADirectoryThatIsNotAnIndex(t *testing.T) {
	tests := []struct {
		name    string
		class   string
		build   func(t *testing.T, dir string)
		removed bool
	}{
		{
			name:  "filesystem backup tree colocated under the data root",
			class: "Backups",
			build: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(
					filepath.Join(dir, "backup-1", "node1", "somecollection"), 0o755))
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, "backup-1", "node1", "somecollection", "chunk-1"),
					[]byte("x"), 0o644))
			},
		},
		{
			name:  "directory with no shard in it",
			class: "Empty",
			build: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(dir, 0o755))
			},
		},
		{
			name:    "a real index directory is still removed",
			class:   "OrphanClass",
			build:   func(t *testing.T, dir string) { writeShardSignature(t, dir) },
			removed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			logger, _ := test.NewNullLogger()
			db := &DB{config: Config{RootPath: root}, logger: logger, indices: map[string]*Index{}}

			dir := filepath.Join(root, indexID(schema.ClassName(tt.class)))
			tt.build(t, dir)

			require.NoError(t, db.DropOrphanedClass(schema.ClassName(tt.class)))

			if tt.removed {
				require.Eventually(t, func() bool {
					_, err := os.Stat(dir)
					return os.IsNotExist(err)
				}, 10*time.Second, 20*time.Millisecond, "index directory must be removed")
				return
			}
			require.Never(t, func() bool {
				_, err := os.Stat(dir)
				return os.IsNotExist(err)
			}, time.Second, 50*time.Millisecond, "removed %s, which is not an index", dir)
		})
	}
}

// writeShardSignature creates what hasShardStore looks for: the lsm store and
// the version file.
func writeShardSignature(t *testing.T, indexDir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(indexDir, "shard1", "lsm"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(indexDir, "shard1", "version"), []byte{1, 0}, 0o644))
}
