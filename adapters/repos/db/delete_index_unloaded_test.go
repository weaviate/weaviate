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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
)

// panickingOffloader fails any test that reaches the cloud.
type panickingOffloader struct {
	modulecapabilities.OffloadCloud
}

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

			_, err := db.DropOrphanedClass(schema.ClassName(class))
			require.NoError(t, err)

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

	_, err := db.DropOrphanedClass(schema.ClassName(class))
	require.Error(t, err, "a drop that left the files in place must not report success")
}

// TestDropOrphanedClassLeavesADirectoryThatIsNotAnIndex pins the one thing the
// caller cannot know: whether the path is only ours.
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

			_, err := db.DropOrphanedClass(schema.ClassName(tt.class))
			require.NoError(t, err)

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

// indexID lowercases, so orphan "Foo_Bar" resolves to live "Foo_bar".
func TestDropOrphanedClassNeverTouchesALiveClassOfDifferentCase(t *testing.T) {
	root := t.TempDir()
	logger, _ := test.NewNullLogger()

	const live, orphan = "Foo_bar", "Foo_Bar"
	liveIdx := &Index{Config: IndexConfig{ClassName: schema.ClassName(live)}}
	db := &DB{
		config:  Config{RootPath: root},
		logger:  logger,
		indices: map[string]*Index{indexID(schema.ClassName(live)): liveIdx},
	}
	dir := filepath.Join(root, indexID(schema.ClassName(live)))
	writeShardSignature(t, dir)

	collided, err := db.DropOrphanedClass(schema.ClassName(orphan))
	require.NoError(t, err)
	require.True(t, collided)

	require.Same(t, liveIdx, db.GetIndex(schema.ClassName(live)), "live index was unloaded")
	require.Never(t, func() bool {
		_, err := os.Stat(dir)
		return os.IsNotExist(err)
	}, time.Second, 50*time.Millisecond, "live class directory %s was removed", dir)
}

// Offload paths are lowercased too.
func TestMigratorDropOrphanedClassSkipsCloudForALiveClassOfDifferentCase(t *testing.T) {
	logger, _ := test.NewNullLogger()
	const live, orphan = "Foo_bar", "Foo_Bar"
	db := &DB{
		config: Config{RootPath: t.TempDir()},
		logger: logger,
		indices: map[string]*Index{
			indexID(schema.ClassName(live)): {Config: IndexConfig{ClassName: schema.ClassName(live)}},
		},
	}
	m := &Migrator{db: db, cloud: panickingOffloader{}}

	require.NoError(t, m.DropOrphanedClass(context.Background(), orphan, true))
}

// GetIndex retries a miss for ~150ms, which every real orphan hits: one lookup
// per orphan, or a sweep over thousands of them stalls startup.
func TestMigratorDropOrphanedClassLooksUpTheIndexOnce(t *testing.T) {
	logger, _ := test.NewNullLogger()
	db := &DB{config: Config{RootPath: t.TempDir()}, logger: logger, indices: map[string]*Index{}}
	m := &Migrator{db: db}

	start := time.Now()
	for _, class := range []string{"OrphanA", "OrphanB", "OrphanC"} {
		require.NoError(t, m.DropOrphanedClass(context.Background(), class, false))
	}
	require.Less(t, time.Since(start), 900*time.Millisecond)
}

// DELETE_CLASS validates nothing, so a delete naming another spelling of a live
// class reaches the store while the schema keeps the live class.
func TestDropClassNeverTouchesALiveClassOfDifferentCase(t *testing.T) {
	root := t.TempDir()
	logger, _ := test.NewNullLogger()

	const live, other = "Foo_bar", "Foo_Bar"
	liveIdx := &Index{Config: IndexConfig{ClassName: schema.ClassName(live)}}
	db := &DB{
		config:  Config{RootPath: root},
		logger:  logger,
		indices: map[string]*Index{indexID(schema.ClassName(live)): liveIdx},
	}
	dir := filepath.Join(root, indexID(schema.ClassName(live)))
	writeShardSignature(t, dir)
	m := &Migrator{db: db, cloud: panickingOffloader{}, classLocks: esync.NewKeyLocker()}

	require.NoError(t, m.DropClass(context.Background(), other, true))

	require.Same(t, liveIdx, db.GetIndex(schema.ClassName(live)), "live index was unloaded")
	_, err := os.Stat(dir)
	require.NoError(t, err, "live class directory was removed")
}
