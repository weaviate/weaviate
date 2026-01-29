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

package compactv2

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func migratorTestLogger() logrus.FieldLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

func TestSnapshotReaderV1V2Rejection(t *testing.T) {
	// Test that V1 and V2 snapshots are rejected with a clear error message
	testCases := []struct {
		name    string
		version uint8
	}{
		{"V1 snapshot", snapshotVersionV1},
		{"V2 snapshot", snapshotVersionV2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			snapshotPath := filepath.Join(dir, "1000.snapshot")

			// Create a minimal snapshot with the specified version
			f, err := os.Create(snapshotPath)
			require.NoError(t, err)

			// Write just the version byte
			_, err = f.Write([]byte{tc.version})
			require.NoError(t, err)
			f.Close()

			// Try to read it
			reader := NewSnapshotReader(migratorTestLogger())
			_, err = reader.ReadFromFile(snapshotPath)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "legacy snapshot version")
			assert.Contains(t, err.Error(), "upgrade not supported")
			assert.Contains(t, err.Error(), "downgrade Weaviate")
		})
	}
}

func TestSnapshotReaderUnknownVersionRejection(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "1000.snapshot")

	// Create a minimal snapshot with an unknown version
	f, err := os.Create(snapshotPath)
	require.NoError(t, err)

	// Write an unknown version byte
	_, err = f.Write([]byte{99})
	require.NoError(t, err)
	f.Close()

	// Try to read it
	reader := NewSnapshotReader(migratorTestLogger())
	_, err = reader.ReadFromFile(snapshotPath)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported snapshot version 99")
}

func TestMigratorIsMigrationComplete(t *testing.T) {
	dir := t.TempDir()
	migrator := NewMigrator(dir, migratorTestLogger())

	// Initially not complete
	assert.False(t, migrator.IsMigrationComplete())

	// Create sentinel file
	err := migrator.MarkMigrationComplete()
	require.NoError(t, err)

	// Now complete
	assert.True(t, migrator.IsMigrationComplete())
}

func TestMigratorNeedsMigration(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		dir := t.TempDir()
		migrator := NewMigrator(dir, migratorTestLogger())

		needs, err := migrator.NeedsMigration()
		require.NoError(t, err)
		assert.False(t, needs)
	})

	t.Run("no condensed files", func(t *testing.T) {
		dir := t.TempDir()

		// Create a sorted file
		sortedPath := filepath.Join(dir, "1000.sorted")
		createTestWALFile(t, sortedPath, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(0, 0))
		})

		migrator := NewMigrator(dir, migratorTestLogger())

		needs, err := migrator.NeedsMigration()
		require.NoError(t, err)
		assert.False(t, needs)
	})

	t.Run("has condensed files", func(t *testing.T) {
		dir := t.TempDir()

		// Create a condensed file
		condensedPath := filepath.Join(dir, "1000.condensed")
		createTestWALFile(t, condensedPath, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(0, 0))
		})

		migrator := NewMigrator(dir, migratorTestLogger())

		needs, err := migrator.NeedsMigration()
		require.NoError(t, err)
		assert.True(t, needs)
	})

	t.Run("migration already complete", func(t *testing.T) {
		dir := t.TempDir()

		// Create a condensed file
		condensedPath := filepath.Join(dir, "1000.condensed")
		createTestWALFile(t, condensedPath, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(0, 0))
		})

		migrator := NewMigrator(dir, migratorTestLogger())

		// Mark complete
		err := migrator.MarkMigrationComplete()
		require.NoError(t, err)

		// Even though condensed exists, migration is already complete
		needs, err := migrator.NeedsMigration()
		require.NoError(t, err)
		assert.False(t, needs)
	})
}

func TestMigratorCondensedConversion(t *testing.T) {
	dir := t.TempDir()

	// Create a condensed file with some data
	condensedPath := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 1))
		require.NoError(t, w.WriteAddNode(0, 1))
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 0, 1))
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(0, 1, 1))
	})

	migrator := NewMigrator(dir, migratorTestLogger())

	// Run migration
	err := migrator.Migrate(context.Background())
	require.NoError(t, err)

	// Verify condensed file is gone
	_, err = os.Stat(condensedPath)
	assert.True(t, os.IsNotExist(err), "condensed file should be deleted")

	// Verify sorted file exists
	sortedPath := filepath.Join(dir, "1000.sorted")
	_, err = os.Stat(sortedPath)
	require.NoError(t, err, "sorted file should exist")

	// Verify sentinel file exists
	assert.True(t, migrator.IsMigrationComplete())

	// Verify sorted file is valid by loading it
	loader := NewLoader(LoaderConfig{
		Dir:    dir,
		Logger: migratorTestLogger(),
	})

	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, uint64(0), result.Graph.Entrypoint)
	assert.Equal(t, uint16(1), result.Graph.Level)
	require.True(t, len(result.Graph.Nodes) > 1)
	require.NotNil(t, result.Graph.Nodes[0])
	require.NotNil(t, result.Graph.Nodes[1])
}

func TestMigratorMultipleCondensedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create multiple condensed files
	condensed1Path := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensed1Path, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	condensed2Path := filepath.Join(dir, "2000.condensed")
	createTestWALFile(t, condensed2Path, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	condensed3Path := filepath.Join(dir, "3000_4000.condensed")
	createTestWALFile(t, condensed3Path, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(2, 0))
	})

	migrator := NewMigrator(dir, migratorTestLogger())

	// Run migration
	err := migrator.Migrate(context.Background())
	require.NoError(t, err)

	// Verify all condensed files are gone
	for _, path := range []string{condensed1Path, condensed2Path, condensed3Path} {
		_, err = os.Stat(path)
		assert.True(t, os.IsNotExist(err), "condensed file should be deleted: %s", path)
	}

	// Verify sorted files exist
	for _, filename := range []string{"1000.sorted", "2000.sorted", "3000_4000.sorted"} {
		sortedPath := filepath.Join(dir, filename)
		_, err = os.Stat(sortedPath)
		require.NoError(t, err, "sorted file should exist: %s", filename)
	}
}

func TestMigratorStateRecovery(t *testing.T) {
	dir := t.TempDir()

	// Create condensed files
	condensed1Path := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensed1Path, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	condensed2Path := filepath.Join(dir, "2000.condensed")
	createTestWALFile(t, condensed2Path, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(1, 0))
	})

	migrator := NewMigrator(dir, migratorTestLogger())

	// Simulate partial migration by manually creating state
	state := &MigrationState{
		CondensedConverted: []string{condensed1Path},
		CondensedPending:   []string{condensed1Path, condensed2Path},
	}
	err := migrator.SaveState(state)
	require.NoError(t, err)

	// Manually convert first file and delete it (simulating completed conversion)
	sorted1Path := filepath.Join(dir, "1000.sorted")
	createTestWALFile(t, sorted1Path, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})
	os.Remove(condensed1Path)

	// Run migration - should resume and only convert the second file
	err = migrator.Migrate(context.Background())
	require.NoError(t, err)

	// Verify second condensed file is gone
	_, err = os.Stat(condensed2Path)
	assert.True(t, os.IsNotExist(err))

	// Verify both sorted files exist
	_, err = os.Stat(sorted1Path)
	require.NoError(t, err)

	sorted2Path := filepath.Join(dir, "2000.sorted")
	_, err = os.Stat(sorted2Path)
	require.NoError(t, err)
}

func TestMigratorIdempotent(t *testing.T) {
	dir := t.TempDir()

	// Create a condensed file
	condensedPath := filepath.Join(dir, "1000.condensed")
	createTestWALFile(t, condensedPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	migrator := NewMigrator(dir, migratorTestLogger())

	// Run migration first time
	err := migrator.Migrate(context.Background())
	require.NoError(t, err)

	sortedPath := filepath.Join(dir, "1000.sorted")
	stat1, err := os.Stat(sortedPath)
	require.NoError(t, err)

	// Run migration second time (should be no-op)
	err = migrator.Migrate(context.Background())
	require.NoError(t, err)

	stat2, err := os.Stat(sortedPath)
	require.NoError(t, err)

	// File should not have been modified
	assert.Equal(t, stat1.ModTime(), stat2.ModTime())
}

func TestMigrateSnapshotDirectory(t *testing.T) {
	t.Run("no old directory", func(t *testing.T) {
		// Create commitlog directory
		baseDir := t.TempDir()
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		migrator := NewMigrator(commitlogDir, migratorTestLogger())

		// Should complete without error when old dir doesn't exist
		err := migrator.MigrateSnapshotDirectory()
		require.NoError(t, err)
	})

	t.Run("V3 snapshot moves successfully", func(t *testing.T) {
		baseDir := t.TempDir()

		// Create old snapshot directory
		snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")
		require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

		// Create V3 snapshot in old directory
		oldSnapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
		createTestSnapshot(t, oldSnapshotPath, 0, 0, []testNode{
			{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
		})

		// Create commitlog directory
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		migrator := NewMigrator(commitlogDir, migratorTestLogger())

		// Run migration
		err := migrator.MigrateSnapshotDirectory()
		require.NoError(t, err)

		// Verify snapshot moved to new location
		newSnapshotPath := filepath.Join(commitlogDir, "1000.snapshot")
		_, err = os.Stat(newSnapshotPath)
		require.NoError(t, err, "snapshot should be in new location")

		// Verify old snapshot is gone
		_, err = os.Stat(oldSnapshotPath)
		assert.True(t, os.IsNotExist(err), "old snapshot should be removed")
	})

	t.Run("V1 snapshot rejects with error", func(t *testing.T) {
		baseDir := t.TempDir()

		// Create old snapshot directory
		snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")
		require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

		// Create V1 snapshot in old directory
		oldSnapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
		f, err := os.Create(oldSnapshotPath)
		require.NoError(t, err)
		_, err = f.Write([]byte{snapshotVersionV1})
		require.NoError(t, err)
		f.Close()

		// Create commitlog directory
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		migrator := NewMigrator(commitlogDir, migratorTestLogger())

		// Run migration - should fail
		err = migrator.MigrateSnapshotDirectory()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "legacy snapshot version 1")
		assert.Contains(t, err.Error(), "upgrade not supported")
	})

	t.Run("V2 snapshot rejects with error", func(t *testing.T) {
		baseDir := t.TempDir()

		// Create old snapshot directory
		snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")
		require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

		// Create V2 snapshot in old directory
		oldSnapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
		f, err := os.Create(oldSnapshotPath)
		require.NoError(t, err)
		_, err = f.Write([]byte{snapshotVersionV2})
		require.NoError(t, err)
		f.Close()

		// Create commitlog directory
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		migrator := NewMigrator(commitlogDir, migratorTestLogger())

		// Run migration - should fail
		err = migrator.MigrateSnapshotDirectory()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "legacy snapshot version 2")
		assert.Contains(t, err.Error(), "upgrade not supported")
	})

	t.Run("checkpoint files are deleted", func(t *testing.T) {
		baseDir := t.TempDir()

		// Create old snapshot directory
		snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")
		require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

		// Create V3 snapshot in old directory
		oldSnapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
		createTestSnapshot(t, oldSnapshotPath, 0, 0, []testNode{
			{id: 0, level: 0, connections: [][]uint64{{}}, tombstone: false},
		})

		// Create checkpoint file
		checkpointPath := filepath.Join(snapshotDir, "1000.snapshot.checkpoints")
		require.NoError(t, os.WriteFile(checkpointPath, []byte("checkpoint data"), 0o644))

		// Create commitlog directory
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		migrator := NewMigrator(commitlogDir, migratorTestLogger())

		// Run migration
		err := migrator.MigrateSnapshotDirectory()
		require.NoError(t, err)

		// Verify checkpoint file is deleted
		_, err = os.Stat(checkpointPath)
		assert.True(t, os.IsNotExist(err), "checkpoint file should be deleted")
	})

	t.Run("non-matching directory name skipped", func(t *testing.T) {
		// Create a directory that doesn't end with .hnsw.commitlog.d
		dir := t.TempDir()

		migrator := NewMigrator(dir, migratorTestLogger())

		// Should complete without error
		err := migrator.MigrateSnapshotDirectory()
		require.NoError(t, err)
	})
}

func TestCleanupMigratingFiles(t *testing.T) {
	dir := t.TempDir()

	// Create orphaned migrating file
	migratingPath := filepath.Join(dir, "1000.snapshot.migrating")
	require.NoError(t, os.WriteFile(migratingPath, []byte("orphaned data"), 0o644))

	// Create a valid file
	validPath := filepath.Join(dir, "1000.sorted")
	createTestWALFile(t, validPath, func(w *WALWriter) {
		require.NoError(t, w.WriteAddNode(0, 0))
	})

	// Run cleanup
	err := CleanupMigratingFiles(dir)
	require.NoError(t, err)

	// Verify migrating file is gone
	_, err = os.Stat(migratingPath)
	assert.True(t, os.IsNotExist(err), "migrating file should be removed")

	// Verify valid file still exists
	_, err = os.Stat(validPath)
	require.NoError(t, err, "valid file should still exist")
}

func TestMigratorLoadStateSaveState(t *testing.T) {
	dir := t.TempDir()
	migrator := NewMigrator(dir, migratorTestLogger())

	// Initially no state
	state, err := migrator.LoadState()
	require.NoError(t, err)
	assert.Nil(t, state)

	// Save state
	state = &MigrationState{
		CondensedConverted: []string{"/path/a.condensed"},
		CondensedPending:   []string{"/path/a.condensed", "/path/b.condensed"},
	}
	err = migrator.SaveState(state)
	require.NoError(t, err)

	// Load state
	loaded, err := migrator.LoadState()
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, state.CondensedConverted, loaded.CondensedConverted)
	assert.Equal(t, state.CondensedPending, loaded.CondensedPending)
}

func TestLoaderIntegrationWithMigrator(t *testing.T) {
	t.Run("loader calls MigrateSnapshotDirectory", func(t *testing.T) {
		baseDir := t.TempDir()

		// Create old snapshot directory with V3 snapshot
		snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")
		require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

		oldSnapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
		createTestSnapshot(t, oldSnapshotPath, 42, 2, []testNode{
			{id: 0, level: 1, connections: [][]uint64{{1}, {}}, tombstone: false},
			{id: 1, level: 0, connections: [][]uint64{{0}}, tombstone: false},
		})

		// Create commitlog directory
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		// Load should migrate snapshot and load it
		loader := NewLoader(LoaderConfig{
			Dir:    commitlogDir,
			Logger: migratorTestLogger(),
		})

		result, err := loader.Load()
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify snapshot was loaded correctly
		assert.Equal(t, uint64(42), result.Graph.Entrypoint)
		assert.Equal(t, uint16(2), result.Graph.Level)
	})

	t.Run("loader fails on V1 snapshot", func(t *testing.T) {
		baseDir := t.TempDir()

		// Create old snapshot directory with V1 snapshot
		snapshotDir := filepath.Join(baseDir, "main.hnsw.snapshot.d")
		require.NoError(t, os.MkdirAll(snapshotDir, 0o755))

		oldSnapshotPath := filepath.Join(snapshotDir, "1000.snapshot")
		f, err := os.Create(oldSnapshotPath)
		require.NoError(t, err)
		_, err = f.Write([]byte{snapshotVersionV1})
		require.NoError(t, err)
		f.Close()

		// Create commitlog directory
		commitlogDir := filepath.Join(baseDir, "main.hnsw.commitlog.d")
		require.NoError(t, os.MkdirAll(commitlogDir, 0o755))

		// Load should fail with clear error
		loader := NewLoader(LoaderConfig{
			Dir:    commitlogDir,
			Logger: migratorTestLogger(),
		})

		_, err = loader.Load()
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "legacy snapshot version") ||
			strings.Contains(err.Error(), "migrate snapshot directory"))
	})
}

func TestMigratorContextCancellation(t *testing.T) {
	dir := t.TempDir()

	// Create multiple condensed files
	for i := 0; i < 5; i++ {
		condensedPath := filepath.Join(dir, filepath.Base(BuildMergedFilename(int64(i*1000), int64(i*1000), FileTypeCondensed)))
		createTestWALFile(t, condensedPath, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(uint64(i), 0))
		})
	}

	migrator := NewMigrator(dir, migratorTestLogger())

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Run migration with cancelled context
	err := migrator.Migrate(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}
