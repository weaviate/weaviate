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
	"errors"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	tlog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
)

func TestBackupMutex(t *testing.T) {
	l, _ := tlog.NewNullLogger()
	t.Run("success first time", func(t *testing.T) {
		m := shardTransfer{log: l, retryDuration: time.Millisecond, notifyDuration: 5 * time.Millisecond}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Millisecond)
		defer cancel()
		if err := m.LockWithContext(ctx); err != nil {
			t.Errorf("error want:nil got:%v ", err)
		}
	})
	t.Run("success after retry", func(t *testing.T) {
		m := shardTransfer{log: l, retryDuration: 2 * time.Millisecond, notifyDuration: 5 * time.Millisecond}
		m.RLock()
		go func() {
			defer m.RUnlock()
			time.Sleep(time.Millisecond * 15)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.LockWithContext(ctx); err != nil {
			t.Errorf("error want:nil got:%v ", err)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		m := shardTransfer{log: l, retryDuration: time.Millisecond, notifyDuration: 5 * time.Millisecond}
		m.RLock()
		defer m.RUnlock()
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Millisecond)
		defer cancel()
		err := m.LockWithContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error want:%v got:%v", err, context.DeadlineExceeded)
		}
	})
}

func TestListInactiveLSMFiles(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, lsmDir string)
		expected []string
	}{
		{
			name: "bucket with segment and wal files",
			setup: func(t *testing.T, lsmDir string) {
				bucketDir := filepath.Join(lsmDir, "objects")
				require.NoError(t, os.MkdirAll(bucketDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-0001.db"), []byte("data"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-0002.db"), []byte("data"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "active.wal"), []byte("wal"), 0o644))
			},
			expected: []string{
				"objects/active.wal",
				"objects/segment-0001.db",
				"objects/segment-0002.db",
			},
		},
		{
			name: "tmp files are excluded",
			setup: func(t *testing.T, lsmDir string) {
				bucketDir := filepath.Join(lsmDir, "mybucket")
				require.NoError(t, os.MkdirAll(bucketDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment.db"), []byte("data"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "compaction.tmp"), []byte("tmp"), 0o644))
			},
			expected: []string{
				"mybucket/segment.db",
			},
		},
		{
			name: "scratch directories inside buckets are skipped",
			setup: func(t *testing.T, lsmDir string) {
				bucketDir := filepath.Join(lsmDir, "objects")
				require.NoError(t, os.MkdirAll(filepath.Join(bucketDir, "scratch"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "scratch", "temp.db"), []byte("x"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment.db"), []byte("data"), 0o644))
			},
			expected: []string{
				"objects/segment.db",
			},
		},
		{
			name: "migrations directory is walked recursively",
			setup: func(t *testing.T, lsmDir string) {
				migDir := filepath.Join(lsmDir, ".migrations")
				require.NoError(t, os.MkdirAll(filepath.Join(migDir, "sub"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(migDir, "m1.json"), []byte("m"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(migDir, "sub", "m2.json"), []byte("m"), 0o644))

				bucketDir := filepath.Join(lsmDir, "objects")
				require.NoError(t, os.MkdirAll(bucketDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment.db"), []byte("d"), 0o644))
			},
			expected: []string{
				".migrations/m1.json",
				".migrations/sub/m2.json",
				"objects/segment.db",
			},
		},
		{
			name: "multiple buckets",
			setup: func(t *testing.T, lsmDir string) {
				for _, name := range []string{"objects", "inverted_idx"} {
					dir := filepath.Join(lsmDir, name)
					require.NoError(t, os.MkdirAll(dir, 0o755))
					require.NoError(t, os.WriteFile(filepath.Join(dir, "seg.db"), []byte("d"), 0o644))
				}
			},
			expected: []string{
				"inverted_idx/seg.db",
				"objects/seg.db",
			},
		},
		{
			name:     "nonexistent lsm dir returns nil",
			setup:    func(t *testing.T, lsmDir string) {},
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rootDir := t.TempDir()
			lsmDir := filepath.Join(rootDir, "lsm")

			tc.setup(t, lsmDir)

			got, err := listInactiveLSMFiles(lsmDir, rootDir)
			require.NoError(t, err)

			// Normalize to sorted relative paths for comparison.
			// listInactiveLSMFiles returns paths relative to rootDir; strip the "lsm/" prefix
			// isn't needed because rootDir is the parent of lsmDir, so paths are "lsm/bucket/file".
			// Actually: rootPath = rootDir, lsmDir = rootDir/lsm, bucketDir = rootDir/lsm/bucket
			// basePath = filepath.Rel(rootDir, bucketDir) = "lsm/bucket"
			// file = basePath + "/" + filename = "lsm/bucket/filename"

			var expected []string
			for _, p := range tc.expected {
				expected = append(expected, filepath.Join("lsm", p))
			}
			sort.Strings(expected)
			sort.Strings(got)
			assert.Equal(t, expected, got)
		})
	}
}

func TestListInactiveShardFiles(t *testing.T) {
	// Create a minimal Index-like setup with a temp dir structure mimicking a shard.
	rootDir := t.TempDir()
	indexID := "myclass"
	shardName := "tenant1"
	indexDir := filepath.Join(rootDir, indexID)
	shardDir := filepath.Join(indexDir, shardName)

	// Create shard directory structure
	require.NoError(t, os.MkdirAll(shardDir, 0o755))

	// Metadata files
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), []byte("42"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "proplengths"), []byte(`{"len":1}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "version"), []byte("2"), 0o644))

	// LSM bucket with segment and WAL
	bucketDir := filepath.Join(shardDir, "lsm", "objects")
	require.NoError(t, os.MkdirAll(bucketDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-0001.db"), []byte("seg"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "active.wal"), []byte("wal"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "compaction.tmp"), []byte("tmp"), 0o644))

	// Vector index directory
	vecDir := filepath.Join(shardDir, "vectors_default")
	require.NoError(t, os.MkdirAll(vecDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(vecDir, "commitlog.0001"), []byte("cl"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(vecDir, "main.hnsw"), []byte("idx"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(vecDir, "scratch.tmp"), []byte("tmp"), 0o644))

	// Build a minimal Index to call listInactiveShardFiles.
	// fakeSchemaGetter is defined in fakes_for_tests.go with NodeName() returning "node1".
	idx := &Index{
		Config:    IndexConfig{RootPath: rootDir, ClassName: "MyClass"},
		getSchema: &fakeSchemaGetter{},
	}

	var sd backup.ShardDescriptor
	files, err := idx.listInactiveShardFiles(shardName, &sd)
	require.NoError(t, err)

	// Verify metadata
	assert.Equal(t, shardName, sd.Name)
	assert.Equal(t, "node1", sd.Node)
	assert.Equal(t, []byte("42"), sd.DocIDCounter)
	assert.Equal(t, []byte(`{"len":1}`), sd.PropLengthTracker)
	assert.Equal(t, []byte("2"), sd.Version)

	// Verify relative paths for metadata
	assert.Equal(t, filepath.Join(indexID, shardName, "indexcount"), sd.DocIDCounterPath)
	assert.Equal(t, filepath.Join(indexID, shardName, "proplengths"), sd.PropLengthTrackerPath)
	assert.Equal(t, filepath.Join(indexID, shardName, "version"), sd.ShardVersionPath)

	// Verify file list: should include .wal, exclude .tmp
	sort.Strings(files)
	expected := []string{
		filepath.Join(indexID, shardName, "lsm", "objects", "active.wal"),
		filepath.Join(indexID, shardName, "lsm", "objects", "segment-0001.db"),
		filepath.Join(indexID, shardName, "vectors_default", "commitlog.0001"),
		filepath.Join(indexID, shardName, "vectors_default", "main.hnsw"),
	}
	sort.Strings(expected)
	assert.Equal(t, expected, files)
}

func TestIsMutableFile(t *testing.T) {
	tests := []struct {
		relPath string
		want    bool
		desc    string
	}{
		{"myclass/shard1/lsm/objects/segment-123.wal", true, "LSM WAL"},
		{"myclass/shard1/lsm/objects/segment-0001.db", false, "LSM segment (immutable)"},
		{"myclass/shard1/main/meta.db", true, "flat index BoltDB (single vector)"},
		{"myclass/shard1/main_custom/meta_custom.db", true, "flat index BoltDB (multi-vector)"},
		{"myclass/shard1/main.hnsw.commitlog.d/1709203456", true, "non-condensed HNSW commitlog"},
		{"myclass/shard1/main.hnsw.commitlog.d/1709203456.condensed", false, "condensed HNSW commitlog (immutable)"},
		{"myclass/shard1/main.hnsw.snapshot.d/1709203456.snapshot", false, "HNSW snapshot (immutable)"},
		{"myclass/shard1/lsm/.migrations/m1", false, "migration file (immutable)"},
		{"myclass/shard1/lsm/objects/segment.db.tmp", false, "tmp file (immutable)"},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got := isMutableFile(tc.relPath)
			assert.Equal(t, tc.want, got, "isMutableFile(%q)", tc.relPath)
		})
	}
}

func TestBackupInactiveShardCopyVsHardlink(t *testing.T) {
	rootDir := t.TempDir()
	indexID := "myclass"
	shardName := "tenant1"
	shardDir := filepath.Join(rootDir, indexID, shardName)

	// Metadata files required by listInactiveShardFiles.
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), []byte("42"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "proplengths"), []byte("{}"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "version"), []byte("2"), 0o644))

	// LSM bucket with segment (immutable) and WAL (mutable).
	bucketDir := filepath.Join(shardDir, "lsm", "objects")
	require.NoError(t, os.MkdirAll(bucketDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-0001.db"), []byte("seg-data"), 0o644))
	walContent := []byte("original-wal-data")
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-123.wal"), walContent, 0o644))

	// Flat vector index metadata (mutable).
	vecDir := filepath.Join(shardDir, "main")
	require.NoError(t, os.MkdirAll(vecDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(vecDir, "meta.db"), []byte("boltdb-data"), 0o644))

	// HNSW commitlog directory with condensed (immutable) and non-condensed (mutable).
	clDir := filepath.Join(shardDir, "main.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(clDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(clDir, "1709203456"), []byte("commitlog"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(clDir, "1709203400.condensed"), []byte("condensed"), 0o644))

	// Staging directory.
	stagingRoot := filepath.Join(rootDir, "staging")
	require.NoError(t, os.MkdirAll(stagingRoot, 0o755))

	idx := &Index{
		Config:    IndexConfig{RootPath: rootDir, ClassName: "MyClass"},
		getSchema: &fakeSchemaGetter{},
	}

	var sd backup.ShardDescriptor
	err := idx.backupInactiveShardWithHardlinks(shardName, &sd, nil, stagingRoot)
	require.NoError(t, err)

	// Helper to get inode number.
	getIno := func(path string) uint64 {
		t.Helper()
		info, err := os.Stat(path)
		require.NoError(t, err)
		return info.Sys().(*syscall.Stat_t).Ino
	}

	// Mutable files: different inodes (copied).
	walSrc := filepath.Join(bucketDir, "segment-123.wal")
	walDst := filepath.Join(stagingRoot, indexID, shardName, "lsm", "objects", "segment-123.wal")
	assert.NotEqual(t, getIno(walSrc), getIno(walDst), "WAL should be copied, not hard-linked")

	metaSrc := filepath.Join(vecDir, "meta.db")
	metaDst := filepath.Join(stagingRoot, indexID, shardName, "main", "meta.db")
	assert.NotEqual(t, getIno(metaSrc), getIno(metaDst), "meta.db should be copied, not hard-linked")

	clSrc := filepath.Join(clDir, "1709203456")
	clDst := filepath.Join(stagingRoot, indexID, shardName, "main.hnsw.commitlog.d", "1709203456")
	assert.NotEqual(t, getIno(clSrc), getIno(clDst), "non-condensed commitlog should be copied, not hard-linked")

	// Immutable files: same inodes (hard-linked).
	segSrc := filepath.Join(bucketDir, "segment-0001.db")
	segDst := filepath.Join(stagingRoot, indexID, shardName, "lsm", "objects", "segment-0001.db")
	assert.Equal(t, getIno(segSrc), getIno(segDst), "segment should be hard-linked, not copied")

	condensedSrc := filepath.Join(clDir, "1709203400.condensed")
	condensedDst := filepath.Join(stagingRoot, indexID, shardName, "main.hnsw.commitlog.d", "1709203400.condensed")
	assert.Equal(t, getIno(condensedSrc), getIno(condensedDst), "condensed commitlog should be hard-linked, not copied")

	// Modify source WAL after backup — staging copy should be unaffected.
	require.NoError(t, os.WriteFile(walSrc, []byte("modified-wal-data"), 0o644))
	stagedWAL, err := os.ReadFile(walDst)
	require.NoError(t, err)
	assert.Equal(t, walContent, stagedWAL, "staged WAL copy should not reflect post-backup source modifications")
}

func TestBackupProtectedShardsBlockActivation(t *testing.T) {
	logger, _ := tlog.NewNullLogger()
	rootDir := t.TempDir()
	ctx := context.Background()
	className := "MyClass"

	newTestIndex := func() *Index {
		return &Index{
			Config: IndexConfig{RootPath: rootDir, ClassName: schema.ClassName(className)},
			getSchema: &fakeSchemaGetter{
				schema: schema.Schema{
					Objects: &models.Schema{
						Classes: []*models.Class{{Class: className}},
					},
				},
			},
			logger:           logger,
			backupLock:       esync.NewKeyRWLocker(),
			shardCreateLocks: esync.NewKeyRWLocker(),
			closingCtx:       context.Background(),
		}
	}

	t.Run("initLocalShardWithForcedLoading blocked by protection", func(t *testing.T) {
		idx := newTestIndex()
		shardName := "tenant1"

		idx.backupProtectedShards.Store(shardName, struct{}{})

		class := &models.Class{Class: className}
		err := idx.initLocalShardWithForcedLoading(ctx, class, shardName, true, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "protected for backup")
	})

	t.Run("getOptInitLocalShard blocked by protection", func(t *testing.T) {
		idx := newTestIndex()
		shardName := "tenant2"

		idx.backupProtectedShards.Store(shardName, struct{}{})

		_, release, err := idx.getOptInitLocalShard(ctx, shardName, true)
		defer release()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "protected for backup")
	})

	t.Run("getOptInitLocalShard skips check when ensureInit is false", func(t *testing.T) {
		idx := newTestIndex()
		shardName := "tenant3"

		idx.backupProtectedShards.Store(shardName, struct{}{})

		// With ensureInit=false, the function returns nil shard without error
		// (the protection check is never reached).
		shard, release, err := idx.getOptInitLocalShard(ctx, shardName, false)
		defer release()
		require.NoError(t, err)
		assert.Nil(t, shard)
	})

	t.Run("ReleaseBackup clears protections and releases locks", func(t *testing.T) {
		idx := newTestIndex()
		shards := []string{"shardA", "shardB", "shardC"}

		// Simulate what backupShardWithoutHardlinks does: hold lock + set flag.
		for _, name := range shards {
			idx.backupLock.Lock(name)
			idx.backupProtectedShards.Store(name, struct{}{})
		}
		// Set backup state so ReleaseBackup has something to reset.
		idx.lastBackup.Store(&BackupState{BackupID: "test-backup", InProgress: true})

		err := idx.ReleaseBackup(ctx, "test-backup")
		require.NoError(t, err)

		// Verify all protection flags are cleared.
		for _, name := range shards {
			_, protected := idx.backupProtectedShards.Load(name)
			assert.False(t, protected, "shard %s should no longer be protected", name)
		}

		// Verify all backupLock.Locks were released by confirming RLock succeeds.
		for _, name := range shards {
			done := make(chan struct{})
			go func() {
				idx.backupLock.RLock(name)
				idx.backupLock.RUnlock(name)
				close(done)
			}()
			select {
			case <-done:
				// Lock was released.
			case <-time.After(time.Second):
				t.Fatalf("RLock on %s should succeed after ReleaseBackup", name)
			}
		}

		// The protection flag is cleared and the lock is released. Combined with the
		// subtests above (which prove the flag blocks activation), this proves that
		// activation is no longer blocked after ReleaseBackup.
	})
}

func TestBackupFrozenShardOmitted(t *testing.T) {
	rootDir := t.TempDir()
	shardName := "frozen_tenant"
	// Do NOT create shard directory — simulates a FROZEN tenant with no local files.

	stagingRoot := filepath.Join(rootDir, "staging")
	require.NoError(t, os.MkdirAll(stagingRoot, 0o755))

	idx := &Index{
		Config:    IndexConfig{RootPath: rootDir, ClassName: "MyClass"},
		getSchema: &fakeSchemaGetter{},
	}

	t.Run("hardlink path returns errFrozenShard for missing shard dir", func(t *testing.T) {
		var sd backup.ShardDescriptor
		err := idx.backupInactiveShardWithHardlinks(shardName, &sd, nil, stagingRoot)
		require.Error(t, err)
		require.True(t, errors.Is(err, errFrozenShard), "expected errFrozenShard, got %v", err)
	})

	t.Run("non-hardlink path returns errFrozenShard for missing shard dir", func(t *testing.T) {
		var sd backup.ShardDescriptor
		err := idx.backupInactiveShardWithoutHardlinks(shardName, &sd, nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errFrozenShard), "expected errFrozenShard, got %v", err)
	})
}
