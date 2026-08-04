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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	tlog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"

	schemaUC "github.com/weaviate/weaviate/usecases/schema"
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
				migDir := filepath.Join(lsmDir, migrationsDir)
				require.NoError(t, os.MkdirAll(filepath.Join(migDir, "sub"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(migDir, "m1.json"), []byte("m"), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(migDir, "sub", "m2.json"), []byte("m"), 0o644))

				bucketDir := filepath.Join(lsmDir, "objects")
				require.NoError(t, os.MkdirAll(bucketDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment.db"), []byte("d"), 0o644))
			},
			expected: []string{
				filepath.Join(migrationsDir, "m1.json"),
				filepath.Join(migrationsDir, "sub", "m2.json"),
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
		db:        stubDBWithNoLiveReindex(),
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
		db:        stubDBWithNoLiveReindex(),
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
			db:               stubDBWithNoLiveReindex(),
		}
	}

	t.Run("initLocalShardWithForcedLoading blocked by protection", func(t *testing.T) {
		idx := newTestIndex()
		shardName := "tenant1"

		idx.backupProtectedShards.Store(shardName, backup.NewOp("protector"))

		class := &models.Class{Class: className}
		err := idx.initLocalShardWithForcedLoading(ctx, class, shardName, true, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "protected for backup")
	})

	t.Run("getOptInitLocalShard blocked by protection", func(t *testing.T) {
		idx := newTestIndex()
		shardName := "tenant2"

		idx.backupProtectedShards.Store(shardName, backup.NewOp("protector"))

		_, release, err := idx.getOptInitLocalShard(ctx, shardName, true)
		defer release()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "protected for backup")
	})

	t.Run("getOptInitLocalShard skips check when ensureInit is false", func(t *testing.T) {
		idx := newTestIndex()
		shardName := "tenant3"

		idx.backupProtectedShards.Store(shardName, backup.NewOp("protector"))

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

		op := backup.NewOp("test-backup")
		for _, name := range shards {
			protectShardUnderOp(t, idx, name, op)
		}
		// Set backup state so ReleaseBackup has something to reset.
		idx.lastBackup.Store(&BackupState{Op: op, InProgress: true})

		err := idx.ReleaseBackup(ctx, op)
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

// protectShardUnderOp reproduces exactly what backupShardWithoutHardlinks does for
// an inactive shard: take the per-shard write lock and record the protection under
// the owning Op, both held until that Op's ReleaseBackup.
func protectShardUnderOp(t *testing.T, idx *Index, name string, op backup.Op) {
	t.Helper()
	idx.backupLock.Lock(name)
	idx.backupProtectedShards.Store(name, op)
}

// Index teardown holds closeLock for writing while waiting on backupLock, so a
// release that waits on closeLock before dropping its protections wedges the index
// for the life of the process.
func TestReleaseBackupDoesNotBlockIndexClose(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{Class: "ReleaseNoBlockClass", InvertedIndexConfig: invertedConfig()}
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)

	idx := db.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)

	var shardName string
	require.NoError(t, idx.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	}))
	require.NotEmpty(t, shardName, "fixture must have registered a shard")

	op := backup.NewOp("release-no-block")
	protectShardUnderOp(t, idx, shardName, op)
	idx.lastBackup.Store(&BackupState{Op: op, InProgress: true})

	teardownDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(teardownDone)
		// Teardown's lock order: closeLock for writing, then backupLock per shard.
		idx.closeLock.Lock()
		defer idx.closeLock.Unlock()
		idx.backupLock.RLock(shardName)
		idx.backupLock.RUnlock(shardName)
	}, idx.logger)

	releaseDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		releaseDone <- db.ReleaseBackup(ctx, op, class.Class)
	}, idx.logger)

	select {
	case <-teardownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown's closeLock+backupLock sequence did not complete within 2s")
	}
	select {
	case err := <-releaseDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("ReleaseBackup did not complete within 2s")
	}
}

// The resume sweep is skipped on a closed index — every shard is already gone — but
// steps 1-3 are unconditional, because they are what teardown is waiting for.
func TestReleaseBackupSkipsSweepOnClosedIndex(t *testing.T) {
	ctx := context.Background()
	idx := newReleaseTestIndex(t)
	idx.closed = true

	op := backup.NewOp("closed-index")
	stagingDir := backupStagingDir(idx.Config.RootPath, op, idx.Config.ClassName)
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))

	const shardName = "tenantA"
	protectShardUnderOp(t, idx, shardName, op)
	idx.lastBackup.Store(&BackupState{Op: op, InProgress: true})

	// A strict mock with NO resumeMaintenanceCycles expectation: reaching the sweep
	// on a closed index is an unexpected call and fails the test. Without it the
	// guard would be indistinguishable from its absence, since an index with no
	// shards makes the sweep a no-op either way.
	sweptShard := NewMockShardLike(t)
	idx.shards.Store("loaded-tenant", sweptShard)

	require.NoError(t, idx.ReleaseBackup(ctx, op))

	assert.NoDirExists(t, stagingDir, "staging removal must not depend on the index being open")
	_, protected := idx.backupProtectedShards.Load(shardName)
	assert.False(t, protected, "protection entry must be cleared on a closed index")
	require.True(t, idx.backupLock.TryRLock(shardName), "the protection's write lock must be released")
	idx.backupLock.RUnlock(shardName)
	assert.Nil(t, idx.lastBackup.Load(), "the admission gate must be cleared on a closed index")
}

// A release belonging to another operation instance must leave the live operation's
// resources alone. The admission-gate column is deliberately not asserted here:
// TestStaleGenerationReleaseInertAgainstSuccessor already pins it.
//
// The rows below cover the sequential case: the other op's release runs while the
// live op's protection is already in place. The concurrent variant — a straggler
// same-Op releaser that read its own Op during Range and only reaches the delete
// after a successor has stored ITS protection under the same key — is closed by the
// sweep's value-conditional CompareAndDelete rather than pinned here: hitting that
// window deterministically needs a hook between sync.Map.Range's value read and the
// delete, which the API does not offer. Stated rather than faked with a sleep.
func TestStaleReleaseLeavesLiveOpResourcesIntact(t *testing.T) {
	ctx := context.Background()
	const shardName = "tenantA"

	// Fence "0" is what makes the same-ID row stale: NewOp's counter starts at 1,
	// so it precedes every minted fence.
	tests := []struct {
		name     string
		otherOp  func(live backup.Op) backup.Op
		relation string
	}{
		{
			name:     "foreign op",
			relation: "a different backup ID",
			otherOp:  func(backup.Op) backup.Op { return backup.NewOp("other-id") },
		},
		{
			name:     "stale same-ID op",
			relation: "the same backup ID with an earlier fence",
			otherOp:  func(live backup.Op) backup.Op { return backup.Op{ID: live.ID, Fence: "0"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newReleaseTestIndex(t)

			liveOp := backup.NewOp("live-id")
			other := tt.otherOp(liveOp)

			protectShardUnderOp(t, idx, shardName, liveOp)
			liveStaging := backupStagingDir(idx.Config.RootPath, liveOp, idx.Config.ClassName)
			require.NoError(t, os.MkdirAll(liveStaging, 0o755))

			require.NoError(t, idx.ReleaseBackup(ctx, other))

			_, protected := idx.backupProtectedShards.Load(shardName)
			require.True(t, protected, "a release for %s must not drop the live op's protection", tt.relation)
			require.False(t, idx.backupLock.TryRLock(shardName),
				"a release for %s must not release the live op's shard lock", tt.relation)
			require.DirExists(t, liveStaging,
				"a release for %s must not delete the live op's staging tree", tt.relation)

			require.NoError(t, idx.ReleaseBackup(ctx, liveOp))

			_, protected = idx.backupProtectedShards.Load(shardName)
			require.False(t, protected, "the live op's own release must drop its protection")
			require.True(t, idx.backupLock.TryRLock(shardName), "the live op's own release must free its shard lock")
			idx.backupLock.RUnlock(shardName)
			require.NoDirExists(t, liveStaging, "the live op's own release must remove its staging tree")
		})
	}
}

// Concurrent releases of the SAME operation must unlock each protected shard exactly
// once: a second Unlock on an already-unlocked RWMutex is an uncatchable runtime
// fatal that kills the whole test binary.
//
// The repro is probabilistic by necessity — deterministically hitting the
// read-vs-delete window would need a hook inside sync.Map.Range, which does not
// exist. Across 800 shard-releases a single hit is fatal, so a miss is very
// unlikely; the exact rate is not measured here.
func TestConcurrentReleasesNeverDoubleUnlock(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		shards    int
		releasers int
		rounds    int
	}{
		{name: "4 shards, 8 releasers, 200 rounds", shards: 4, releasers: 8, rounds: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newReleaseTestIndex(t)

			names := make([]string, tt.shards)
			for i := range names {
				names[i] = fmt.Sprintf("tenant-%d", i)
			}

			for round := range tt.rounds {
				op := backup.NewOp(fmt.Sprintf("round-%d", round))
				for _, name := range names {
					protectShardUnderOp(t, idx, name, op)
				}

				var wg sync.WaitGroup
				for range tt.releasers {
					wg.Add(1)
					enterrors.GoWrapper(func() {
						defer wg.Done()
						assert.NoError(t, idx.ReleaseBackup(ctx, op))
					}, idx.logger)
				}
				wg.Wait()

				for _, name := range names {
					require.True(t, idx.backupLock.TryRLock(name),
						"round %d: %s must be unlocked exactly once", round, name)
					idx.backupLock.RUnlock(name)
					_, protected := idx.backupProtectedShards.Load(name)
					require.False(t, protected, "round %d: %s must no longer be protected", round, name)
				}
			}
		})
	}
}

// A cancelled operation and its same-ID retry must stage into different directories,
// or the stale instance's release deletes the retry's live snapshot mid-hardlink.
func TestStagingDirIsFencedPerOpInstance(t *testing.T) {
	const root = "/tmp/weaviate-test-root"
	className := schema.ClassName("StagingClass")

	opA, opB := backup.NewOp("X"), backup.NewOp("X")

	tests := []struct {
		name  string
		left  backup.Op
		right backup.Op
		equal bool
	}{
		{name: "same ID, distinct instances", left: opA, right: opB, equal: false},
		{name: "same instance twice", left: opA, right: opA, equal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := backupStagingDir(root, tt.left, className)
			right := backupStagingDir(root, tt.right, className)

			if tt.equal {
				assert.Equal(t, left, right)
			} else {
				assert.NotEqual(t, left, right)
			}
			for _, p := range []string{left, right} {
				assert.True(t, strings.HasPrefix(filepath.Base(p), backup.BackupStagingPrefix),
					"staging dir %q must keep the startup sweep's prefix", p)
			}
		})
	}
}

// newReleaseTestIndex builds the minimal Index that Index.ReleaseBackup touches:
// staging root, protection map + per-shard locks, admission gate and an empty shard
// map for the resume sweep.
func newReleaseTestIndex(t *testing.T) *Index {
	t.Helper()
	logger, _ := tlog.NewNullLogger()
	return &Index{
		Config:                 IndexConfig{RootPath: t.TempDir(), ClassName: "ReleaseTestClass"},
		getSchema:              &fakeSchemaGetter{},
		logger:                 logger,
		backupLock:             esync.NewKeyRWLocker(),
		shardCreateLocks:       esync.NewKeyRWLocker(),
		replicaSnapshotOpLocks: esync.NewKeyRWLocker(),
		closingCtx:             context.Background(),
		db:                     stubDBWithNoLiveReindex(),
	}
}

// newLazyTestShard registers a never-loaded *LazyLoadShard on a real index. Shared
// with the lazy-wrapper teardown rows in shard_autoresume_maintenance_test.go.
func newLazyTestShard(t *testing.T, ctx context.Context, className string) (*Index, *LazyLoadShard) {
	t.Helper()
	_, idx := testShard(t, ctx, className)

	const lazyName = "lazy-cold-shard"
	sl, err := idx.initShard(ctx, lazyName, &models.Class{Class: className}, nil, false, false)
	require.NoError(t, err)
	lazy, ok := sl.(*LazyLoadShard)
	require.True(t, ok, "expected a *LazyLoadShard")
	require.False(t, lazy.isLoaded(), "precondition: shard must start unloaded")
	idx.shards.Store(lazyName, sl)

	return idx, lazy
}

// A cold shard holds no halt to resume, and force-loading one from the index-wide
// sweep would build a second live shard on the same directory that is not in the
// index's shard map and that nothing would ever shut down.
func TestLazyShardResumeDoesNotForceLoad(t *testing.T) {
	ctx := context.Background()
	_, lazy := newLazyTestShard(t, ctx, "LazyResumeNoForceLoad")

	require.NoError(t, lazy.resumeMaintenanceCycles(ctx, backup.NewOp("B").HaltOwner()))
	require.False(t, lazy.isLoaded(), "the resume must not force-load a cold shard")
}

// The sweep costs no per-shard wall-clock delay, so its runtime stays proportional
// to the resume work itself rather than to the tenant count.
func TestResumeMaintenanceCyclesHasNoPerShardDelay(t *testing.T) {
	ctx := context.Background()
	logger, _ := tlog.NewNullLogger()
	idx := &Index{
		Config: IndexConfig{RootPath: t.TempDir(), ClassName: "SweepClass"},
		logger: logger,
	}

	const shards = 50
	for i := range shards {
		mockShard := NewMockShardLike(t)
		mockShard.EXPECT().resumeMaintenanceCycles(mock.Anything, mock.Anything).Return(nil)
		idx.shards.Store(fmt.Sprintf("tenant-%d", i), mockShard)
	}

	begin := time.Now()
	require.NoError(t, idx.resumeMaintenanceCycles(ctx, backup.NewOp("B").HaltOwner()))
	// A 10 ms per-shard delay would put a 500 ms floor under 50 shards.
	require.Less(t, time.Since(begin), 100*time.Millisecond)
}

// A class admitted after the operation is over has no releaser left: its admission
// gate stays set for the life of the process.
func TestBackupDescriptorsDoesNotAdmitAfterCancel(t *testing.T) {
	classes := []*models.Class{
		{Class: "CancelClassA", InvertedIndexConfig: invertedConfig()},
		{Class: "CancelClassB", InvertedIndexConfig: invertedConfig()},
	}
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), classes...)

	names := make([]string, 0, len(classes))
	for _, c := range classes {
		names = append(names, c.Class)
		require.NotNil(t, db.GetIndex(schema.ClassName(c.Class)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := db.BackupDescriptors(ctx, backup.NewOp("cancelled"), names, nil)
	emitted := drainDescriptors(t, ch)

	require.NotEmpty(t, emitted, "a cancelled run must still report why it stopped")
	for _, desc := range emitted {
		require.Error(t, desc.Error)
		assert.Contains(t, desc.Error.Error(), context.Canceled.Error())
	}
	for _, c := range classes {
		idx := db.GetIndex(schema.ClassName(c.Class))
		assert.Nil(t, idx.lastBackup.Load(), "class %s must not have been admitted", c.Class)
	}
}

// The producer runs under GoWrapper, whose recover() swallows a panic raised under
// idx.descriptor; without the deferred close the channel would stay open forever and
// the uploader's join would hang until its budget expires.
func TestBackupDescriptorsClosesChannelOnPanic(t *testing.T) {
	// The guarantee under test only exists on the recovered-panic path, which is the
	// production posture. The CI suite exports DISABLE_RECOVERY_ON_PANIC=true globally
	// (test/integration/run.sh) and GoWrapper reads it at recover time, so without
	// this pin the injected panic escapes the wrapper and kills the whole test binary.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	const className = "PanicDescriptorClass"
	logger, _ := tlog.NewNullLogger()

	class := &models.Class{Class: className}
	mockReader := schemaUC.NewMockSchemaReader(t)
	mockReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(string, bool, func(*models.Class, *sharding.State) error) error {
			panic("schema read exploded")
		}).Maybe()

	idx := &Index{
		Config: IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName(className)},
		getSchema: &fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}},
		},
		schemaReader:     mockReader,
		logger:           logger,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
		closingCtx:       context.Background(),
		db:               stubDBWithNoLiveReindex(),
	}

	db := &DB{
		logger:  logger,
		config:  Config{RootPath: idx.Config.RootPath},
		indices: map[string]*Index{indexID(schema.ClassName(className)): idx},
	}

	ch := db.BackupDescriptors(context.Background(), backup.NewOp("panicking"), []string{className}, nil)
	// Bounded, never a bare range: in the un-fixed state the channel is never closed
	// and a bare range would hang the package binary until the 10-minute test panic.
	drainDescriptors(t, ch)
}

// drainDescriptors reads until close, failing rather than hanging if the producer
// never closes the channel.
func drainDescriptors(t *testing.T, ch <-chan backup.ClassDescriptor) []backup.ClassDescriptor {
	t.Helper()
	var out []backup.ClassDescriptor
	for {
		select {
		case desc, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, desc)
		case <-time.After(5 * time.Second):
			t.Fatal("BackupDescriptors did not close its channel within 5s")
		}
	}
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
		db:        stubDBWithNoLiveReindex(),
	}

	t.Run("hardlink path returns errShardNoLocalData for missing shard dir", func(t *testing.T) {
		var sd backup.ShardDescriptor
		err := idx.backupInactiveShardWithHardlinks(shardName, &sd, nil, stagingRoot)
		require.Error(t, err)
		require.True(t, errors.Is(err, errShardNoLocalData), "expected errShardNoLocalData, got %v", err)
	})

	t.Run("non-hardlink path returns errShardNoLocalData for missing shard dir", func(t *testing.T) {
		var sd backup.ShardDescriptor
		err := idx.backupInactiveShardWithoutHardlinks(shardName, &sd, nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errShardNoLocalData), "expected errShardNoLocalData, got %v", err)
	})
}

// newDescriptorTestIndex creates a minimal Index wired up for testing Index.descriptor.
// The returned Index has a mock SchemaReader, a fakeSchemaGetter, and proper locking
// infrastructure. Callers populate idx.shards and the filesystem as needed.
func newDescriptorTestIndex(t *testing.T, rootDir, className string, shardState *sharding.State) *Index {
	t.Helper()
	logger, _ := tlog.NewNullLogger()

	class := &models.Class{Class: className}
	mockReader := schemaUC.NewMockSchemaReader(t)
	mockReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, shardState)
		}).Maybe()

	return &Index{
		Config: IndexConfig{RootPath: rootDir, ClassName: schema.ClassName(className)},
		getSchema: &fakeSchemaGetter{
			schema: schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{class},
				},
			},
		},
		schemaReader:     mockReader,
		logger:           logger,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
		closingCtx:       context.Background(),
		db:               stubDBWithNoLiveReindex(),
	}
}

// createColdShardFiles creates a minimal shard directory on disk simulating
// a COLD tenant with metadata, an LSM segment, and a WAL file.
func createColdShardFiles(t *testing.T, rootDir, className, shardName string) {
	t.Helper()
	shardDir := filepath.Join(rootDir, indexID(schema.ClassName(className)), shardName)

	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), []byte("42"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "proplengths"), []byte("{}"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "version"), []byte("2"), 0o644))

	bucketDir := filepath.Join(shardDir, "lsm", "objects")
	require.NoError(t, os.MkdirAll(bucketDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-0001.db"), []byte("seg-data"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "segment-123.wal"), []byte("wal-data"), 0o644))
}

func TestDescriptorColdAndFrozenTenants(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	ctx := context.Background()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant("cold-tenant", models.TenantActivityStatusCOLD).
		AddTenant("frozen-tenant", models.TenantActivityStatusFROZEN).
		WithReplicationFactor(2).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)

	// COLD tenant: real files on disk.
	createColdShardFiles(t, rootDir, className, "cold-tenant")

	// FROZEN tenant: no directory at all.

	var desc backup.ClassDescriptor
	err := idx.descriptor(ctx, backup.NewOp("test-backup"), &desc, nil)
	require.NoError(t, err)

	// Only COLD should be in desc.Shards — FROZEN is omitted.
	require.Len(t, desc.Shards, 1)

	coldDesc := desc.Shards[0]
	assert.Equal(t, "cold-tenant", coldDesc.Name)
	assert.Equal(t, "node1", coldDesc.Node)
	assert.Equal(t, []byte("42"), coldDesc.DocIDCounter)
	assert.NotEmpty(t, coldDesc.Files, "COLD descriptor should have files from disk")

	// ShardingState should contain both tenants with correct statuses.
	require.NotNil(t, desc.ShardingState, "ShardingState should be marshalled")
	var restoredState sharding.State
	require.NoError(t, json.Unmarshal(desc.ShardingState, &restoredState))
	assert.Contains(t, restoredState.Physical, "cold-tenant")
	assert.Contains(t, restoredState.Physical, "frozen-tenant")
	assert.Equal(t, models.TenantActivityStatusCOLD, restoredState.Physical["cold-tenant"].Status)
	assert.Equal(t, models.TenantActivityStatusFROZEN, restoredState.Physical["frozen-tenant"].Status)

	// Schema should be marshalled.
	assert.NotNil(t, desc.Schema, "Schema should be marshalled")
}

func TestDescriptorColdShardMutableFilesCopied(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	ctx := context.Background()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant("cold-tenant", models.TenantActivityStatusCOLD).
		WithReplicationFactor(1).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)

	// Create COLD shard files including HNSW commitlogs.
	shardDir := filepath.Join(rootDir, indexID(schema.ClassName(className)), "cold-tenant")
	createColdShardFiles(t, rootDir, className, "cold-tenant")

	clDir := filepath.Join(shardDir, "main.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(clDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(clDir, "1709203456"), []byte("commitlog"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(clDir, "1709203400.condensed"), []byte("condensed"), 0o644))

	var desc backup.ClassDescriptor
	err := idx.descriptor(ctx, backup.NewOp("test-backup"), &desc, nil)
	require.NoError(t, err)
	require.Len(t, desc.Shards, 1)

	stagingDir := desc.StagingDir
	require.DirExists(t, stagingDir)

	getIno := func(path string) uint64 {
		t.Helper()
		info, err := os.Stat(path)
		require.NoError(t, err)
		return info.Sys().(*syscall.Stat_t).Ino
	}

	idxID := indexID(schema.ClassName(className))
	bucketDir := filepath.Join(shardDir, "lsm", "objects")

	// Mutable files: different inodes (copied).
	walSrc := filepath.Join(bucketDir, "segment-123.wal")
	walDst := filepath.Join(stagingDir, idxID, "cold-tenant", "lsm", "objects", "segment-123.wal")
	assert.NotEqual(t, getIno(walSrc), getIno(walDst), "WAL should be copied")

	clSrc := filepath.Join(clDir, "1709203456")
	clDst := filepath.Join(stagingDir, idxID, "cold-tenant", "main.hnsw.commitlog.d", "1709203456")
	assert.NotEqual(t, getIno(clSrc), getIno(clDst), "non-condensed commitlog should be copied")

	// Immutable files: same inodes (hard-linked).
	segSrc := filepath.Join(bucketDir, "segment-0001.db")
	segDst := filepath.Join(stagingDir, idxID, "cold-tenant", "lsm", "objects", "segment-0001.db")
	assert.Equal(t, getIno(segSrc), getIno(segDst), "segment should be hard-linked")

	condensedSrc := filepath.Join(clDir, "1709203400.condensed")
	condensedDst := filepath.Join(stagingDir, idxID, "cold-tenant", "main.hnsw.commitlog.d", "1709203400.condensed")
	assert.Equal(t, getIno(condensedSrc), getIno(condensedDst), "condensed commitlog should be hard-linked")
}

func TestDescriptorAllFrozenTenants(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	ctx := context.Background()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant("frozen-1", models.TenantActivityStatusFROZEN).
		AddTenant("frozen-2", models.TenantActivityStatusFROZEN).
		AddTenant("frozen-3", models.TenantActivityStatusFROZEN).
		WithReplicationFactor(3).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)
	// No directories, no shards in map.

	var desc backup.ClassDescriptor
	err := idx.descriptor(ctx, backup.NewOp("test-backup"), &desc, nil)
	require.NoError(t, err)
	assert.Empty(t, desc.Shards, "all-FROZEN collection should have no shard descriptors")

	// ShardingState should still contain all 3.
	var restoredState sharding.State
	require.NoError(t, json.Unmarshal(desc.ShardingState, &restoredState))
	assert.Len(t, restoredState.Physical, 3)
}

func TestDescriptorConcurrentBackupBlocked(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant("tenant-1", models.TenantActivityStatusHOT).
		WithReplicationFactor(1).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)

	// First backup: set state.
	require.NoError(t, idx.initBackup(backup.NewOp("backup-1")))

	// Second backup: should fail.
	var desc backup.ClassDescriptor
	err := idx.descriptor(context.Background(), backup.NewOp("backup-2"), &desc, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet released")
}

func TestDescriptorReleaseCleansUpStagingDir(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	ctx := context.Background()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant("cold-tenant", models.TenantActivityStatusCOLD).
		WithReplicationFactor(1).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)
	createColdShardFiles(t, rootDir, className, "cold-tenant")

	var desc backup.ClassDescriptor
	op := backup.NewOp("test-backup")
	err := idx.descriptor(ctx, op, &desc, nil)
	require.NoError(t, err)

	stagingDir := desc.StagingDir
	require.DirExists(t, stagingDir)

	// Release should clean up.
	require.NoError(t, idx.ReleaseBackup(ctx, op))
	assert.NoDirExists(t, stagingDir, "staging dir should be removed after ReleaseBackup")
	assert.Nil(t, idx.lastBackup.Load(), "backup state should be reset")
}

// TestDescriptorHotTenants verifies that descriptor() correctly backs up HOT
// and COLD tenants through the same CreateBackupSnapshot interface
func TestDescriptorHotAndColdTenants(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	ctx := context.Background()

	hotTenants := []string{"hot-tenant-1", "hot-tenant-2"}
	coldTenants := []string{"cold-tenant-1"}
	allTenants := append(hotTenants, coldTenants...)

	builder := NewMultiTenantShardingStateBuilder().
		WithReplicationFactor(int64(len(allTenants)))
	for _, name := range hotTenants {
		builder.AddTenant(name, models.TenantActivityStatusHOT)
	}
	for _, name := range coldTenants {
		builder.AddTenant(name, models.TenantActivityStatusCOLD)
	}
	shardState := builder.Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)

	// Store MockShardLike for each HOT tenant. MockShardLike is neither
	// *LazyLoadShard nor *Shard, so it exercises the interface-based code path.
	hotFiles := map[string][]string{
		"hot-tenant-1": {"hot-tenant-1/lsm/objects/segment-0001.db"},
		"hot-tenant-2": {"hot-tenant-2/lsm/objects/segment-0002.db"},
	}
	for _, name := range hotTenants {
		name := name
		mockShard := NewMockShardLike(t)
		files := hotFiles[name]
		// preventShutdown is acquired by backupShardWithHardlinks on the active
		// path before releasing shardCreateLocks. See weaviate/0-weaviate-issues#234.
		mockShard.EXPECT().preventShutdown().Return(func() {}, nil)
		mockShard.EXPECT().
			CreateBackupSnapshot(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ string, sd *backup.ShardDescriptor, _ string) ([]string, error) {
				sd.Name = name
				sd.Node = "node1"
				return files, nil
			})
		idx.shards.Store(name, mockShard)
	}

	// COLD tenants: real files on disk (shard not in shardMap).
	for _, name := range coldTenants {
		createColdShardFiles(t, rootDir, className, name)
	}

	var desc backup.ClassDescriptor
	err := idx.descriptor(ctx, backup.NewOp("test-backup"), &desc, nil)
	require.NoError(t, err)

	require.Len(t, desc.Shards, len(hotTenants)+len(coldTenants),
		"descriptor should include both HOT and COLD tenants")

	shardsByName := map[string]*backup.ShardDescriptor{}
	for _, sd := range desc.Shards {
		shardsByName[sd.Name] = sd
	}

	// Verify HOT tenants were backed up through the mock.
	for _, name := range hotTenants {
		sd, ok := shardsByName[name]
		require.True(t, ok, "HOT tenant %s should be in descriptor", name)
		assert.Equal(t, "node1", sd.Node)
		assert.Equal(t, hotFiles[name], sd.Files,
			"HOT tenant %s should have files from CreateBackupSnapshot", name)
	}

	// Verify COLD tenants were backed up from disk.
	for _, name := range coldTenants {
		sd, ok := shardsByName[name]
		require.True(t, ok, "COLD tenant %s should be in descriptor", name)
		assert.Equal(t, "node1", sd.Node)
		assert.NotEmpty(t, sd.Files, "COLD tenant %s should have files from disk", name)
	}

	// ShardingState should contain all tenants.
	require.NotNil(t, desc.ShardingState)
	var restoredState sharding.State
	require.NoError(t, json.Unmarshal(desc.ShardingState, &restoredState))
	for _, name := range hotTenants {
		assert.Contains(t, restoredState.Physical, name)
	}
	for _, name := range coldTenants {
		assert.Contains(t, restoredState.Physical, name)
	}
}

// Asserts shardCreateLocks is released before CreateBackupSnapshot so concurrent
// queries (via Index.getOptInitLocalShard) don't stall for the snapshot duration.
// See weaviate/0-weaviate-issues#234.
func TestBackupShardWithHardlinks_ConcurrentRLockNotBlocked(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	shardName := "test-shard"
	ctx := context.Background()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant(shardName, models.TenantActivityStatusHOT).
		WithReplicationFactor(1).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)

	snapshotStarted := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	releaseCalled := make(chan struct{})

	mockShard := NewMockShardLike(t)
	mockShard.EXPECT().preventShutdown().Return(func() {
		close(releaseCalled)
	}, nil)
	mockShard.EXPECT().
		CreateBackupSnapshot(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, sd *backup.ShardDescriptor, _ string) ([]string, error) {
			sd.Name = shardName
			sd.Node = "node1"
			close(snapshotStarted)
			<-releaseSnapshot
			return []string{}, nil
		})
	idx.shards.Store(shardName, mockShard)

	type backupResult struct {
		sd  *backup.ShardDescriptor
		err error
	}
	backupDone := make(chan backupResult, 1)
	go func() {
		sd, err := idx.backupShardWithHardlinks(ctx, shardName, backup.NewOp("test-backup"), nil, t.TempDir())
		backupDone <- backupResult{sd: sd, err: err}
	}()

	select {
	case <-snapshotStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("CreateBackupSnapshot was not invoked within 5s")
	}

	require.True(t, idx.shardCreateLocks.TryRLock(shardName),
		"shardCreateLocks must be released before CreateBackupSnapshot")
	idx.shardCreateLocks.RUnlock(shardName)

	require.False(t, idx.backupLock.TryRLock(shardName),
		"backupLock must remain held during CreateBackupSnapshot")

	close(releaseSnapshot)
	select {
	case res := <-backupDone:
		require.NoError(t, res.err)
		require.NotNil(t, res.sd)
		assert.Equal(t, shardName, res.sd.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("backupShardWithHardlinks did not return within 5s of releasing snapshot")
	}

	select {
	case <-releaseCalled:
	default:
		t.Error("preventShutdown release callback was not invoked")
	}
	require.True(t, idx.backupLock.TryRLock(shardName), "backupLock must be released after return")
	idx.backupLock.RUnlock(shardName)
	require.True(t, idx.shardCreateLocks.TryRLock(shardName), "shardCreateLocks must remain released after return")
	idx.shardCreateLocks.RUnlock(shardName)
}

// Asserts both locks are released on the preventShutdown error path — guards
// the shardCreateLocksHeld bookkeeping that supports the early-release.
func TestBackupShardWithHardlinks_PreventShutdownErrorReleasesLocks(t *testing.T) {
	rootDir := t.TempDir()
	className := "TestClass"
	shardName := "test-shard"
	ctx := context.Background()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant(shardName, models.TenantActivityStatusHOT).
		WithReplicationFactor(1).
		Build()

	idx := newDescriptorTestIndex(t, rootDir, className, shardState)

	mockShard := NewMockShardLike(t)
	mockShard.EXPECT().preventShutdown().Return(nil, errors.New("shard is shutting down"))
	idx.shards.Store(shardName, mockShard)

	_, err := idx.backupShardWithHardlinks(ctx, shardName, backup.NewOp("test-backup"), nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prevent shutdown")

	// Both locks must be released even on the error path.
	require.True(t, idx.backupLock.TryRLock(shardName),
		"backupLock must be released after preventShutdown failure")
	idx.backupLock.RUnlock(shardName)
	require.True(t, idx.shardCreateLocks.TryRLock(shardName),
		"shardCreateLocks must be released after preventShutdown failure")
	idx.shardCreateLocks.RUnlock(shardName)
}
