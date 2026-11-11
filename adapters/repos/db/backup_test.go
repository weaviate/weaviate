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
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	tlog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
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
			name: "migrations tmp leftovers are excluded, records and sentinels are not",
			setup: func(t *testing.T, lsmDir string) {
				trackerDir := filepath.Join(lsmDir, migrationsDir, "searchable_retokenize_text_1")
				recordsDir := filepath.Join(lsmDir, migrationsDir, "records")
				require.NoError(t, os.MkdirAll(trackerDir, 0o755))
				require.NoError(t, os.MkdirAll(recordsDir, 0o755))
				for _, name := range []string{
					"payload.mig", "started.mig", "properties.mig", "progress.mig.000000001",
				} {
					require.NoError(t, os.WriteFile(filepath.Join(trackerDir, name), []byte("x"), 0o644))
				}
				require.NoError(t, os.WriteFile(
					filepath.Join(recordsDir, "7_searchable_retokenize_shard-1__node-0.json"), []byte("{}"), 0o644))

				// Same call the record store's atomic write makes, so the name
				// carries the real random infix rather than one the test picked.
				leftover, err := os.CreateTemp(recordsDir, "7_searchable_retokenize_shard-1__node-0.json.*.tmp")
				require.NoError(t, err)
				require.NoError(t, leftover.Close())
			},
			expected: []string{
				filepath.Join(migrationsDir, "records", "7_searchable_retokenize_shard-1__node-0.json"),
				filepath.Join(migrationsDir, "searchable_retokenize_text_1", "payload.mig"),
				filepath.Join(migrationsDir, "searchable_retokenize_text_1", "progress.mig.000000001"),
				filepath.Join(migrationsDir, "searchable_retokenize_text_1", "properties.mig"),
				filepath.Join(migrationsDir, "searchable_retokenize_text_1", "started.mig"),
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
	indexID := "myclass"
	shardName := "tenant1"

	// rootFiles are written as regular files at the shard root, where the vector
	// indexes keep their state. extraDirs maps a shard subdirectory to the files
	// created inside it. unreadableDir is a shard subdirectory made unreadable.
	tests := []struct {
		name          string
		rootFiles     []string
		extraDirs     map[string][]string
		unreadableDir string
		extraExpected []string
		wantErr       bool
	}{
		{
			name: "no vector index state",
		},
		{
			name:          "dynamic index state db",
			rootFiles:     []string{shardmeta.FileName},
			extraExpected: []string{filepath.Join(indexID, shardName, shardmeta.FileName)},
		},
		{
			name:          "flat index metadata of the unnamed vector",
			rootFiles:     []string{"meta.db"},
			extraExpected: []string{filepath.Join(indexID, shardName, "meta.db")},
		},
		{
			name:      "flat index metadata of every named vector",
			rootFiles: []string{"meta_first.db", "meta_second.db"},
			extraExpected: []string{
				filepath.Join(indexID, shardName, "meta_first.db"),
				filepath.Join(indexID, shardName, "meta_second.db"),
			},
		},
		{
			name:      "dynamic and flat state side by side",
			rootFiles: []string{shardmeta.FileName, "meta.db", "meta_first.db"},
			extraExpected: []string{
				filepath.Join(indexID, shardName, shardmeta.FileName),
				filepath.Join(indexID, shardName, "meta.db"),
				filepath.Join(indexID, shardName, "meta_first.db"),
			},
		},
		{
			name:      "unrelated root files are left out",
			rootFiles: []string{"metadata.db", "usage.json.tmp"},
		},
		{
			name: "hashtree and change-capture logs are left out",
			extraDirs: map[string][]string{
				hashTreeDirName:  {"hashtree-1a2b3c.ht"},
				changelogDirName: {"op-1.log"},
			},
		},
		{
			name: "nested vector index files are listed, nested .tmp files are not",
			extraDirs: map[string][]string{
				"main.hfresh.d":          {"centroids.bin"},
				"main.hfresh.d/postings": {"postings-1.bin", "postings-2.bin.tmp"},
			},
			extraExpected: []string{
				filepath.Join(indexID, shardName, "main.hfresh.d", "centroids.bin"),
				filepath.Join(indexID, shardName, "main.hfresh.d", "postings", "postings-1.bin"),
			},
		},
		{
			name:      "empty vector index directory",
			extraDirs: map[string][]string{"main.queue.d": nil},
		},
		{
			name:          "unreadable nested vector index directory fails the listing",
			extraDirs:     map[string][]string{"main.hfresh.d/postings": {"postings-1.bin"}},
			unreadableDir: "main.hfresh.d/postings",
			wantErr:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a minimal Index-like setup with a temp dir structure mimicking a shard.
			rootDir := t.TempDir()
			indexDir := filepath.Join(rootDir, indexID)
			shardDir := filepath.Join(indexDir, shardName)

			// Create shard directory structure
			require.NoError(t, os.MkdirAll(shardDir, 0o755))

			// Metadata files
			require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), []byte("42"), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(shardDir, "proplengths"), []byte(`{"len":1}`), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(shardDir, "version"), []byte("2"), 0o644))

			for _, name := range test.rootFiles {
				require.NoError(t, os.WriteFile(filepath.Join(shardDir, name), []byte("bolt"), 0o644))
			}

			for dir, names := range test.extraDirs {
				require.NoError(t, os.MkdirAll(filepath.Join(shardDir, dir), 0o755))
				for _, name := range names {
					require.NoError(t, os.WriteFile(filepath.Join(shardDir, dir, name), []byte("data"), 0o644))
				}
			}

			if test.unreadableDir != "" {
				if os.Getuid() == 0 {
					t.Skip("root ignores directory permissions")
				}
				dir := filepath.Join(shardDir, test.unreadableDir)
				require.NoError(t, os.Chmod(dir, 0o000))
				t.Cleanup(func() { os.Chmod(dir, 0o755) })
			}

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
				Config:    IndexConfig{NodeName: "node1", RootPath: rootDir, ClassName: "MyClass"},
				getSchema: &fakeSchemaGetter{},
				db:        stubDBWithNoLiveReindex(),
			}

			var sd backup.ShardDescriptor
			files, err := idx.listInactiveShardFiles(shardName, &sd)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Verify metadata
			assert.Equal(t, shardName, sd.Name)
			assert.Equal(t, "node1", sd.Node)
			assert.Equal(t, []byte("42"), sd.DocIDCounter)
			assert.Equal(t, []byte(`{"len":1}`), sd.PropLengthTracker)
			assert.Equal(t, []byte("2"), sd.Version)
			// The descriptor holds these for the whole backup, once per shard.
			assert.Equal(t, len(sd.DocIDCounter), cap(sd.DocIDCounter))
			assert.Equal(t, len(sd.PropLengthTracker), cap(sd.PropLengthTracker))
			assert.Equal(t, len(sd.Version), cap(sd.Version))

			// Verify relative paths for metadata
			assert.Equal(t, filepath.Join(indexID, shardName, "indexcount"), sd.DocIDCounterPath)
			assert.Equal(t, filepath.Join(indexID, shardName, "proplengths"), sd.PropLengthTrackerPath)
			assert.Equal(t, filepath.Join(indexID, shardName, "version"), sd.ShardVersionPath)

			// Verify file list: should include .wal, exclude .tmp
			sort.Strings(files)
			expected := append([]string{
				filepath.Join(indexID, shardName, "lsm", "objects", "active.wal"),
				filepath.Join(indexID, shardName, "lsm", "objects", "segment-0001.db"),
				filepath.Join(indexID, shardName, "vectors_default", "commitlog.0001"),
				filepath.Join(indexID, shardName, "vectors_default", "main.hnsw"),
			}, test.extraExpected...)
			sort.Strings(expected)
			assert.Equal(t, expected, files)
		})
	}
}

func TestIsImmutableFile(t *testing.T) {
	tests := []struct {
		relPath string
		want    bool
		desc    string
	}{
		// Immutable files (safe to hardlink):
		{"myclass/shard1/lsm/objects/segment-0001.db", true, "LSM segment (immutable)"},
		{"myclass/shard1/main.hnsw.commitlog.d/1709203456.condensed", true, "condensed HNSW commitlog (immutable)"},
		{"myclass/shard1/main.hnsw.snapshot.d/1709203456.snapshot", true, "HNSW snapshot (immutable)"},

		// Mutable files (must be copied):
		{"myclass/shard1/lsm/objects/segment-123.wal", false, "LSM WAL"},
		{"myclass/shard1/main/meta.db", false, "flat index BoltDB (single vector)"},
		{"myclass/shard1/main_custom/meta_custom.db", false, "flat index BoltDB (multi-vector)"},
		{"myclass/shard1/main.hnsw.commitlog.d/1709203456", false, "non-condensed HNSW commitlog"},
		{"myclass/shard1/main.queue.d/chunk-1709203456000000.bin", false, "async indexing queue chunk"},
		{"myclass/shard1/index.db", false, "dynamic vector index BoltDB"},

		// Unknown files default to mutable (safe — will be copied):
		{"myclass/shard1/lsm/.migrations/m1", false, "migration file (unknown, copied)"},
		{"myclass/shard1/lsm/objects/segment.db.tmp", false, "tmp file (unknown, copied)"},
		{"myclass/shard1/hashtree_uuid/hashtree-abc.ht", false, "hashtree (unknown, copied)"},
		{"myclass/shard1/some-new-file.bin", false, "unknown file type (copied by default)"},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got := backup.IsImmutableFile(tc.relPath)
			assert.Equal(t, tc.want, got, "isImmutableFile(%q)", tc.relPath)
		})
	}
}

// TestIsImmutableFile_CompactV2 audits isImmutableFile coverage for the file
// types the compact-v2 commit-log subsystem produces.
//
// Filename patterns are sourced from
// adapters/repos/db/vector/hnsw/compact/file_discovery.go:BuildMergedFilename
// and adapters/repos/db/vector/hnsw/compact/file_info.go:FileType.Suffix.
// Snapshot location is sourced from compact/compactor.go:writeSnapshot, which
// writes into c.config.Dir — the commit log directory — *not* the legacy
// .hnsw.snapshot.d sibling directory.
//
// isImmutableFile is consulted only on the inactive-shard backup path
// (backupInactiveShardWithHardlinks). A miss here is a *performance*
// regression, not a correctness bug: missing files fall back to copyFile
// (full byte copy + fsync) instead of os.Link (instant hardlink). Compact-v2
// makes .sorted the dominant post-compaction format, so a missing classifier
// makes inactive-tenant backups noticeably slower than legacy.
func TestIsImmutableFile_CompactV2(t *testing.T) {
	tests := []struct {
		name    string
		relPath string
		want    bool
		why     string
	}{
		{
			name:    "compact-v2 sorted file (single-source name)",
			relPath: "myclass/shard1/main.hnsw.commitlog.d/1709203456.sorted",
			want:    true,
			why:     "produced by compact.SortedWriter via SafeFileWriter; atomically renamed in on commit, never reopened for writes",
		},
		{
			name:    "compact-v2 sorted file (range-merged name)",
			relPath: "myclass/shard1/main.hnsw.commitlog.d/1709203456_1709204000.sorted",
			want:    true,
			why:     "n-way merge of two or more sorted files; atomic write via SafeFileWriter",
		},
		{
			name:    "compact-v2 snapshot lives in commitlog dir (single-source name)",
			relPath: "myclass/shard1/main.hnsw.commitlog.d/1709203456.snapshot",
			want:    true,
			why:     "compact-v2 writes snapshots into the commitlog dir (compactor.writeSnapshot uses c.config.Dir), not the legacy .hnsw.snapshot.d sibling",
		},
		{
			name:    "compact-v2 snapshot lives in commitlog dir (range-merged name)",
			relPath: "myclass/shard1/main.hnsw.commitlog.d/1709203456_1709204000.snapshot",
			want:    true,
			why:     "n-way merge of snapshot + sorted files, atomic write",
		},
		{
			name:    "compact-v2 sorted file under a named target vector",
			relPath: "myclass/shard1/customVec.hnsw.commitlog.d/1709203456.sorted",
			want:    true,
			why:     "named target vectors get their own commitlog dir; classification is by extension and applies regardless of dir",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := backup.IsImmutableFile(tc.relPath)
			if got != tc.want {
				t.Errorf("isImmutableFile(%q) = %v, want %v\n\twhy it should be %v: %s",
					tc.relPath, got, tc.want, tc.want, tc.why)
			}
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

	// Dynamic index state DB at the shard root (mutable).
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, shardmeta.FileName), []byte("bolt-data"), 0o644))

	// Flat vector index metadata (mutable), at the shard root where flat.New writes it.
	require.NoError(t, os.WriteFile(filepath.Join(shardDir, "meta.db"), []byte("boltdb-data"), 0o644))

	// HNSW commitlog directory with condensed (immutable) and non-condensed (mutable).
	clDir := filepath.Join(shardDir, "main.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(clDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(clDir, "1709203456"), []byte("commitlog"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(clDir, "1709203400.condensed"), []byte("condensed"), 0o644))

	// Staging directory.
	stagingRoot := filepath.Join(rootDir, "staging")
	require.NoError(t, os.MkdirAll(stagingRoot, 0o755))

	idx := &Index{
		Config:    IndexConfig{NodeName: "node1", RootPath: rootDir, ClassName: "MyClass"},
		getSchema: &fakeSchemaGetter{},
		db:        stubDBWithNoLiveReindex(),
	}

	var sd backup.ShardDescriptor
	err := idx.backupInactiveShardWithHardlinks(shardName, &sd, nil, stagingRoot, newReindexGate(idx.db, reindexGateMaxAge))
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

	metaSrc := filepath.Join(shardDir, "meta.db")
	metaDst := filepath.Join(stagingRoot, indexID, shardName, "meta.db")
	assert.NotEqual(t, getIno(metaSrc), getIno(metaDst), "meta.db should be copied, not hard-linked")

	clSrc := filepath.Join(clDir, "1709203456")
	clDst := filepath.Join(stagingRoot, indexID, shardName, "main.hnsw.commitlog.d", "1709203456")
	assert.NotEqual(t, getIno(clSrc), getIno(clDst), "non-condensed commitlog should be copied, not hard-linked")

	stateDBSrc := filepath.Join(shardDir, shardmeta.FileName)
	stateDBDst := filepath.Join(stagingRoot, indexID, shardName, shardmeta.FileName)
	assert.NotEqual(t, getIno(stateDBSrc), getIno(stateDBDst), "index.db should be copied, not hard-linked")

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

func TestBackupFrozenShardOmitted(t *testing.T) {
	rootDir := t.TempDir()
	shardName := "frozen_tenant"
	// Do NOT create shard directory — simulates a FROZEN tenant with no local files.

	stagingRoot := filepath.Join(rootDir, "staging")
	require.NoError(t, os.MkdirAll(stagingRoot, 0o755))

	idx := &Index{
		Config:    IndexConfig{NodeName: "node1", RootPath: rootDir, ClassName: "MyClass"},
		getSchema: &fakeSchemaGetter{},
		db:        stubDBWithNoLiveReindex(),
	}

	t.Run("hardlink path returns errShardNoLocalData for missing shard dir", func(t *testing.T) {
		var sd backup.ShardDescriptor
		err := idx.backupInactiveShardWithHardlinks(shardName, &sd, nil, stagingRoot, newReindexGate(idx.db, reindexGateMaxAge))
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
	mockReader := local.NewMockSchemaReader(t)
	mockReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, shardState)
		}).Maybe()

	return &Index{
		Config: IndexConfig{NodeName: "node1", RootPath: rootDir, ClassName: schema.ClassName(className)},
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
	err := idx.descriptor(ctx, "test-backup", &desc, nil, nil, newReindexGate(idx.db, reindexGateMaxAge))
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
	err := idx.descriptor(ctx, "test-backup", &desc, nil, nil, newReindexGate(idx.db, reindexGateMaxAge))
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
	err := idx.descriptor(ctx, "test-backup", &desc, nil, nil, newReindexGate(idx.db, reindexGateMaxAge))
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
	require.NoError(t, idx.initBackup("backup-1"))

	// Second backup: should fail.
	var desc backup.ClassDescriptor
	err := idx.descriptor(context.Background(), "backup-2", &desc, nil, nil, newReindexGate(idx.db, reindexGateMaxAge))
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
	backupID := "test-backup"
	err := idx.descriptor(ctx, backupID, &desc, nil, nil, newReindexGate(idx.db, reindexGateMaxAge))
	require.NoError(t, err)

	stagingDir := desc.StagingDir
	require.DirExists(t, stagingDir)

	// Release should clean up.
	require.NoError(t, idx.ReleaseBackup(ctx, backupID))
	assert.NoDirExists(t, stagingDir, "staging dir should be removed after ReleaseBackup")
	assert.Nil(t, idx.lastBackup.Load(), "backup state should be reset")
}

func TestReleaseBackupForeignHalt(t *testing.T) {
	t.Run("a halt held by another operation survives the release", func(t *testing.T) {
		ctx := context.Background()
		index, shard := newSharedHaltTestShard(t)

		require.NoError(t, shard.HaltForTransfer(ctx, false))
		defer func() { require.NoError(t, shard.resumeMaintenanceCycles(ctx)) }()

		require.NoError(t, index.ReleaseBackup(ctx, "backup-1"))

		assert.EqualValues(t, 1, shard.haltForTransferCount.Load())
		assert.False(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.IsActive(),
			"vector maintenance must stay paused for the operation that halted the shard")
		assert.False(t, shard.cycleCallbacks.geoPropsCombinedCallbacksCtrl.IsActive(),
			"geo maintenance must stay paused for the operation that halted the shard")
	})
}

func TestBackupableHardlinkPreflight(t *testing.T) {
	t.Run("an unsupported filesystem refuses before any class is checked", func(t *testing.T) {
		t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
		const className = "ClassA"
		builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(1)
		builder.AddTenant("cold-tenant", models.TenantActivityStatusCOLD)
		db := backupTestDBWithColdTenants(t, []string{className}, []string{"cold-tenant"}, builder.Build())
		var fetches atomic.Int64
		db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
			fetches.Add(1)
			return makeActivityBuilder(nil)()
		})

		err := db.Backupable(context.Background(), []string{className, "MissingClass"})
		require.ErrorContains(t, err, "backup requires a filesystem that supports hard links")
		assert.NotContains(t, err.Error(), "doesn't exist", "the refusal must precede the per-class checks")
		assert.Zero(t, fetches.Load(), "the refusal must precede the reindex gate")
	})
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
			RunAndReturn(func(_ context.Context, sd *backup.ShardDescriptor, _ string, _ reindexGate) ([]string, error) {
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
	err := idx.descriptor(ctx, "test-backup", &desc, nil, nil, newReindexGate(idx.db, reindexGateMaxAge))
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
		RunAndReturn(func(_ context.Context, sd *backup.ShardDescriptor, _ string, _ reindexGate) ([]string, error) {
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
		sd, err := idx.backupShardWithHardlinks(ctx, shardName, nil, t.TempDir(), newReindexGate(idx.db, reindexGateMaxAge))
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

// Asserts the shard reference is dropped only after backupLock is unlocked, on
// every return that follows preventShutdown. Dropping the last reference can run
// the shard teardown inline, which under backupLock blocks all writes to the shard.
func TestBackupShardWithHardlinks_ReleasesShardAfterBackupLock(t *testing.T) {
	className := "TestClass"
	shardName := "test-shard"
	snapshotFile := "objects/segment-0001.db"
	ctx := context.Background()

	tests := []struct {
		name        string
		snapshotErr error
		// missingBigFile makes FillFileInfo stat a file that is absent from the staging dir.
		missingBigFile bool
		expectedErr    string
	}{
		{name: "success"},
		{name: "snapshot fails", snapshotErr: errors.New("out of disk"), expectedErr: "snapshot shard"},
		{name: "file info fails", missingBigFile: true, expectedErr: "gather shard"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardState := NewMultiTenantShardingStateBuilder().
				AddTenant(shardName, models.TenantActivityStatusHOT).
				WithReplicationFactor(1).
				Build()
			idx := newDescriptorTestIndex(t, t.TempDir(), className, shardState)

			var released, backupLockFreeAtRelease bool
			mockShard := NewMockShardLike(t)
			mockShard.EXPECT().preventShutdown().Return(func() {
				released = true
				backupLockFreeAtRelease = idx.backupLock.TryRLock(shardName)
				if backupLockFreeAtRelease {
					idx.backupLock.RUnlock(shardName)
				}
			}, nil)
			mockShard.EXPECT().
				CreateBackupSnapshot(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, sd *backup.ShardDescriptor, _ string, _ reindexGate) ([]string, error) {
					if tt.snapshotErr != nil {
						return nil, tt.snapshotErr
					}
					sd.Name = shardName
					sd.Node = "node1"
					return []string{snapshotFile}, nil
				})
			idx.shards.Store(shardName, mockShard)

			var baseDescrs []*backup.BaseClassDescriptor
			if tt.missingBigFile {
				baseDescrs = []*backup.BaseClassDescriptor{{
					BackupID: "base-backup",
					Shards: []backup.BaseShardDescriptor{{
						Name:          shardName,
						BigFilesChunk: map[string]backup.BigFileInfo{snapshotFile: {Size: 1}},
					}},
				}}
			}

			_, err := idx.backupShardWithHardlinks(ctx, shardName, baseDescrs, t.TempDir(), newReindexGate(idx.db, reindexGateMaxAge))
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			require.True(t, released, "shard reference must be released")
			assert.True(t, backupLockFreeAtRelease,
				"backupLock must be unlocked before the shard reference is dropped")
		})
	}
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
	mockShard.EXPECT().preventShutdown().Return(func() {}, errors.New("shard is shutting down"))
	idx.shards.Store(shardName, mockShard)

	_, err := idx.backupShardWithHardlinks(ctx, shardName, nil, t.TempDir(), newReindexGate(idx.db, reindexGateMaxAge))
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

// TestBackupDescriptorsClosesChannelWhenCancelled pins that the producer closes
// the descriptor channel on the early return a cancelled context takes. The
// caller drains that channel to close before it releases any index, so a producer
// that returns without closing leaves the backup waiting on a channel with no sender.
func TestBackupDescriptorsClosesChannelWhenCancelled(t *testing.T) {
	logger, _ := tlog.NewNullLogger()
	db := &DB{localNodeName: "node1", logger: logger, indices: map[string]*Index{}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := db.BackupDescriptors(ctx, "backup-1", []string{"Class-A", "Class-B"}, nil, nil)

	var got []backup.ClassDescriptor
	for {
		select {
		case d, ok := <-ch:
			if !ok {
				require.Len(t, got, 1, "the cancelled class is the last descriptor sent")
				assert.Equal(t, "Class-A", got[0].Name)
				require.Error(t, got[0].Error)
				assert.ErrorIs(t, got[0].Error, context.Canceled)
				return
			}
			got = append(got, d)
		case <-time.After(5 * time.Second):
			t.Fatal("BackupDescriptors never closed the descriptor channel")
		}
	}
}

// TestBackupDescriptorsClosesChannelOnPanic pins the exit that defer close(ds)
// exists for. GoWrapper recovers a panic and lets the goroutine end, so a close
// that is not deferred leaves the caller draining a channel with no sender.
func TestBackupDescriptorsClosesChannelOnPanic(t *testing.T) {
	// The integration suite exports this, which turns GoWrapper's recover off.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	logger, hook := tlog.NewNullLogger()
	// A zero-value Index panics inside descriptor on its nil logger.
	db := &DB{localNodeName: "node1", logger: logger, indices: map[string]*Index{indexID("Class-A"): {}}}

	ch := db.BackupDescriptors(context.Background(), "backup-1", []string{"Class-A"}, nil, nil)

	select {
	case d, ok := <-ch:
		require.False(t, ok, "descriptor %q was sent, so the producer returned instead of panicking", d.Name)
	case <-time.After(5 * time.Second):
		t.Fatal("BackupDescriptors never closed the descriptor channel after the producer panicked")
	}

	// GoWrapper logs the recovery after the deferred close ran. Returning at close
	// would let t.Setenv restore the flag while the recover is still reading it.
	require.Eventually(t, func() bool {
		for _, e := range hook.AllEntries() {
			if strings.Contains(e.Message, "Recovered from panic") {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the producer goroutine never recovered a panic")
}

func TestDB_ShardReplicas(t *testing.T) {
	ctx := testCtx()
	logger, _ := tlog.NewNullLogger()

	newDB := func(t *testing.T, className string, state *sharding.State) *DB {
		mockSchemaReader := local.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				return readFunc(&models.Class{Class: className}, state)
			},
		)
		return &DB{logger: logger, schemaReader: mockSchemaReader}
	}

	t.Run("replicated and single-replica shards", func(t *testing.T) {
		className := "ReplicasClass"
		db := newDB(t, className, &sharding.State{
			Physical: map[string]sharding.Physical{
				"shard1": {Name: "shard1", BelongsToNodes: []string{"node1", "node2", "node3"}},
				"shard2": {Name: "shard2", BelongsToNodes: []string{"node2"}},
			},
		})

		replicas, err := db.ShardReplicas(ctx, className)
		require.NoError(t, err)
		assert.Equal(t, map[string][]string{
			"shard1": {"node1", "node2", "node3"},
			"shard2": {"node2"},
		}, replicas)
	})

	t.Run("empty node names filtered and empty shards omitted", func(t *testing.T) {
		className := "FilteredClass"
		db := newDB(t, className, &sharding.State{
			Physical: map[string]sharding.Physical{
				"shard1": {Name: "shard1", BelongsToNodes: []string{"", "node1", ""}},
				"shard2": {Name: "shard2", BelongsToNodes: []string{""}},
				"shard3": {Name: "shard3", BelongsToNodes: nil},
			},
		})

		replicas, err := db.ShardReplicas(ctx, className)
		require.NoError(t, err)
		assert.Equal(t, map[string][]string{"shard1": {"node1"}}, replicas)
	})

	t.Run("tenants without local data omitted, HOT, COLD and empty status kept", func(t *testing.T) {
		className := "TenantStatusClass"
		db := newDB(t, className, &sharding.State{
			Physical: map[string]sharding.Physical{
				"hot":        {Name: "hot", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusHOT},
				"cold":       {Name: "cold", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusCOLD},
				"frozen":     {Name: "frozen", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusFROZEN},
				"freezing":   {Name: "freezing", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusFREEZING},
				"unfreezing": {Name: "unfreezing", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusUNFREEZING},
				"no-status":  {Name: "no-status", BelongsToNodes: []string{"node1", "node2"}},
			},
		})

		replicas, err := db.ShardReplicas(ctx, className)
		require.NoError(t, err)
		assert.Equal(t, map[string][]string{
			"hot":       {"node1", "node2"},
			"cold":      {"node1", "node2"},
			"no-status": {"node1", "node2"},
		}, replicas)
	})

	t.Run("nil sharding state", func(t *testing.T) {
		className := "NilStateClass"
		db := newDB(t, className, nil)

		replicas, err := db.ShardReplicas(ctx, className)
		assert.Error(t, err)
		assert.Nil(t, replicas)
		assert.Contains(t, err.Error(), "failed to read sharding state")
	})
}

// Regression: a task-list fetch per shard, then per collection, scaled backup
// time with tenant and collection count. COLD tenants take the inactive-shard
// path; HOT ones halt each loaded shard.
func TestBackupFetchesReindexTasksOncePerPass(t *testing.T) {
	classes := []string{"ClassA", "ClassB", "ClassC"}
	tenants := []string{"tenant-1", "tenant-2", "tenant-3"}

	for _, status := range []string{models.TenantActivityStatusCOLD, models.TenantActivityStatusHOT} {
		for _, tc := range []struct {
			name    string
			live    map[[2]string]bool
			refused bool
		}{
			{name: "no reindex running"},
			{name: "reindex running on one tenant", live: map[[2]string]bool{{"ClassB", "tenant-2"}: true}, refused: true},
		} {
			t.Run(status+"/"+tc.name, func(t *testing.T) {
				// One replica per node puts every tenant on this node.
				builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(len(tenants)))
				for _, name := range tenants {
					builder.AddTenant(name, status)
				}
				var db *DB
				if status == models.TenantActivityStatusCOLD {
					db = backupTestDBWithColdTenants(t, classes, tenants, builder.Build())
				} else {
					db = backupTestDBWithLoadedShards(t, classes, builder.Build())
				}

				var fetches atomic.Int64
				db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
					fetches.Add(1)
					return makeActivityBuilder(tc.live)()
				})

				err := db.Backupable(context.Background(), classes)
				if tc.refused {
					require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
				} else {
					require.NoError(t, err)
				}
				assert.EqualValues(t, 1, fetches.Load(), "Backupable")

				fetches.Store(0)
				var descs []backup.ClassDescriptor
				for d := range db.BackupDescriptors(context.Background(), "backup-1", classes, nil, nil) {
					descs = append(descs, d)
				}
				if tc.refused {
					require.NotEmpty(t, descs)
					require.ErrorIs(t, descs[len(descs)-1].Error, backup.ErrBackupBlockedByInFlightReindex)
				} else {
					require.Len(t, descs, len(classes))
					for _, d := range descs {
						require.NoError(t, d.Error)
						require.Len(t, d.Shards, len(tenants))
						require.NoError(t, db.ReleaseBackup(context.Background(), "backup-1", d.Name))
					}
				}
				assert.EqualValues(t, 1, fetches.Load(), "BackupDescriptors")
			})
		}
	}
}

func TestBackupableRefusalSizeDoesNotGrowWithTenants(t *testing.T) {
	const className = "ClassA"
	// One shard's refusal is about 600 characters; ten sampled names fit in the rest.
	const maxErrLen = 2000
	tenants := make([]string, 1000)
	everyTenant := map[[2]string]bool{}
	builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(len(tenants)))
	for i := range tenants {
		tenants[i] = fmt.Sprintf("tenant-%04d", i)
		everyTenant[[2]string{className, tenants[i]}] = true
		builder.AddTenant(tenants[i], models.TenantActivityStatusCOLD)
	}
	db := backupTestDBWithColdTenants(t, []string{className}, tenants, builder.Build())

	for _, tc := range []struct {
		name      string
		lookup    ShardReindexActivityLookupBuilder
		wantErr   error
		wantCount string
		wantFirst string
	}{
		{
			name:      "reindex running on every tenant",
			lookup:    makeActivityBuilder(everyTenant),
			wantErr:   backup.ErrBackupBlockedByInFlightReindex,
			wantCount: "1000 of 1000 shards",
			wantFirst: "tenant-0000",
		},
		{
			name:      "reindex running on one tenant",
			lookup:    makeActivityBuilder(map[[2]string]bool{{className, "tenant-0500"}: true}),
			wantErr:   backup.ErrBackupBlockedByInFlightReindex,
			wantCount: "1 of 1000 shards blocked (tenant-0500)",
			wantFirst: "tenant-0500",
		},
		{
			name:      "task manager unreachable",
			lookup:    unreachableActivityBuilder,
			wantErr:   ErrReindexGateUnavailable,
			wantCount: "1000 of 1000 shards",
			wantFirst: "tenant-0000",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db.SetShardReindexActivityLookup(tc.lookup)

			err := db.Backupable(context.Background(), []string{className})
			require.ErrorIs(t, err, tc.wantErr)
			assert.Contains(t, err.Error(), tc.wantCount)
			assert.Contains(t, err.Error(), fmt.Sprintf("shard %q", tc.wantFirst))
			assert.Less(t, len(err.Error()), maxErrLen)
		})
	}
}

func backupTestDBWithColdTenants(t *testing.T, classes, tenants []string, state *sharding.State) *DB {
	t.Helper()
	logger, _ := tlog.NewNullLogger()
	db := &DB{logger: logger, indices: map[string]*Index{}}
	for _, className := range classes {
		rootDir := t.TempDir()
		for _, name := range tenants {
			createColdShardFiles(t, rootDir, className, name)
		}
		idx := newDescriptorTestIndex(t, rootDir, className, state)
		idx.db = db
		db.indices[indexID(schema.ClassName(className))] = idx
	}
	return db
}

func backupTestDBWithLoadedShards(t *testing.T, classNames []string, state *sharding.State) *DB {
	t.Helper()
	var classes []*models.Class
	for _, className := range classNames {
		classes = append(classes, &models.Class{
			Class:               className,
			MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
			VectorIndexConfig:   enthnsw.UserConfig{Skip: true},
			InvertedIndexConfig: invertedConfig(),
		})
	}
	db := openTestDatabase(t, t.TempDir(), state, monitoring.GetMetrics(), nil, nil, classes...)
	t.Cleanup(func() { require.NoError(t, db.Shutdown(context.Background())) })
	for _, className := range classNames {
		idx := db.GetIndex(schema.ClassName(className))
		require.NotNil(t, idx)
		for _, name := range state.AllPhysicalShards() {
			require.NoError(t, idx.LoadLocalShardForTenantAdd(context.Background(), name))
			// An unloaded shard would take the inactive-shard path.
			if lazy, ok := idx.shards.Load(name).(*LazyLoadShard); ok {
				_, _, err := lazy.loadIfCold(context.Background())
				require.NoError(t, err)
			}
		}
	}
	return db
}

// snapshotHookIndex runs onSnapshot before SnapshotMutableFiles. CreateBackupSnapshot
// calls it after a successful halt and before its deferred resume.
type snapshotHookIndex struct {
	VectorIndex
	onSnapshot func()
}

func (i *snapshotHookIndex) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	i.onSnapshot()
	return i.VectorIndex.SnapshotMutableFiles(ctx, basePath, stagingDir)
}

// newResumeFailingBackupShard returns a shard whose backup snapshot fails to
// resume. While halted, the snapshot adds a target-node override, an
// async-replication enable that the halt skips. It then unregisters the vector
// maintenance callbacks, so the resume fails with ErrorCallbackNotFound.
func newResumeFailingBackupShard(t *testing.T) (*Index, *Shard) {
	t.Helper()
	ctx := context.Background()
	index, shard := newSharedHaltTestShard(t)
	// A single replica at factor 1: the override alone enables async replication.
	index.schemaReader.(*local.MockSchemaReader).EXPECT().ShardReplicas(mock.Anything, mock.Anything).
		Return([]string{"node1"}, nil).Maybe()
	index.asyncReplicationScheduler = newSchedulerForUnitTest(t)
	index.replicationConfigLock.Lock()
	index.Config.AsyncReplicationConfig = minAsyncReplicationConfig()
	index.replicationConfigLock.Unlock()

	var names []string
	var inner VectorIndex
	require.NoError(t, shard.ForEachVectorIndex(func(name string, vi VectorIndex) error {
		names, inner = append(names, name), vi
		return nil
	}))
	require.Len(t, names, 1)
	shard.setVectorIndex(names[0], &snapshotHookIndex{VectorIndex: inner, onSnapshot: func() {
		require.NoError(t, shard.addTargetNodeOverride(ctx, additional.AsyncReplicationTargetNodeOverride{
			CollectionID:   index.Config.ClassName.String(),
			ShardID:        shard.name,
			SourceNode:     "nodeA",
			TargetNode:     "nodeB",
			UpperTimeBound: time.Now().Add(time.Hour).UnixMilli(),
		}))
		require.False(t, asyncReplicationInstalled(shard), "the enable must be skipped while halted")
		require.NoError(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.Unregister(ctx))
	}})
	return index, shard
}

// failBackupSnapshotResume runs a backup snapshot whose halt succeeds and whose
// resume fails, leaving the shard at halt count 0 with resumeIncomplete set.
func failBackupSnapshotResume(t *testing.T, index *Index, shard *Shard) error {
	t.Helper()
	stagingRoot := backupStagingDir(index.Config.RootPath, "backup-1", index.Config.ClassName)
	_, err := shard.CreateBackupSnapshot(context.Background(), &backup.ShardDescriptor{}, stagingRoot,
		newReindexGate(index.db, reindexGateMaxAge))
	require.Zero(t, shard.haltForTransferCount.Load())
	require.True(t, shard.resumeIncomplete.Load())
	return err
}

// reregisterVectorCallbacks registers the vector maintenance callbacks again as
// inactive, the state the halt left them in, so only a resume can activate them.
func reregisterVectorCallbacks(s *Shard) {
	cc := s.cycleCallbacks
	cc.vectorCombinedCallbacksCtrl = cyclemanager.NewCombinedCallbackCtrl(2, s.index.logger,
		s.index.cycleCallbacks.vectorCommitLoggerCallbacks.Register(
			"retry/commit_logger", cc.vectorCommitLoggerCallbacks.CycleCallback, cyclemanager.AsInactive()),
		s.index.cycleCallbacks.vectorTombstoneCleanupCallbacks.Register(
			"retry/tombstone_cleanup", cc.vectorTombstoneCleanupCallbacks.CycleCallback, cyclemanager.AsInactive()))
}

func TestCreateBackupSnapshot(t *testing.T) {
	t.Run("a failed resume fails the snapshot", func(t *testing.T) {
		index, shard := newResumeFailingBackupShard(t)

		err := failBackupSnapshotResume(t, index, shard)
		require.ErrorContains(t, err, "resume maintenance after backup snapshot")
		require.ErrorIs(t, err, cyclemanager.ErrorCallbackNotFound)
	})
}

func TestReleaseBackupResumeRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("an incomplete resume is retried and re-applies the skipped enable", func(t *testing.T) {
		index, shard := newResumeFailingBackupShard(t)
		require.Error(t, failBackupSnapshotResume(t, index, shard))
		require.False(t, asyncReplicationInstalled(shard), "a failed resume must not re-apply")
		reregisterVectorCallbacks(shard)

		require.NoError(t, index.ReleaseBackup(ctx, "backup-1"))

		require.Zero(t, shard.haltForTransferCount.Load())
		require.False(t, shard.resumeIncomplete.Load())
		require.True(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.IsActive(), "the retry must restart vector maintenance")
		require.True(t, asyncReplicationInstalled(shard), "the retry must re-apply the skipped enable")
	})

	t.Run("an incomplete resume under another operation's halt is left to that operation", func(t *testing.T) {
		index, shard := newResumeFailingBackupShard(t)
		require.Error(t, failBackupSnapshotResume(t, index, shard))
		reregisterVectorCallbacks(shard)
		require.NoError(t, shard.HaltForTransfer(ctx, false))

		require.NoError(t, index.ReleaseBackup(ctx, "backup-1"))

		require.EqualValues(t, 1, shard.haltForTransferCount.Load(), "the release must not end another operation's halt")
		require.True(t, shard.resumeIncomplete.Load())
		require.False(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.IsActive(),
			"vector maintenance must stay paused for the operation that halted the shard")

		// The holder's own resume completes the unfinished one.
		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		require.False(t, shard.resumeIncomplete.Load())
	})

	t.Run("a cold lazy shard stays cold", func(t *testing.T) {
		index, loaded := newSharedHaltTestShard(t)
		cold := NewLazyLoadShard(ctx, nil, "cold", index, loaded.class, index.centralJobQueue,
			nil, index.shardLoadLimiter, index.shardReindexer, false, index.bitmapBufPool)
		index.shards.Store("cold", cold)

		require.NoError(t, index.ReleaseBackup(ctx, "backup-1"))
		require.False(t, cold.isLoaded(), "the release must not load the shard")
	})
}
