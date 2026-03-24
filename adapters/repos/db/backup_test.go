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
	"testing"
	"time"

	tlog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
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
