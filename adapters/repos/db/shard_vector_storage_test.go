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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// newTestDynamicState opens a metadata DB in shardDir and returns dynamic's
// state namespace, the way the shard hands it to the index.
func newTestDynamicState(t *testing.T, shardDir string) dynamic.StateOps {
	t.Helper()
	db, err := shardmeta.Open(shardDir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db.Namespace(dynamic.StateNamespace)
}

// TestVectorIndexStorageDirs pins which directories each index type
// occupies for a physical ID; the probe checks them and the durability
// step syncs them.
func TestVectorIndexStorageDirs(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
		id        string
		verdict   []byte // dynamic only: stored under its state key, nil for none
		want      []string
		wantErr   string
	}{
		{name: "hnsw legacy", indexType: "hnsw", id: "main", want: []string{"main.hnsw.commitlog.d"}},
		{name: "hnsw named", indexType: "hnsw", id: "vectors_title", want: []string{"vectors_title.hnsw.commitlog.d"}},
		{name: "flat legacy", indexType: "flat", id: "main", want: []string{"lsm/vectors"}},
		{name: "flat named", indexType: "flat", id: "vectors_title", want: []string{"lsm/vectors_title"}},
		{name: "hfresh", indexType: "hfresh", id: "vectors_title", want: []string{"vectors_title.hfresh.d"}},
		{name: "dynamic not upgraded", indexType: "dynamic", id: "vectors_title", verdict: []byte{0}, want: []string{"lsm/vectors_title"}},
		{name: "dynamic upgraded", indexType: "dynamic", id: "vectors_title", verdict: []byte{1}, want: []string{"vectors_title.hnsw.commitlog.d"}},
		{name: "dynamic legacy without a verdict is flat", indexType: "dynamic", id: "main", want: []string{"lsm/vectors"}},
		{name: "unknown type", indexType: "bogus", id: "main", wantErr: `unknown vector index type "bogus"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardDir := t.TempDir()
			state := newTestDynamicState(t, shardDir)
			if tt.verdict != nil {
				key := "upgraded"
				if tt.id != "main" {
					key = "upgraded_" + tt.id[len("vectors_"):]
				}
				require.NoError(t, state.Put([]byte(key), tt.verdict))
			}

			dirs, err := vectorIndexStorageDirs(shardDir, state, tt.indexType, tt.id)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			var want []string
			for _, rel := range tt.want {
				want = append(want, filepath.Join(shardDir, rel))
			}
			assert.Equal(t, want, dirs)
		})
	}
}

func TestVectorIndexStorageExists(t *testing.T) {
	shardDir := t.TempDir()
	dirs := []string{
		filepath.Join(shardDir, "main.hnsw.commitlog.d"),
		filepath.Join(shardDir, "lsm", "vectors"),
	}

	// nothing there yet
	exists, err := vectorIndexStorageExists(dirs)
	require.NoError(t, err)
	assert.False(t, exists)

	// one of two is not all
	require.NoError(t, os.MkdirAll(dirs[0], 0o755))
	exists, err = vectorIndexStorageExists(dirs)
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, os.MkdirAll(dirs[1], 0o755))
	exists, err = vectorIndexStorageExists(dirs)
	require.NoError(t, err)
	assert.True(t, exists)

	// a file where a directory belongs is an error, not "absent"
	require.NoError(t, os.RemoveAll(dirs[0]))
	require.NoError(t, os.WriteFile(dirs[0], []byte("x"), 0o644))
	_, err = vectorIndexStorageExists(dirs)
	require.ErrorContains(t, err, "not a directory")

	// no directories means nothing can be missing
	exists, err = vectorIndexStorageExists(nil)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestSyncVectorIndexStorage(t *testing.T) {
	shardDir := t.TempDir()
	dirs := []string{
		filepath.Join(shardDir, "main.hnsw.commitlog.d"),
		filepath.Join(shardDir, "lsm", "vectors"),
		filepath.Join(shardDir, "lsm", "vectors_title"),
	}
	for _, dir := range dirs {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}

	require.NoError(t, syncVectorIndexStorage(dirs))
	require.NoError(t, syncVectorIndexStorage(nil))

	// a directory that is not there cannot be made durable
	missing := filepath.Join(shardDir, "vectors_gone.hnsw.commitlog.d")
	err := syncVectorIndexStorage([]string{missing})
	require.ErrorContains(t, err, missing)
}
