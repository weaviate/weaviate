//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	numClasses = 100
	numShards  = 10
	chars      = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" + "0123456789"
)

var (
	rootFiles = []string{
		"classifications.db",
		"modules.db",
		"schema.db",
	}
	indexDirExts = []string{
		".hnsw.commitlog.d",
		"_someGeoProp.hnsw.commitlog.d",
		"_lsm",
	}
	indexFileExts = []string{
		".indexcount",
		".proplengths",
		".version",
	}
	migratedRootFiles = append(rootFiles,
		"migration1.22.fs.hierarchy")
)

func TestFileStructureMigration(t *testing.T) {
	m := make(map[string]struct{}, numClasses*numShards)

	t.Run("generate index and shard names", func(t *testing.T) {
		for i := 0; i < numClasses; i++ {
			c := randClassName()
			for j := 0; j < numShards; j++ {
				s := randShardName()
				m[fmt.Sprintf("%s_%s", c, s)] = struct{}{}
			}
		}
	})

	root := t.TempDir()

	t.Run("write test db files", func(t *testing.T) {
		for _, f := range rootFiles {
			require.Nil(t, os.WriteFile(path.Join(root, f), nil, os.ModePerm))
		}

		for k := range m {
			idx := path.Join(root, k)
			for _, ext := range indexDirExts {
				require.Nil(t, os.MkdirAll(idx+ext, os.ModePerm))
			}
			for _, ext := range indexFileExts {
				require.Nil(t, os.WriteFile(idx+ext, nil, os.ModePerm))
			}
		}
	})

	files, err := os.ReadDir(root)
	require.Nil(t, err)

	t.Run("assert expected flat contents length", func(t *testing.T) {
		// Flat structure root contains:
		//  - (3 dirs + 3 files) per shard per index
		//    - dirs: main commilog, geo prop commitlog, lsm store
		//    - files: indexcount, proplengths, version
		//  - 3 root db files
		expectedLen := numClasses*numShards*(len(indexDirExts)+len(indexFileExts)) + len(rootFiles)
		require.Len(t, files, expectedLen)
	})

	t.Run("migrate the db", func(t *testing.T) {
		db := testDB(root)
		require.Nil(t, db.migrateFileStructureIfNecessary())
	})

	files, err = os.ReadDir(root)
	require.Nil(t, err)

	t.Run("assert expected hierarchical contents length", func(t *testing.T) {
		// After migration, the hierarchical structure root contains:
		//  - one dir per index
		//  - 3 original root db files, and one additional which is the FS migration indicator
		expectedLen := numClasses + len(migratedRootFiles)
		require.Len(t, files, expectedLen)
	})

	t.Run("assert all db files were migrated", func(t *testing.T) {
		var foundRootFiles []string
		for _, f := range files {
			if f.IsDir() {
				idx := f
				shardsRoot, err := os.ReadDir(path.Join(root, idx.Name()))
				require.Nil(t, err)
				for _, shard := range shardsRoot {
					assertShardRootContents(t, m, root, idx, shard)
				}
			} else {
				foundRootFiles = append(foundRootFiles, f.Name())
			}
		}

		assert.ElementsMatch(t, migratedRootFiles, foundRootFiles)
	})
}

func assertShardRootContents(t *testing.T, flatFiles map[string]struct{}, root string, idx, shard os.DirEntry) {
	assert.True(t, shard.IsDir())

	// Whatever we find in this shard directory, it should be able to
	// be mapped back to the original flat structure root contents
	_, ok := flatFiles[fmt.Sprintf("%s_%s", idx.Name(), shard.Name())]
	assert.True(t, ok)

	// Now we will get a set of all expected files within the shard dir.
	// Check to see if all of these files are found.
	expected := expectedShardContents()
	shardFiles, err := os.ReadDir(path.Join(root, idx.Name(), shard.Name()))
	require.Nil(t, err)
	for _, sf := range shardFiles {
		expected[sf.Name()] = true
	}
	expected.assert(t)
}

func testDB(root string) *DB {
	return &DB{
		config: Config{RootPath: root},
	}
}

func randClassName() string {
	return randStringBytes(16)
}

func randShardName() string {
	return randStringBytes(8)
}

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

type shardContents map[string]bool

func expectedShardContents() shardContents {
	return shardContents{
		"main.hnsw.commitlog.d":            false,
		"geo.someGeoProp.hnsw.commitlog.d": false,
		"lsm":                              false,
		"indexcount":                       false,
		"proplengths":                      false,
		"version":                          false,
	}
}

func (c shardContents) assert(t *testing.T) {
	for name, found := range c {
		assert.True(t, found, "didn't find %q in shard contents", name)
	}
}
