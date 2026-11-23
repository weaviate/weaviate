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

package postingedge

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// indexFiles lists every file of the index on disk, its own directory and
// the buckets of the store that are its, with the size of each.
func (f *persistentFixture) indexFiles(t *testing.T) map[string]int64 {
	t.Helper()
	files := map[string]int64{}
	dirs := []string{filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID))}
	for _, name := range helpers.PostingEdgeBucketNames(f.cfg.ID) {
		dirs = append(dirs, filepath.Join(f.root, "lsm", name))
	}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files[path] = info.Size()
			return nil
		})
		require.NoError(t, err)
	}
	return files
}

// An index whose meta is missing is not an empty index: opening it fails,
// and nothing of it is deleted. A copy or a restore that left the meta out
// opens as nothing at all otherwise, with no error. Put back, the meta opens
// the index as it was. An index that never had a base has no meta and no
// files of one, and opens as it always did.
func TestPersistentOpensWithoutItsMeta(t *testing.T) {
	const k = 10
	ctx := context.Background()

	t.Run("a base whose meta is missing", func(t *testing.T) {
		f := newPersistentFixture(t, persistentBase+200)
		f.add(t, 0, persistentBase+200)
		require.NotNil(t, f.index.g)
		before := f.answers(t, k)
		f.close(t)
		files := f.indexFiles(t)
		require.NotEmpty(t, files)
		meta := filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID), diskMetaFile)
		raw, err := os.ReadFile(meta)
		require.NoError(t, err)
		require.NoError(t, os.Remove(meta))
		delete(files, meta)

		noop := cyclemanager.NewCallbackGroupNoop()
		store, err := lsmkv.New(filepath.Join(f.root, "lsm"), f.root, testLogger(), nil, nil, noop, noop, noop)
		require.NoError(t, err)
		index, err := Open(f.cfg, f.uc, store)
		if err == nil {
			require.NoError(t, index.Shutdown(ctx))
		}
		require.NoError(t, store.Shutdown(ctx))
		assert.Error(t, err, "an index without its meta opens")
		assert.Equal(t, files, f.indexFiles(t), "what is on disk of the index, after it was opened without its meta")

		// The meta put back, the index is the one it was.
		require.NoError(t, os.WriteFile(meta, raw, 0o644))
		f.open(t)
		require.NotNil(t, f.index.g)
		f.covered(t, "with its meta back")
		assert.Equal(t, before, f.answers(t, k))
	})

	t.Run("an index that never had a base", func(t *testing.T) {
		f := newPersistentFixture(t, persistentBase)
		f.restart(t)
		require.Nil(t, f.index.g)
		f.add(t, 0, persistentBase/2)
		before := f.answers(t, k)
		f.restart(t)
		require.Nil(t, f.index.g, "still gathering")
		assert.Equal(t, persistentBase/2, f.keptAside(t))
		assert.Equal(t, before, f.answers(t, k))
	})
}
