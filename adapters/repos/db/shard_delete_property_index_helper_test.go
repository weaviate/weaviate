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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
)

func ptrBool(v bool) *bool { return &v }

func TestPropertyDeleteIndexHelper_IsPropertyIndexRemoved(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	t.Run("nil pointer returns false", func(t *testing.T) {
		assert.False(t, h.isPropertyIndexRemoved(nil))
	})

	t.Run("true pointer returns false", func(t *testing.T) {
		assert.False(t, h.isPropertyIndexRemoved(ptrBool(true)))
	})

	t.Run("false pointer returns true", func(t *testing.T) {
		assert.True(t, h.isPropertyIndexRemoved(ptrBool(false)))
	})
}

func TestPropertyDeleteIndexHelper_MarkPropertyIndexRemoved(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	t.Run("creates migration directory and flag file", func(t *testing.T) {
		indexPath := t.TempDir()

		err := h.markPropertyIndexRemoved(indexPath, "title")
		require.NoError(t, err)

		migDir := filepath.Join(indexPath, ".migrations")
		assert.DirExists(t, migDir)

		flagFile := filepath.Join(migDir, "title.index.removed")
		assert.FileExists(t, flagFile)
	})

	t.Run("migration directory already exists", func(t *testing.T) {
		indexPath := t.TempDir()
		migDir := filepath.Join(indexPath, ".migrations")
		require.NoError(t, os.Mkdir(migDir, 0o755))

		err := h.markPropertyIndexRemoved(indexPath, "author")
		require.NoError(t, err)

		flagFile := filepath.Join(migDir, "author.index.removed")
		assert.FileExists(t, flagFile)
	})

	t.Run("flag file already exists - idempotent", func(t *testing.T) {
		indexPath := t.TempDir()

		err := h.markPropertyIndexRemoved(indexPath, "year")
		require.NoError(t, err)

		err = h.markPropertyIndexRemoved(indexPath, "year")
		require.NoError(t, err)

		flagFile := filepath.Join(indexPath, ".migrations", "year.index.removed")
		assert.FileExists(t, flagFile)
	})

	t.Run("multiple properties create separate flag files", func(t *testing.T) {
		indexPath := t.TempDir()

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))
		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "author"))
		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "year"))

		migDir := filepath.Join(indexPath, ".migrations")
		assert.FileExists(t, filepath.Join(migDir, "title.index.removed"))
		assert.FileExists(t, filepath.Join(migDir, "author.index.removed"))
		assert.FileExists(t, filepath.Join(migDir, "year.index.removed"))
	})
}

func TestPropertyDeleteIndexHelper_EnsureBucketsAreRemoved(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	createBucketDir := func(t *testing.T, indexPath, shardName, bucketName string) string {
		t.Helper()
		dir := filepath.Join(indexPath, shardName, "lsm", bucketName)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		// place a file inside so we can verify the whole tree is removed
		require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), []byte("test"), 0o644))
		return dir
	}

	t.Run("no migration directory - no-op", func(t *testing.T) {
		indexPath := t.TempDir()
		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, "shard1", class)
		require.NoError(t, err)
	})

	t.Run("migration directory exists but no flag files - no-op", func(t *testing.T) {
		indexPath := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(indexPath, ".migrations"), 0o755))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, "shard1", class)
		require.NoError(t, err)
	})

	t.Run("removes filterable bucket when index is disabled", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		bucketName := helpers.BucketFromPropNameLSM("title")
		bucketDir := createBucketDir(t, indexPath, shardName, bucketName)

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.NoDirExists(t, bucketDir)
	})

	t.Run("removes searchable bucket when index is disabled", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		bucketName := helpers.BucketSearchableFromPropNameLSM("author")
		bucketDir := createBucketDir(t, indexPath, shardName, bucketName)

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "author"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "author", IndexSearchable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.NoDirExists(t, bucketDir)
	})

	t.Run("removes rangeable bucket when index is disabled", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		bucketName := helpers.BucketRangeableFromPropNameLSM("year")
		bucketDir := createBucketDir(t, indexPath, shardName, bucketName)

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "year"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "year", IndexRangeFilters: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.NoDirExists(t, bucketDir)
	})

	t.Run("removes all bucket types for a single property", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		filterableDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("title"))
		searchableDir := createBucketDir(t, indexPath, shardName, helpers.BucketSearchableFromPropNameLSM("title"))
		rangeableDir := createBucketDir(t, indexPath, shardName, helpers.BucketRangeableFromPropNameLSM("title"))

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{
					Name:              "title",
					IndexFilterable:   ptrBool(false),
					IndexSearchable:   ptrBool(false),
					IndexRangeFilters: ptrBool(false),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.NoDirExists(t, filterableDir)
		assert.NoDirExists(t, searchableDir)
		assert.NoDirExists(t, rangeableDir)
	})

	t.Run("does not remove buckets when index is still enabled", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		filterableDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("title"))
		searchableDir := createBucketDir(t, indexPath, shardName, helpers.BucketSearchableFromPropNameLSM("title"))

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{
					Name:            "title",
					IndexFilterable: ptrBool(true),
					IndexSearchable: ptrBool(true),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.DirExists(t, filterableDir)
		assert.DirExists(t, searchableDir)
	})

	t.Run("does not remove buckets when index setting is nil", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		filterableDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("title"))

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title"},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.DirExists(t, filterableDir)
	})

	t.Run("flag file exists but bucket does not exist on disk - no error", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})

	t.Run("flag file for property not in class - ignored", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "nonexistent"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})

	t.Run("multiple properties - only removes disabled indexes", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		titleFilterableDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("title"))
		authorSearchableDir := createBucketDir(t, indexPath, shardName, helpers.BucketSearchableFromPropNameLSM("author"))
		yearFilterableDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("year"))
		yearRangeableDir := createBucketDir(t, indexPath, shardName, helpers.BucketRangeableFromPropNameLSM("year"))

		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "title"))
		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "author"))
		require.NoError(t, h.markPropertyIndexRemoved(indexPath, "year"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{
					Name:            "title",
					IndexFilterable: ptrBool(false), // disabled - should be removed
					IndexSearchable: ptrBool(true),  // enabled - should stay
				},
				{
					Name:            "author",
					IndexFilterable: ptrBool(true),  // enabled - should stay
					IndexSearchable: ptrBool(false), // disabled - should be removed
				},
				{
					Name:              "year",
					IndexFilterable:   ptrBool(true),  // enabled - should stay
					IndexRangeFilters: ptrBool(false), // disabled - should be removed
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.NoDirExists(t, titleFilterableDir, "title filterable should be removed")
		assert.NoDirExists(t, authorSearchableDir, "author searchable should be removed")
		assert.DirExists(t, yearFilterableDir, "year filterable should stay")
		assert.NoDirExists(t, yearRangeableDir, "year rangeable should be removed")
	})

	t.Run("non-flag files in migration directory are ignored", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		bucketDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("title"))

		migDir := filepath.Join(indexPath, ".migrations")
		require.NoError(t, os.Mkdir(migDir, 0o755))
		// create a file that doesn't match the flag suffix
		require.NoError(t, os.WriteFile(filepath.Join(migDir, "title.other"), []byte{}, 0o644))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.DirExists(t, bucketDir, "bucket should not be removed without proper flag file")
	})

	t.Run("subdirectories in migration directory are ignored", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"

		migDir := filepath.Join(indexPath, ".migrations")
		require.NoError(t, os.Mkdir(migDir, 0o755))
		// create a subdirectory with the flag suffix name
		require.NoError(t, os.Mkdir(filepath.Join(migDir, "title.index.removed"), 0o755))

		bucketDir := createBucketDir(t, indexPath, shardName, helpers.BucketFromPropNameLSM("title"))

		class := &models.Class{
			Class: "Book",
			Properties: []*models.Property{
				{Name: "title", IndexFilterable: ptrBool(false)},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
		assert.DirExists(t, bucketDir, "subdirectory should not be treated as flag file")
	})
}
