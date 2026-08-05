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

func boolPtr(b bool) *bool {
	return &b
}

func TestPropertyDeleteIndexHelper_IsPropertyIndexRemoved(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	tests := []struct {
		name     string
		setting  *bool
		expected bool
	}{
		{
			name:     "nil setting",
			setting:  nil,
			expected: false,
		},
		{
			name:     "true setting",
			setting:  boolPtr(true),
			expected: false,
		},
		{
			name:     "false setting",
			setting:  boolPtr(false),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, h.isPropertyIndexRemoved(tt.setting))
		})
	}
}

func TestPropertyDeleteIndexHelper_GetPropertyIndexDir(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	result := h.getPropertyIndexDir("/data/index", "shard1", "property_name")
	assert.Equal(t, filepath.Join("/data/index", "shard1", "lsm", "property_name"), result)
}

func TestPropertyDeleteIndexHelper_PropertyIndexBucketExistsOnDisk(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	indexPath := t.TempDir()
	shardName := "shard1"

	bucketName := "property_myProp"
	bucketPath := filepath.Join(indexPath, shardName, "lsm", bucketName)
	require.NoError(t, os.MkdirAll(bucketPath, 0o755))

	t.Run("returns true when bucket directory exists", func(t *testing.T) {
		assert.True(t, h.propertyIndexBucketExistsOnDisk(indexPath, shardName, bucketName))
	})

	t.Run("returns false when bucket directory does not exist", func(t *testing.T) {
		assert.False(t, h.propertyIndexBucketExistsOnDisk(indexPath, shardName, "property_nonexistent"))
	})
}

func TestPropertyDeleteIndexHelper_RemovePropertyIndexBucketFromDisk(t *testing.T) {
	h := newPropertyDeleteIndexHelper()

	t.Run("removes existing bucket directory", func(t *testing.T) {
		indexPath := t.TempDir()
		shardName := "shard1"
		bucketName := "property_myProp"
		bucketPath := filepath.Join(indexPath, shardName, "lsm", bucketName)
		require.NoError(t, os.MkdirAll(bucketPath, 0o755))

		// create a file inside to verify recursive removal
		require.NoError(t, os.WriteFile(filepath.Join(bucketPath, "data.db"), []byte("data"), 0o644))

		err := h.removePropertyIndexBucketFromDisk(indexPath, shardName, bucketName)
		require.NoError(t, err)

		_, statErr := os.Stat(bucketPath)
		assert.True(t, os.IsNotExist(statErr))
	})

	t.Run("succeeds when bucket directory does not exist", func(t *testing.T) {
		indexPath := t.TempDir()
		err := h.removePropertyIndexBucketFromDisk(indexPath, "shard1", "property_nonexistent")
		require.NoError(t, err)
	})
}

func TestPropertyDeleteIndexHelper_EnsureBucketsAreRemovedForNonExistentPropertyIndexes(t *testing.T) {
	setup := func(t *testing.T) (string, string) {
		t.Helper()
		indexPath := t.TempDir()
		shardName := "shard1"
		return indexPath, shardName
	}

	createBucket := func(t *testing.T, indexPath, shardName, bucketName string) {
		t.Helper()
		bucketPath := filepath.Join(indexPath, shardName, "lsm", bucketName)
		require.NoError(t, os.MkdirAll(bucketPath, 0o755))
	}

	bucketExists := func(indexPath, shardName, bucketName string) bool {
		bucketPath := filepath.Join(indexPath, shardName, "lsm", bucketName)
		_, err := os.Stat(bucketPath)
		return err == nil
	}

	t.Run("no properties", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		class := &models.Class{
			Class:      "TestClass",
			Properties: nil,
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})

	t.Run("all indexes enabled - buckets kept", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		propName := "myProp"
		filterableBucket := helpers.BucketFromPropNameLSM(propName)
		searchableBucket := helpers.BucketSearchableFromPropNameLSM(propName)
		rangeableBucket := helpers.BucketRangeableFromPropNameLSM(propName)

		createBucket(t, indexPath, shardName, filterableBucket)
		createBucket(t, indexPath, shardName, searchableBucket)
		createBucket(t, indexPath, shardName, rangeableBucket)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              propName,
					IndexFilterable:   boolPtr(true),
					IndexSearchable:   boolPtr(true),
					IndexRangeFilters: boolPtr(true),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.True(t, bucketExists(indexPath, shardName, filterableBucket))
		assert.True(t, bucketExists(indexPath, shardName, searchableBucket))
		assert.True(t, bucketExists(indexPath, shardName, rangeableBucket))
	})

	t.Run("all indexes nil - buckets kept", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		propName := "myProp"
		filterableBucket := helpers.BucketFromPropNameLSM(propName)
		searchableBucket := helpers.BucketSearchableFromPropNameLSM(propName)
		rangeableBucket := helpers.BucketRangeableFromPropNameLSM(propName)

		createBucket(t, indexPath, shardName, filterableBucket)
		createBucket(t, indexPath, shardName, searchableBucket)
		createBucket(t, indexPath, shardName, rangeableBucket)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              propName,
					IndexFilterable:   nil,
					IndexSearchable:   nil,
					IndexRangeFilters: nil,
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.True(t, bucketExists(indexPath, shardName, filterableBucket))
		assert.True(t, bucketExists(indexPath, shardName, searchableBucket))
		assert.True(t, bucketExists(indexPath, shardName, rangeableBucket))
	})

	t.Run("all indexes disabled - all buckets removed", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		propName := "myProp"
		filterableBucket := helpers.BucketFromPropNameLSM(propName)
		searchableBucket := helpers.BucketSearchableFromPropNameLSM(propName)
		rangeableBucket := helpers.BucketRangeableFromPropNameLSM(propName)

		createBucket(t, indexPath, shardName, filterableBucket)
		createBucket(t, indexPath, shardName, searchableBucket)
		createBucket(t, indexPath, shardName, rangeableBucket)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              propName,
					IndexFilterable:   boolPtr(false),
					IndexSearchable:   boolPtr(false),
					IndexRangeFilters: boolPtr(false),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, bucketExists(indexPath, shardName, filterableBucket))
		assert.False(t, bucketExists(indexPath, shardName, searchableBucket))
		assert.False(t, bucketExists(indexPath, shardName, rangeableBucket))
	})

	t.Run("filterable disabled only - only filterable bucket removed", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		propName := "myProp"
		filterableBucket := helpers.BucketFromPropNameLSM(propName)
		searchableBucket := helpers.BucketSearchableFromPropNameLSM(propName)
		rangeableBucket := helpers.BucketRangeableFromPropNameLSM(propName)

		createBucket(t, indexPath, shardName, filterableBucket)
		createBucket(t, indexPath, shardName, searchableBucket)
		createBucket(t, indexPath, shardName, rangeableBucket)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              propName,
					IndexFilterable:   boolPtr(false),
					IndexSearchable:   boolPtr(true),
					IndexRangeFilters: boolPtr(true),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, bucketExists(indexPath, shardName, filterableBucket))
		assert.True(t, bucketExists(indexPath, shardName, searchableBucket))
		assert.True(t, bucketExists(indexPath, shardName, rangeableBucket))
	})

	t.Run("searchable disabled only - only searchable bucket removed", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		propName := "myProp"
		filterableBucket := helpers.BucketFromPropNameLSM(propName)
		searchableBucket := helpers.BucketSearchableFromPropNameLSM(propName)
		rangeableBucket := helpers.BucketRangeableFromPropNameLSM(propName)

		createBucket(t, indexPath, shardName, filterableBucket)
		createBucket(t, indexPath, shardName, searchableBucket)
		createBucket(t, indexPath, shardName, rangeableBucket)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              propName,
					IndexFilterable:   boolPtr(true),
					IndexSearchable:   boolPtr(false),
					IndexRangeFilters: boolPtr(true),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.True(t, bucketExists(indexPath, shardName, filterableBucket))
		assert.False(t, bucketExists(indexPath, shardName, searchableBucket))
		assert.True(t, bucketExists(indexPath, shardName, rangeableBucket))
	})

	t.Run("rangeFilters disabled only - only rangeable bucket removed", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		propName := "myProp"
		filterableBucket := helpers.BucketFromPropNameLSM(propName)
		searchableBucket := helpers.BucketSearchableFromPropNameLSM(propName)
		rangeableBucket := helpers.BucketRangeableFromPropNameLSM(propName)

		createBucket(t, indexPath, shardName, filterableBucket)
		createBucket(t, indexPath, shardName, searchableBucket)
		createBucket(t, indexPath, shardName, rangeableBucket)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              propName,
					IndexFilterable:   boolPtr(true),
					IndexSearchable:   boolPtr(true),
					IndexRangeFilters: boolPtr(false),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.True(t, bucketExists(indexPath, shardName, filterableBucket))
		assert.True(t, bucketExists(indexPath, shardName, searchableBucket))
		assert.False(t, bucketExists(indexPath, shardName, rangeableBucket))
	})

	t.Run("disabled index but no bucket on disk - no error", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:              "myProp",
					IndexFilterable:   boolPtr(false),
					IndexSearchable:   boolPtr(false),
					IndexRangeFilters: boolPtr(false),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})

	t.Run("multiple properties with mixed settings", func(t *testing.T) {
		h := newPropertyDeleteIndexHelper()
		indexPath, shardName := setup(t)

		// prop1: filterable disabled, searchable enabled
		// prop2: filterable enabled, searchable disabled
		prop1Filterable := helpers.BucketFromPropNameLSM("prop1")
		prop1Searchable := helpers.BucketSearchableFromPropNameLSM("prop1")
		prop2Filterable := helpers.BucketFromPropNameLSM("prop2")
		prop2Searchable := helpers.BucketSearchableFromPropNameLSM("prop2")

		createBucket(t, indexPath, shardName, prop1Filterable)
		createBucket(t, indexPath, shardName, prop1Searchable)
		createBucket(t, indexPath, shardName, prop2Filterable)
		createBucket(t, indexPath, shardName, prop2Searchable)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name:            "prop1",
					IndexFilterable: boolPtr(false),
					IndexSearchable: boolPtr(true),
				},
				{
					Name:            "prop2",
					IndexFilterable: boolPtr(true),
					IndexSearchable: boolPtr(false),
				},
			},
		}

		err := h.ensureBucketsAreRemovedForNonExistentPropertyIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, bucketExists(indexPath, shardName, prop1Filterable))
		assert.True(t, bucketExists(indexPath, shardName, prop1Searchable))
		assert.True(t, bucketExists(indexPath, shardName, prop2Filterable))
		assert.False(t, bucketExists(indexPath, shardName, prop2Searchable))
	})
}
