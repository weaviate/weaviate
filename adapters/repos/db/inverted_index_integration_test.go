//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexByTimestampsNullState_AddClass(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	class := &models.Class{
		Class:             "TestClass",
		VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			IndexTimestamps: true,
			IndexNullState:  true,
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     []string{"string"},
				Tokenization: "word",
			},
		},
	}
	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	defer repo.Shutdown(context.Background())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("add class", func(t *testing.T) {
		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
	})

	t.Run("check for timestamp buckets", func(t *testing.T) {
		for _, idx := range migrator.db.indices {
			for _, shd := range idx.Shards {
				createBucket := shd.store.Bucket("property__creationTimeUnix")
				assert.NotNil(t, createBucket, "property__creationTimeUnix bucket not found")

				createHashBucket := shd.store.Bucket("hash_property__creationTimeUnix")
				assert.NotNil(t, createHashBucket, "hash_property__creationTimeUnix bucket not found")

				updateBucket := shd.store.Bucket("property__lastUpdateTimeUnix")
				assert.NotNil(t, updateBucket)

				updateHashBucket := shd.store.Bucket("hash_property__lastUpdateTimeUnix")
				assert.NotNil(t, updateHashBucket, "hash_property__creationTimeUnix bucket not found")

				assert.NotNil(t, shd.store.Bucket("property_name"+filters.InternalNullIndex), "property_name"+filters.InternalNullIndex+"bucket not found")
				assert.NotNil(t, shd.store.Bucket("hash_property_name"+filters.InternalNullIndex), "hash_property_name"+filters.InternalNullIndex+"bucket not found")

			}
		}
	})

	t.Run("Add Objects", func(t *testing.T) {
		testID1 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
		objWithProperty := &models.Object{
			ID:         testID1,
			Class:      "TestClass",
			Properties: map[string]interface{}{"name": "objectarooni"},
		}
		vec := []float32{1, 2, 3}
		require.Nil(t, repo.PutObject(context.Background(), objWithProperty, vec))

		testID2 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a63")
		objWithoutProperty := &models.Object{
			ID:         testID2,
			Class:      "TestClass",
			Properties: map[string]interface{}{"name": nil},
		}
		require.Nil(t, repo.PutObject(context.Background(), objWithoutProperty, vec))
	})
}

func TestIndexNullState_GetClass(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	class := &models.Class{
		Class:             "TestClass",
		VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			IndexTimestamps:        true,
			IndexNullState:         true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{"string"},
			},
			{
				Name:     "number array",
				DataType: []string{"number[]"},
			},
		},
	}

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	defer repo.Shutdown(testCtx())
	migrator := NewMigrator(repo, logger)

	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))

	testID1 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
	objWithProperty := &models.Object{
		ID:         testID1,
		Class:      "TestClass",
		Properties: map[string]interface{}{"name": "objectarooni", "name array": []float64{0.5, 1.4}},
	}
	require.Nil(t, repo.PutObject(context.Background(), objWithProperty, []float32{1, 2, 3}))

	testID2 := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a63")
	objWithoutProperty := &models.Object{
		ID:         testID2,
		Class:      "TestClass",
		Properties: map[string]interface{}{"name": nil, "name array": nil},
	}
	require.Nil(t, repo.PutObject(context.Background(), objWithoutProperty, []float32{1, 2, 4}))

	require.Equal(t, 1, len(migrator.db.indices["testclass"].Shards))
	for _, shd := range migrator.db.indices["testclass"].Shards {
		bucket := shd.store.Bucket("property_name" + filters.InternalNullIndex)
		require.NotNil(t, bucket)
	}

	tests := map[string]strfmt.UUID{"filterNull": testID1, "filterNonNull": testID2}
	for name, searchVal := range tests {
		t.Run("test "+name+" directly on nullState property", func(t *testing.T) {
			createTimeStringFilter := &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "TestClass",
						Property: "name_nullState",
					},
					Value: &filters.Value{
						Value: searchVal != testID1,
						Type:  "boolean",
					},
				},
			}

			res1, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				ClassName:  "TestClass",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    createTimeStringFilter,
			})
			require.Nil(t, err)
			assert.Len(t, res1, 1)
			assert.Equal(t, searchVal, res1[0].ID)
		})
	}
}

func TestIndexByTimestamps_GetClass(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	class := &models.Class{
		Class:             "TestClass",
		VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
			IndexTimestamps: true,
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     []string{"string"},
				Tokenization: "word",
			},
		},
	}

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	defer repo.Shutdown(testCtx())
	migrator := NewMigrator(repo, logger)

	t.Run("add class", func(t *testing.T) {
		err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
		require.Nil(t, err)
	})

	now := time.Now().UnixNano() / int64(time.Millisecond)
	testID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")

	t.Run("insert test object", func(t *testing.T) {
		obj := &models.Object{
			ID:                 testID,
			Class:              "TestClass",
			CreationTimeUnix:   now,
			LastUpdateTimeUnix: now,
			Properties:         map[string]interface{}{"name": "objectarooni"},
		}
		vec := []float32{1, 2, 3}
		err := repo.PutObject(context.Background(), obj, vec)
		require.Nil(t, err)
	})

	t.Run("get testObject with timestamp filters", func(t *testing.T) {
		createTimeStringFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    "TestClass",
					Property: "_creationTimeUnix",
				},
				Value: &filters.Value{
					Value: fmt.Sprint(now),
					Type:  dtString,
				},
			},
		}

		updateTimeStringFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    "TestClass",
					Property: "_lastUpdateTimeUnix",
				},
				Value: &filters.Value{
					Value: fmt.Sprint(now),
					Type:  dtString,
				},
			},
		}

		createTimeDateFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    "TestClass",
					Property: "_creationTimeUnix",
				},
				Value: &filters.Value{
					Value: msToRFC3339(now),
					Type:  dtDate,
				},
			},
		}

		updateTimeDateFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    "TestClass",
					Property: "_lastUpdateTimeUnix",
				},
				Value: &filters.Value{
					Value: msToRFC3339(now),
					Type:  dtDate,
				},
			},
		}

		res1, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    createTimeStringFilter,
		})
		require.Nil(t, err)
		assert.Len(t, res1, 1)
		assert.Equal(t, testID, res1[0].ID)

		res2, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    updateTimeStringFilter,
		})
		require.Nil(t, err)
		assert.Len(t, res2, 1)
		assert.Equal(t, testID, res2[0].ID)

		res3, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    createTimeDateFilter,
		})
		require.Nil(t, err)
		assert.Len(t, res3, 1)
		assert.Equal(t, testID, res3[0].ID)

		res4, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName:  "TestClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    updateTimeDateFilter,
		})
		require.Nil(t, err)
		assert.Len(t, res4, 1)
		assert.Equal(t, testID, res4[0].ID)
	})
}

func msToRFC3339(ms int64) time.Time {
	sec, ns := splitMilliTimestamp(ms)
	return time.Unix(sec, ns)
}

// splitMilliTimestamp allows us to take a timestamp
// in unix epoch milliseconds, and split it into the
// needed seconds/nanoseconds required by `time.Unix`.
// once weaviate supports go version >= 1.17, we can
// remove this func and just pass `ms` to `time.UnixMilli`
func splitMilliTimestamp(ms int64) (sec int64, ns int64) {
	// remove 3 least significant digits of `ms`
	// so we end up with the seconds/nanoseconds
	// needed to convert to RFC3339 formatted
	// timestamp.
	for i := int64(0); i < 3; i++ {
		ns += int64(math.Pow(float64(10), float64(i))) * (ms % 10)
		ms /= 10
	}

	// after removing 3 least significant digits,
	// ms now represents the timestamp in seconds
	sec = ms

	// the least 3 significant digits only represent
	// milliseconds, and need to be converted to nano
	ns *= 1e6

	return
}
