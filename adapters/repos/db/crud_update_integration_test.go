//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	libschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Updates are non trivial, because vector indices are built under the
// assumption that items are immutable (this is true for HNSW, the assumption
// is that this is generally true in the majority of cases). Therefore an
// update is essentially a delete and a new import with a new doc ID. This
// needs to be tested extensively because there's a lot of room for error
// regarding the clean up of Doc ID pointers in the inverted indices, etc.
func TestUpdateJourney(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	schema := libschema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{updateTestClass()},
		},
	}

	t.Run("add schema", func(t *testing.T) {
		err := migrator.AddClass(context.Background(), updateTestClass(), schemaGetter.shardState)
		require.Nil(t, err)
	})
	schemaGetter.schema = schema

	t.Run("import some objects", func(t *testing.T) {
		for _, res := range updateTestData() {
			err := repo.PutObject(context.Background(), res.Object(), res.Vector, nil)
			require.Nil(t, err)
		}

		tracker := getTracker(repo, "UpdateTestClass")

		require.Nil(t, err)

		sum, count, mean, err := tracker.PropertyTally("name")
		require.Nil(t, err)
		assert.Equal(t, 4, sum)
		assert.Equal(t, 4, count)
		assert.InEpsilon(t, 1, mean, 0.1)
	})

	searchVector := []float32{0.1, 0.1, 0.1}

	t.Run("verify vector search results are initially as expected",
		func(t *testing.T) {
			res, err := repo.VectorSearch(context.Background(), dto.GetParams{
				ClassName:    "UpdateTestClass",
				SearchVector: searchVector,
				Pagination: &filters.Pagination{
					Limit: 100,
				},
			})

			expectedInAnyOrder := []interface{}{
				"element-0", "element-1", "element-2", "element-3",
			}

			require.Nil(t, err)
			require.Len(t, res, 4)
			assert.ElementsMatch(t, expectedInAnyOrder, extractPropValues(res, "name"))
		})

	searchInv := func(t *testing.T, op filters.Operator, value int) []interface{} {
		res, err := repo.ObjectSearch(context.Background(), 0, 100,
			&filters.LocalFilter{
				Root: &filters.Clause{
					Operator: op,
					On: &filters.Path{
						Class:    "UpdateTestClass",
						Property: libschema.PropertyName("intProp"),
					},
					Value: &filters.Value{
						Type:  libschema.DataTypeInt,
						Value: value,
					},
				},
			}, nil, additional.Properties{}, "")
		require.Nil(t, err)
		return extractPropValues(res, "name")
	}

	t.Run("verify invert index results are initially as expected",
		func(t *testing.T) {
			expectedInAnyOrder := []interface{}{
				"element-0", "element-1", "element-2", "element-3",
			}
			assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

			expectedInAnyOrder = []interface{}{"element-0"}
			assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 0))

			expectedInAnyOrder = []interface{}{"element-1"}
			assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 10))

			expectedInAnyOrder = []interface{}{"element-2"}
			assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 20))

			expectedInAnyOrder = []interface{}{"element-3"}
			assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 30))
		})

	t.Run("update vector position of one item to move it into a different direction",
		func(t *testing.T) {
			// updating element-0 to be very far away from our search vector
			updatedVec := []float32{-0.1, -0.12, -0.105}
			id := updateTestData()[0].ID

			old, err := repo.ObjectByID(context.Background(), id, search.SelectProperties{}, additional.Properties{}, "")
			require.Nil(t, err)

			err = repo.PutObject(context.Background(), old.Object(), updatedVec, nil)
			require.Nil(t, err)

			tracker := getTracker(repo, "UpdateTestClass")

			require.Nil(t, err)

			sum, count, mean, err := tracker.PropertyTally("name")
			require.Nil(t, err)
			assert.Equal(t, 4, sum)
			assert.Equal(t, 4, count)
			assert.InEpsilon(t, 1, mean, 0.1)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Pagination: &filters.Pagination{
				Limit: 100,
			},
		})

		expectedInAnyOrder := []interface{}{
			"element-0", "element-1", "element-2", "element-3",
		}

		require.Nil(t, err)
		require.Len(t, res, 4)
		assert.ElementsMatch(t, expectedInAnyOrder, extractPropValues(res, "name"))
	})

	t.Run("verify invert results still work properly", func(t *testing.T) {
		expectedInAnyOrder := []interface{}{
			"element-0", "element-1", "element-2", "element-3",
		}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

		expectedInAnyOrder = []interface{}{"element-0"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 0))

		expectedInAnyOrder = []interface{}{"element-1"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 10))

		expectedInAnyOrder = []interface{}{"element-2"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 20))

		expectedInAnyOrder = []interface{}{"element-3"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 30))
	})

	t.Run("update a second object and modify vector and invert props at the same time",
		func(t *testing.T) {
			// this time we are updating element-2 and move it away from the search
			// vector, as well as updating an invert prop

			updatedVec := []float32{-0.1, -0.12, -0.105123}
			id := updateTestData()[2].ID

			old, err := repo.ObjectByID(context.Background(), id, search.SelectProperties{}, additional.Properties{}, "")
			require.Nil(t, err)

			old.Schema.(map[string]interface{})["intProp"] = int64(21)
			err = repo.PutObject(context.Background(), old.Object(), updatedVec, nil)
			require.Nil(t, err)

			tracker := getTracker(repo, "UpdateTestClass")

			require.Nil(t, err)

			sum, count, mean, err := tracker.PropertyTally("name")
			require.Nil(t, err)
			assert.Equal(t, 4, sum)
			assert.Equal(t, 4, count)
			assert.InEpsilon(t, 1, mean, 0.1)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Pagination: &filters.Pagination{
				Limit: 100,
			},
		})

		expectedInAnyOrder := []interface{}{
			"element-0", "element-1", "element-2", "element-3",
		}

		require.Nil(t, err)
		require.Len(t, res, 4)
		assert.ElementsMatch(t, expectedInAnyOrder, extractPropValues(res, "name"))
	})

	t.Run("verify invert results have been updated correctly", func(t *testing.T) {
		expectedInAnyOrder := []interface{}{
			"element-0", "element-1", "element-2", "element-3",
		}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

		expectedInAnyOrder = []interface{}{"element-0"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 0))

		expectedInAnyOrder = []interface{}{"element-1"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 10))

		expectedInAnyOrder = []interface{}{} // value is no longer 20, but 21
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 20))

		expectedInAnyOrder = []interface{}{"element-2"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 21))

		expectedInAnyOrder = []interface{}{"element-3"}
		assert.ElementsMatch(t, expectedInAnyOrder, searchInv(t, filters.OperatorEqual, 30))
	})

	t.Run("test recount", func(t *testing.T) {
		tracker := getTracker(repo, "UpdateTestClass")

		require.Nil(t, err)

		sum, count, mean, err := tracker.PropertyTally("name")
		require.Nil(t, err)
		assert.Equal(t, 4, sum)
		assert.Equal(t, 4, count)
		assert.InEpsilon(t, 1, mean, 0.1)

		tracker.Clear()
		sum, count, mean, err = tracker.PropertyTally("name")
		require.Nil(t, err)
		assert.Equal(t, 0, sum)
		assert.Equal(t, 0, count)
		assert.Equal(t, float64(0), mean)

		logger := logrus.New()
		migrator := NewMigrator(repo, logger)
		migrator.RecountProperties(context.Background())

		sum, count, mean, err = tracker.PropertyTally("name")
		require.Nil(t, err)
		assert.Equal(t, 4, sum)
		assert.Equal(t, 4, count)
		assert.Equal(t, float64(1), mean)
	})
}

func updateTestClass() *models.Class {
	return &models.Class{
		Class:             "UpdateTestClass",
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 3,
		},
		Properties: []*models.Property{
			{
				DataType: []string{string(schema.DataTypeInt)},
				Name:     "intProp",
			},
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
		},
	}
}

func updateTestData() search.Results {
	return search.Results{
		search.Result{
			ClassName: "UpdateTestClass",
			ID:        "426b0b29-9ded-40b6-b786-da3d1fec412f",
			Schema: map[string]interface{}{
				"intProp": int64(0),
				"name":    "element-0",
			},
			Vector: []float32{0.89379513, 0.67022973, 0.57360715},
		},
		search.Result{
			ClassName: "UpdateTestClass",
			ID:        "a1560f12-f0f0-4439-b5b8-b7bcecf5fed7",

			Schema: map[string]interface{}{
				"intProp": int64(10),
				"name":    "element-1",
			},
			Vector: []float32{0.9660323, 0.35887036, 0.6072966},
		},
		search.Result{
			ClassName: "UpdateTestClass",
			ID:        "0c73f145-5dc4-49a9-bd58-82725f8b13fa",

			Schema: map[string]interface{}{
				"intProp": int64(20),
				"name":    "element-2",
			},
			Vector: []float32{0.8194746, 0.56142205, 0.5130103},
		},
		search.Result{
			ClassName: "UpdateTestClass",
			ID:        "aec8462e-276a-4989-a612-8314c35d163a",
			Schema: map[string]interface{}{
				"intProp": int64(30),
				"name":    "element-3",
			},
			Vector: []float32{0.42401955, 0.8278863, 0.5952888},
		},
	}
}

func extractPropValues(in search.Results, propName string) []interface{} {
	out := make([]interface{}, len(in))

	for i, res := range in {
		out[i] = res.Schema.(map[string]interface{})[propName]
	}

	return out
}

func getTracker(repo *DB, className string) *inverted.JsonShardMetaData {
	index := repo.GetIndex("UpdateTestClass")
	var shard ShardLike
	index.ForEachShard(func(name string, shardv ShardLike) error {
		shard = shardv
		return nil
	})

	tracker := shard.GetPropertyLengthTracker()

	return tracker
}
