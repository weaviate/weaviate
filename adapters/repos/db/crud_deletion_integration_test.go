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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	libschema "github.com/weaviate/weaviate/entities/schema"
)

func TestDeleteJourney(t *testing.T) {
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
		err := migrator.AddClass(context.Background(), updateTestClass(),
			schemaGetter.CopyShardingState(updateTestClass().Class))
		require.Nil(t, err)
	})
	schemaGetter.schema = schema

	t.Run("import some objects", func(t *testing.T) {
		for _, res := range updateTestData() {
			err := repo.PutObject(context.Background(), res.Object(), res.Vector, nil)
			require.Nil(t, err)
		}
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

			expectedOrder := []interface{}{
				"element-0", "element-2", "element-3", "element-1",
			}

			require.Nil(t, err)
			require.Len(t, res, 4)
			assert.Equal(t, expectedOrder, extractPropValues(res, "name"))
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
			expectedOrder := []interface{}{
				"element-0", "element-1", "element-2", "element-3",
			}
			assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

			expectedOrder = []interface{}{"element-0"}
			assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 0))

			expectedOrder = []interface{}{"element-1"}
			assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 10))

			expectedOrder = []interface{}{"element-2"}
			assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 20))

			expectedOrder = []interface{}{"element-3"}
			assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 30))
		})

	t.Run("delete element-0",
		func(t *testing.T) {
			id := updateTestData()[0].ID

			err := repo.DeleteObject(context.Background(), "UpdateTestClass", id, nil, "")
			require.Nil(t, err)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Pagination: &filters.Pagination{
				Limit: 100,
			},
		})

		expectedOrder := []interface{}{
			"element-2", "element-3", "element-1",
		}

		require.Nil(t, err)
		require.Len(t, res, 3)
		assert.Equal(t, expectedOrder, extractPropValues(res, "name"))
	})

	t.Run("verify invert results still work properly", func(t *testing.T) {
		expectedOrder := []interface{}{
			"element-1", "element-2", "element-3",
		}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

		expectedOrder = []interface{}{"element-1"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 10))

		expectedOrder = []interface{}{"element-2"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 20))

		expectedOrder = []interface{}{"element-3"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 30))
	})

	t.Run("delete element-1",
		func(t *testing.T) {
			id := updateTestData()[1].ID

			err := repo.DeleteObject(context.Background(), "UpdateTestClass", id, nil, "")
			require.Nil(t, err)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Pagination: &filters.Pagination{
				Limit: 100,
			},
		})

		expectedOrder := []interface{}{
			"element-2", "element-3",
		}

		require.Nil(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, expectedOrder, extractPropValues(res, "name"))
	})

	t.Run("verify invert results have been updated correctly", func(t *testing.T) {
		expectedOrder := []interface{}{
			"element-2", "element-3",
		}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

		expectedOrder = []interface{}{"element-2"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 20))

		expectedOrder = []interface{}{"element-3"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 30))
	})

	t.Run("delete the index", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Pagination: &filters.Pagination{
				Limit: 100,
			},
		})

		expectedOrder := []interface{}{
			"element-2", "element-3",
		}

		require.Nil(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, expectedOrder, extractPropValues(res, "name"))

		id := updateTestData()[2].ID

		err = repo.DeleteObject(context.Background(), "UpdateTestClass", id, nil, "")
		require.Nil(t, err)

		index := repo.GetIndex("UpdateTestClass")
		require.NotNil(t, index)

		err = repo.DeleteIndex("UpdateTestClass")
		assert.Nil(t, err)
	})
}
