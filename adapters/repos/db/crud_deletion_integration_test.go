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
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	libschema "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	schema := libschema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{updateTestClass()},
		},
	}

	t.Run("add schema", func(t *testing.T) {
		err := migrator.AddClass(context.Background(), updateTestClass(),
			schemaGetter.ShardingState(updateTestClass().Class))
		require.Nil(t, err)
	})
	schemaGetter.schema = schema

	t.Run("import some objects", func(t *testing.T) {
		for _, res := range updateTestData() {
			err := repo.PutObject(context.Background(), res.Object(), res.Vector)
			require.Nil(t, err)
		}
	})

	searchVector := []float32{0.1, 0.1, 0.1}

	t.Run("verify vector search results are initially as expected",
		func(t *testing.T) {
			res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil,
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
			}, additional.Properties{})
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

			err := repo.DeleteObject(context.Background(), "UpdateTestClass", id)
			require.Nil(t, err)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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

			err := repo.DeleteObject(context.Background(), "UpdateTestClass", id)
			require.Nil(t, err)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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

		err = repo.DeleteObject(context.Background(), "UpdateTestClass", id)
		require.Nil(t, err)

		index := repo.GetIndex("UpdateTestClass")
		require.NotNil(t, index)

		err = repo.DeleteIndex("UpdateTestClass")
		assert.Nil(t, err)
	})
}
