//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	libschema "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
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
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	schema := libschema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{updateTestClass()},
		},
	}

	t.Run("add schema", func(t *testing.T) {
		err := migrator.AddClass(context.Background(), kind.Thing, updateTestClass())
		require.Nil(t, err)
	})
	schemaGetter.schema = schema

	t.Run("import some objects", func(t *testing.T) {
		for _, res := range updateTestData() {
			err := repo.PutThing(context.Background(), res.Thing(), res.Vector)
			require.Nil(t, err)
		}
	})

	searchVector := []float32{0.1, 0.1, 0.1}

	t.Run("verify vector search results are initially as expected",
		func(t *testing.T) {
			res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
				ClassName:    "UpdateTestClass",
				SearchVector: searchVector,
				Kind:         kind.Thing,
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
		res, err := repo.ThingSearch(context.Background(), 100,
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
			}, traverser.UnderscoreProperties{})
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

	t.Run("delete element-0 and verify that there's 1 document flagged for deletion",
		func(t *testing.T) {
			id := updateTestData()[0].ID

			err := repo.DeleteThing(context.Background(), "UpdateTestClass", id)
			require.Nil(t, err)

			index := repo.GetIndex(kind.Thing, "UpdateTestClass")
			require.NotNil(t, index)

			deletedIDsCount := len(index.Shards["single"].deletedDocIDs.GetAll())
			assert.Equal(t, 1, deletedIDsCount)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Kind:         kind.Thing,
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

	t.Run("delete element-1 and verify that there are 2 documents flagged for deletion",
		func(t *testing.T) {
			id := updateTestData()[1].ID

			err := repo.DeleteThing(context.Background(), "UpdateTestClass", id)
			require.Nil(t, err)

			index := repo.GetIndex(kind.Thing, "UpdateTestClass")
			require.NotNil(t, index)

			deletedIDsCount := len(index.Shards["single"].deletedDocIDs.GetAll())
			assert.Equal(t, 2, deletedIDsCount)
		})

	t.Run("verify new vector search results are as expected", func(t *testing.T) {
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
			ClassName:    "UpdateTestClass",
			SearchVector: searchVector,
			Kind:         kind.Thing,
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

	t.Run("wait 70 seconds until the clenup is done and verify that it has performed",
		func(t *testing.T) {
			ticker := time.Tick(70 * time.Second)
			<-ticker

			index := repo.GetIndex(kind.Thing, "UpdateTestClass")
			require.NotNil(t, index)

			deletedIDsCount := len(index.Shards["single"].deletedDocIDs.GetAll())
			assert.Equal(t, 0, deletedIDsCount)
		})
}
