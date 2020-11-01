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
	"github.com/semi-technologies/weaviate/entities/schema"
	libschema "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Updates are non trivial, because vector indices are built under the
// assumption that items are immutable (this is true for HNSW, the assumption
// is that this is generally true in the majority of cases). Therefore an
// update is essentially a delete and a new import with a new doc ID. This
// needs to be tested extensively because there's a lot of room for error
// regarding the clean up of Doc ID pointers in the inverted indices, etc.
func TestUpdateJourney(t *testing.T) {
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

	t.Run("update vector position of one item to move it into a different direction",
		func(t *testing.T) {
			// updating element-0 to be very far away from our search vector
			updatedVec := []float32{-0.1, -0.12, -0.105}
			id := updateTestData()[0].ID

			old, err := repo.ThingByID(context.Background(), id, traverser.SelectProperties{},
				traverser.UnderscoreProperties{})
			require.Nil(t, err)

			err = repo.PutThing(context.Background(), old.Thing(), updatedVec)
			require.Nil(t, err)
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
			"element-2", "element-3", "element-1", "element-0",
		}

		require.Nil(t, err)
		require.Len(t, res, 4)
		assert.Equal(t, expectedOrder, extractPropValues(res, "name"))
	})

	t.Run("verify invert results still work properly", func(t *testing.T) {
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

	t.Run("update a second object and modify vector and invert props at the same time",
		func(t *testing.T) {
			// this time we are updating element-2 and move it away from the search
			// vector, as well as updating an invert prop

			updatedVec := []float32{-0.1, -0.12, -0.105123}
			id := updateTestData()[2].ID

			old, err := repo.ThingByID(context.Background(), id, traverser.SelectProperties{},
				traverser.UnderscoreProperties{})
			require.Nil(t, err)

			old.Schema.(map[string]interface{})["intProp"] = int64(21)

			err = repo.PutThing(context.Background(), old.Thing(), updatedVec)
			require.Nil(t, err)
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
			"element-3", "element-1", "element-0", "element-2",
		}

		require.Nil(t, err)
		require.Len(t, res, 4)
		assert.Equal(t, expectedOrder, extractPropValues(res, "name"))
	})

	t.Run("verify invert results have been updated correctly", func(t *testing.T) {
		expectedOrder := []interface{}{
			"element-0", "element-1", "element-2", "element-3",
		}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorGreaterThanEqual, 0))

		expectedOrder = []interface{}{"element-0"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 0))

		expectedOrder = []interface{}{"element-1"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 10))

		expectedOrder = []interface{}{} // value is no longer 20, but 21
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 20))

		expectedOrder = []interface{}{"element-2"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 21))

		expectedOrder = []interface{}{"element-3"}
		assert.Equal(t, expectedOrder, searchInv(t, filters.OperatorEqual, 30))
	})
}

func updateTestClass() *models.Class {
	return &models.Class{
		Class: "UpdateTestClass",
		Properties: []*models.Property{
			&models.Property{
				DataType: []string{string(schema.DataTypeInt)},
				Name:     "intProp",
			},
			&models.Property{
				DataType: []string{string(schema.DataTypeString)},
				Name:     "name",
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
