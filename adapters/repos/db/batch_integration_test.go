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
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchPutObjects(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0777)
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
	migrator := NewMigrator(repo)

	t.Run("creating the thing class", testAddBatchThingClass(repo, migrator,
		schemaGetter))
	t.Run("creating the action class", testAddBatchActionClass(repo, migrator,
		schemaGetter))
	t.Run("batch import things", testBatchImportThings(repo))

}

func testAddBatchThingClass(repo *DB, migrator *Migrator,
	schemaGetter *fakeSchemaGetter) func(t *testing.T) {
	return func(t *testing.T) {
		class := &models.Class{
			Class: "ThingForBatching",
			Properties: []*models.Property{
				&models.Property{
					Name:     "stringProp",
					DataType: []string{string(schema.DataTypeString)},
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Thing, class))

		schemaGetter.schema.Things = &models.Schema{
			Classes: []*models.Class{class},
		}
	}
}

func testAddBatchActionClass(repo *DB, migrator *Migrator,
	schemaGetter *fakeSchemaGetter) func(t *testing.T) {
	return func(t *testing.T) {
		class := &models.Class{
			Class: "ActionForBatching",
			Properties: []*models.Property{
				&models.Property{
					Name:     "stringProp",
					DataType: []string{string(schema.DataTypeString)},
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Action, class))

		schemaGetter.schema.Actions = &models.Schema{
			Classes: []*models.Class{class},
		}
	}
}

func testBatchImportThings(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("with a prior validation error, but nothing to cause errors in the db", func(t *testing.T) {
			batch := kinds.BatchThings{
				kinds.BatchThing{
					OriginalIndex: 0,
					Err:           nil,
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					},
					UUID:   "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					Vector: []float32{1, 2, 3},
				},
				kinds.BatchThing{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already has a validation error"),
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "second element",
						},
						ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
					},
					UUID:   "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
					Vector: []float32{1, 2, 3},
				},
				kinds.BatchThing{
					OriginalIndex: 2,
					Err:           nil,
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "third element",
						},
						ID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
					},
					UUID:   "90ade18e-2b99-4903-aa34-1d5d648c932d",
					Vector: []float32{1, 2, 3},
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutThings(context.Background(), batch)
				require.Nil(t, err)

				assert.Nil(t, batchRes[0].Err)
				assert.Nil(t, batchRes[2].Err)
			})

			params := traverser.GetParams{
				Kind:       kind.Thing,
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)

			t.Run("contains first element", func(t *testing.T) {
				item, ok := findID(res, batch[0].Thing.ID)
				require.Equal(t, true, ok, "results should contain our desired id")
				assert.Equal(t, "first element", item.Schema.(map[string]interface{})["stringProp"])
			})

			t.Run("contains third element", func(t *testing.T) {
				item, ok := findID(res, batch[2].Thing.ID)
				require.Equal(t, true, ok, "results should contain our desired id")
				assert.Equal(t, "third element", item.Schema.(map[string]interface{})["stringProp"])
			})
		})

		t.Run("with an import which will fail", func(t *testing.T) {
			batch := kinds.BatchThings{
				kinds.BatchThing{
					OriginalIndex: 0,
					Err:           nil,
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
					},
					UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
				},
				kinds.BatchThing{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already had a prior error"),
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
					},
					UUID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
				},
				kinds.BatchThing{
					OriginalIndex: 2,
					Err:           nil,
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "second element",
						},
						ID: "", // ID can't be empty in es, this should produce an error
					},
					UUID: "",
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutThings(context.Background(), batch)
				require.Nil(t, err, "there shouldn't be an overall error, only inividual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 3)
					assert.NotNil(t, batchRes[1].Err) // from validation
					assert.NotNil(t, batchRes[2].Err) // from db
				})
			})

			params := traverser.GetParams{
				Kind:       kind.Thing,
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)

			t.Run("does not contain second element (validation error)", func(t *testing.T) {
				_, ok := findID(res, batch[1].Thing.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})

			t.Run("does not contain third element (es error)", func(t *testing.T) {
				_, ok := findID(res, batch[2].Thing.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})
		})
	}
}

func testBatchImportActions(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		batch := kinds.BatchActions{
			kinds.BatchAction{
				OriginalIndex: 0,
				Err:           nil,
				Action: &models.Action{
					Class: "ActionForBatching",
					Schema: map[string]interface{}{
						"stringProp": "first element",
					},
					ID: "6e90812c-5d56-4e44-8ad2-aac9b992beba",
				},
				UUID: "6e90812c-5d56-4e44-8ad2-aac9b992beba",
			},
			kinds.BatchAction{
				OriginalIndex: 1,
				Err:           fmt.Errorf("already has a validation error"),
				Action: &models.Action{
					Class: "ActionForBatching",
					Schema: map[string]interface{}{
						"stringProp": "second element",
					},
					ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
				},
				UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
			},
			kinds.BatchAction{
				OriginalIndex: 2,
				Err:           nil,
				Action: &models.Action{
					Class: "ActionForBatching",
					Schema: map[string]interface{}{
						"stringProp": "third element",
					},
					ID: "d739abd8-4433-46f9-bc10-93e89cb9d2c6",
				},
				UUID: "d739abd8-4433-46f9-bc10-93e89cb9d2c6",
			},
		}

		t.Run("can import", func(t *testing.T) {
			_, err := repo.BatchPutActions(context.Background(), batch)
			require.Nil(t, err)
		})

		params := traverser.GetParams{
			Kind:       kind.Action,
			ClassName:  "ActionForBatching",
			Pagination: &filters.Pagination{Limit: 10},
			Filters:    nil,
		}
		res, err := repo.ClassSearch(context.Background(), params)
		require.Nil(t, err)
		require.Len(t, res, 2)

		t.Run("contains first element", func(t *testing.T) {
			item, ok := findID(res, batch[0].Action.ID)
			require.Equal(t, true, ok, "results should contain our desired id")
			assert.Equal(t, "first element", item.Schema.(map[string]interface{})["stringProp"])
		})

		t.Run("contains first element", func(t *testing.T) {
			item, ok := findID(res, batch[2].Action.ID)
			require.Equal(t, true, ok, "results should contain our desired id")
			assert.Equal(t, "third element", item.Schema.(map[string]interface{})["stringProp"])
		})
	}
}
