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
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchPutObjects(t *testing.T) {
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

	t.Run("creating the thing class", testAddBatchThingClass(repo, migrator,
		schemaGetter))
	t.Run("creating the action class", testAddBatchActionClass(repo, migrator,
		schemaGetter))
	t.Run("batch import things", testBatchImportThings(repo))
	t.Run("batch import things with geo props", testBatchImportGeoThings(repo))
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
				&models.Property{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
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

		t.Run("with a duplicate UUID", func(t *testing.T) {
			// it should ignore the first one as the second one would overwrite the
			// first one anyway
			batch := make(kinds.BatchThings, 53)

			batch[0] = kinds.BatchThing{
				OriginalIndex: 0,
				Err:           nil,
				Vector:        []float32{7, 8, 9},
				Thing: &models.Thing{
					Class: "ThingForBatching",
					Schema: map[string]interface{}{
						"stringProp": "first element",
					},
					ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
				},
				UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
			}

			// add 50 more nonsensical items, so we cross the transaction threshold

			for i := 1; i < 51; i++ {
				uuid, err := uuid.NewV4()
				require.Nil(t, err)
				id := strfmt.UUID(uuid.String())
				batch[i] = kinds.BatchThing{
					OriginalIndex: i,
					Err:           nil,
					Vector:        []float32{0.05, 0.1, 0.2},
					Thing: &models.Thing{
						Class: "ThingForBatching",
						Schema: map[string]interface{}{
							"stringProp": "ignore me",
						},
						ID: id,
					},
					UUID: id,
				}
			}

			batch[51] = kinds.BatchThing{
				OriginalIndex: 51,
				Err:           fmt.Errorf("already had a prior error"),
				Vector:        []float32{3, 2, 1},
				Thing: &models.Thing{
					Class: "ThingForBatching",
					Schema: map[string]interface{}{
						"stringProp": "first element",
					},
					ID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
				},
				UUID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
			}
			batch[52] = kinds.BatchThing{
				OriginalIndex: 52,
				Err:           nil,
				Vector:        []float32{1, 2, 3},
				Thing: &models.Thing{
					Class: "ThingForBatching",
					Schema: map[string]interface{}{
						"stringProp": "first element, imported a second time",
					},
					ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3", // note the duplicate id with item 1
				},
				UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3", // note the duplicate id with item 1
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutThings(context.Background(), batch)
				require.Nil(t, err, "there shouldn't be an overall error, only inividual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 53)
					assert.NotNil(t, batchRes[51].Err) // from validation
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
				_, ok := findID(res, batch[51].Thing.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})

			t.Run("does not contain third element (es error)", func(t *testing.T) {
				_, ok := findID(res, batch[52].Thing.ID)
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

// geo props are the first props with property specific indices, so making sure
// that they work with batches at scale adds value beyond the regular batch
// import tests
func testBatchImportGeoThings(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		size := 500
		batchSize := 50

		objects := make([]*models.Thing, size)

		t.Run("generate random vectors", func(t *testing.T) {
			for i := 0; i < size; i++ {
				id, _ := uuid.NewV4()
				objects[i] = &models.Thing{
					Class: "ThingForBatching",
					ID:    strfmt.UUID(id.String()),
					Schema: map[string]interface{}{
						"location": randGeoCoordinates(),
					},
					Vector: []float32{0.123, 0.234, 0.345}, // does not matter for this test
				}
			}
		})

		t.Run("import vectors in batches", func(t *testing.T) {
			for i := 0; i < size; i += batchSize {
				batch := make(kinds.BatchThings, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = kinds.BatchThing{
						OriginalIndex: j,
						Thing:         objects[i+j],
						Vector:        objects[i+j].Vector,
					}
				}

				res, err := repo.BatchPutThings(context.Background(), batch)
				require.Nil(t, err)
				assertAllItemsErrorFree(t, res)
			}
		})

		const km = 1000
		distances := []float32{
			0.1,
			1,
			10,
			100,
			1000,
			2000,
			5000,
			7500,
			10000,
			12500,
			15000,
			20000,
			35000,
			100000, // larger than the circumference of the earth, should contain all
		}

		t.Run("query for expected results", func(t *testing.T) {
			queryGeo := randGeoCoordinates()

			for _, maxDist := range distances {
				t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
					var relevant int
					var retrieved int

					controlList := bruteForceMaxDist(objects, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
						Kind:       kind.Thing,
						ClassName:  "ThingForBatching",
						Pagination: &filters.Pagination{Limit: 500},
						Filters: buildFilter("location", filters.GeoRange{
							GeoCoordinates: queryGeo,
							Distance:       maxDist * km,
						}, filters.OperatorWithinGeoRange, schema.DataTypeGeoCoordinates),
					})
					require.Nil(t, err)

					retrieved += len(res)
					relevant += matchesInUUIDLists(controlList, resToUUIDs(res))

					if relevant == 0 {
						// skip, as we risk dividing by zero, if both relevant and retrieved
						// are zero, however, we want to fail with a divide-by-zero if only
						// retrieved is 0 and relevant was more than 0
						return
					}
					recall := float32(relevant) / float32(retrieved)
					fmt.Printf("recall is %f\n", recall)
					assert.True(t, recall >= 0.99)
				})
			}
		})

		t.Run("renew vector positions to test batch geo updates", func(t *testing.T) {
			for i, obj := range objects {
				obj.Schema = map[string]interface{}{
					"location": randGeoCoordinates(),
				}
				objects[i] = obj
			}
		})

		t.Run("import in batches again (as update - same IDs!)", func(t *testing.T) {
			for i := 0; i < size; i += batchSize {
				batch := make(kinds.BatchThings, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = kinds.BatchThing{
						OriginalIndex: j,
						Thing:         objects[i+j],
						Vector:        objects[i+j].Vector,
					}
				}

				res, err := repo.BatchPutThings(context.Background(), batch)
				require.Nil(t, err)
				assertAllItemsErrorFree(t, res)
			}
		})

		t.Run("query again to verify updates worked", func(t *testing.T) {
			queryGeo := randGeoCoordinates()

			for _, maxDist := range distances {
				t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
					var relevant int
					var retrieved int

					controlList := bruteForceMaxDist(objects, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
						Kind:       kind.Thing,
						ClassName:  "ThingForBatching",
						Pagination: &filters.Pagination{Limit: 500},
						Filters: buildFilter("location", filters.GeoRange{
							GeoCoordinates: queryGeo,
							Distance:       maxDist * km,
						}, filters.OperatorWithinGeoRange, schema.DataTypeGeoCoordinates),
					})
					require.Nil(t, err)

					retrieved += len(res)
					relevant += matchesInUUIDLists(controlList, resToUUIDs(res))

					if relevant == 0 {
						// skip, as we risk dividing by zero, if both relevant and retrieved
						// are zero, however, we want to fail with a divide-by-zero if only
						// retrieved is 0 and relevant was more than 0
						return
					}
					recall := float32(relevant) / float32(retrieved)
					fmt.Printf("recall is %f\n", recall)
					assert.True(t, recall >= 0.99)
				})
			}
		})
	}
}

func assertAllItemsErrorFree(t *testing.T, res kinds.BatchThings) {
	for _, elem := range res {
		assert.Nil(t, elem.Err)
	}
}

func bruteForceMaxDist(inputs []*models.Thing, query []float32, maxDist float32) []strfmt.UUID {
	type distanceAndIndex struct {
		distance float32
		index    int
	}

	distances := make([]distanceAndIndex, len(inputs))

	distancer := distancer.NewGeoProvider().New(query)
	for i, elem := range inputs {
		coord := elem.Schema.(map[string]interface{})["location"].(*models.GeoCoordinates)
		vec := []float32{*coord.Latitude, *coord.Longitude}

		dist, _, _ := distancer.Distance(vec)
		distances[i] = distanceAndIndex{
			index:    i,
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]strfmt.UUID, len(distances))
	i := 0
	for _, elem := range distances {
		if elem.distance > maxDist {
			break
		}
		out[i] = inputs[distances[i].index].ID
		i++
	}

	return out[:i]
}

func randGeoCoordinates() *models.GeoCoordinates {
	maxLat := float32(90.0)
	minLat := float32(-90.0)
	maxLon := float32(180)
	minLon := float32(-180)

	lat := minLat + (maxLat-minLat)*rand.Float32()
	lon := minLon + (maxLon-minLon)*rand.Float32()
	return &models.GeoCoordinates{
		Latitude:  &lat,
		Longitude: &lon,
	}
}

func resToUUIDs(in []search.Result) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in))
	for i, obj := range in {
		out[i] = obj.ID
	}

	return out
}

func matchesInUUIDLists(control []strfmt.UUID, results []strfmt.UUID) int {
	desired := map[strfmt.UUID]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches int
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}
