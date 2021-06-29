//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
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
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/objects"
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
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator,
		schemaGetter))
	t.Run("batch import things", testBatchImportObjects(repo))
	t.Run("batch import things with geo props", testBatchImportGeoObjects(repo))
}

func testAddBatchObjectClass(repo *DB, migrator *Migrator,
	schemaGetter *fakeSchemaGetter) func(t *testing.T) {
	return func(t *testing.T) {
		class := &models.Class{
			Class:               "ThingForBatching",
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:     "stringProp",
					DataType: []string{string(schema.DataTypeString)},
				},
				{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), class))

		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{class},
		}
	}
}

func testBatchImportObjects(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("with a prior validation error, but nothing to cause errors in the db", func(t *testing.T) {
			batch := objects.BatchObjects{
				objects.BatchObject{
					OriginalIndex: 0,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					},
					UUID:   "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
					Vector: []float32{1, 2, 3},
				},
				objects.BatchObject{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already has a validation error"),
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "second element",
						},
						ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
					},
					UUID:   "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
					Vector: []float32{1, 2, 3},
				},
				objects.BatchObject{
					OriginalIndex: 2,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "third element",
						},
						ID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
					},
					UUID:   "90ade18e-2b99-4903-aa34-1d5d648c932d",
					Vector: []float32{1, 2, 3},
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch)
				require.Nil(t, err)

				assert.Nil(t, batchRes[0].Err)
				assert.Nil(t, batchRes[2].Err)
			})

			params := traverser.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)

			t.Run("contains first element", func(t *testing.T) {
				item, ok := findID(res, batch[0].Object.ID)
				require.Equal(t, true, ok, "results should contain our desired id")
				assert.Equal(t, "first element", item.Schema.(map[string]interface{})["stringProp"])
			})

			t.Run("contains third element", func(t *testing.T) {
				item, ok := findID(res, batch[2].Object.ID)
				require.Equal(t, true, ok, "results should contain our desired id")
				assert.Equal(t, "third element", item.Schema.(map[string]interface{})["stringProp"])
			})

			t.Run("can be queried through the inverted index", func(t *testing.T) {
				filter := buildFilter("stringProp", "third", eq, dtString)
				params := traverser.GetParams{
					ClassName:  "ThingForBatching",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    filter,
				}
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)

				require.Len(t, res, 1)
				assert.Equal(t, strfmt.UUID("90ade18e-2b99-4903-aa34-1d5d648c932d"),
					res[0].ID)
			})
		})

		t.Run("with an import which will fail", func(t *testing.T) {
			batch := objects.BatchObjects{
				objects.BatchObject{
					OriginalIndex: 0,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "first element",
						},
						ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
					},
					UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
				},
				objects.BatchObject{
					OriginalIndex: 1,
					Err:           fmt.Errorf("already had a prior error"),
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "second element",
						},
						ID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
					},
					UUID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
				},
				objects.BatchObject{
					OriginalIndex: 2,
					Err:           nil,
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "third element",
						},
						ID: "", // ID can't be empty in es, this should produce an error
					},
					UUID: "",
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch)
				require.Nil(t, err, "there shouldn't be an overall error, only inividual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 3)
					assert.NotNil(t, batchRes[1].Err) // from validation
					assert.NotNil(t, batchRes[2].Err) // from db
				})
			})

			params := traverser.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)

			t.Run("does not contain second element (validation error)", func(t *testing.T) {
				_, ok := findID(res, batch[1].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})

			t.Run("does not contain third element (es error)", func(t *testing.T) {
				_, ok := findID(res, batch[2].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})
		})

		t.Run("with a duplicate UUID", func(t *testing.T) {
			// it should ignore the first one as the second one would overwrite the
			// first one anyway
			batch := make(objects.BatchObjects, 53)

			batch[0] = objects.BatchObject{
				OriginalIndex: 0,
				Err:           nil,
				Vector:        []float32{7, 8, 9},
				Object: &models.Object{
					Class: "ThingForBatching",
					Properties: map[string]interface{}{
						"stringProp": "first element",
					},
					ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
				},
				UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3",
			}

			// add 50 more nonsensical items, so we cross the transaction threshold

			for i := 1; i < 51; i++ {
				uid, err := uuid.NewRandom()
				require.Nil(t, err)
				id := strfmt.UUID(uid.String())
				batch[i] = objects.BatchObject{
					OriginalIndex: i,
					Err:           nil,
					Vector:        []float32{0.05, 0.1, 0.2},
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "ignore me",
						},
						ID: id,
					},
					UUID: id,
				}
			}

			batch[51] = objects.BatchObject{
				OriginalIndex: 51,
				Err:           fmt.Errorf("already had a prior error"),
				Vector:        []float32{3, 2, 1},
				Object: &models.Object{
					Class: "ThingForBatching",
					Properties: map[string]interface{}{
						"stringProp": "first element",
					},
					ID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
				},
				UUID: "1c2d8ce6-32da-4081-9794-a81e23e673e4",
			}
			batch[52] = objects.BatchObject{
				OriginalIndex: 52,
				Err:           nil,
				Vector:        []float32{1, 2, 3},
				Object: &models.Object{
					Class: "ThingForBatching",
					Properties: map[string]interface{}{
						"stringProp": "first element, imported a second time",
					},
					ID: "79aebd44-7486-4fed-9334-3a74cc09a1c3", // note the duplicate id with item 1
				},
				UUID: "79aebd44-7486-4fed-9334-3a74cc09a1c3", // note the duplicate id with item 1
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch)
				require.Nil(t, err, "there shouldn't be an overall error, only inividual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 53)
					assert.NotNil(t, batchRes[51].Err) // from validation
				})
			})

			params := traverser.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.ClassSearch(context.Background(), params)
			require.Nil(t, err)

			t.Run("does not contain second element (validation error)", func(t *testing.T) {
				_, ok := findID(res, batch[51].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})

			t.Run("does not contain third element (es error)", func(t *testing.T) {
				_, ok := findID(res, batch[52].Object.ID)
				require.Equal(t, false, ok, "results should not contain our desired id")
			})
		})

		t.Run("when a context expires", func(t *testing.T) {
			// it should ignore the first one as the second one would overwrite the
			// first one anyway
			size := 50
			batch := make(objects.BatchObjects, size)
			// add 50 more nonsensical items, so we cross the transaction threshold

			for i := 0; i < size; i++ {
				uid, err := uuid.NewRandom()
				require.Nil(t, err)
				id := strfmt.UUID(uid.String())
				batch[i] = objects.BatchObject{
					Err:    nil,
					Vector: []float32{0.05, 0.1, 0.2},
					Object: &models.Object{
						Class: "ThingForBatching",
						Properties: map[string]interface{}{
							"stringProp": "ignore me",
						},
						ID: id,
					},
					UUID: id,
				}
			}

			t.Run("can import", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
				defer cancel()

				batchRes, err := repo.BatchPutObjects(ctx, batch)
				require.Nil(t, err, "there shouldn't be an overall error, only inividual ones")

				t.Run("some elements have error'd due to context", func(t *testing.T) {
					require.Len(t, batchRes, 50)

					errCount := 0
					for _, elem := range batchRes {
						if elem.Err != nil {
							errCount++
							assert.Contains(t, elem.Err.Error(), "context deadline exceeded")
						}
					}

					assert.True(t, errCount > 0)
				})
			})
		})
	}
}

// geo props are the first props with property specific indices, so making sure
// that they work with batches at scale adds value beyond the regular batch
// import tests
func testBatchImportGeoObjects(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		size := 500
		batchSize := 50

		objs := make([]*models.Object, size)

		t.Run("generate random vectors", func(t *testing.T) {
			for i := 0; i < size; i++ {
				id, _ := uuid.NewRandom()
				objs[i] = &models.Object{
					Class: "ThingForBatching",
					ID:    strfmt.UUID(id.String()),
					Properties: map[string]interface{}{
						"location": randGeoCoordinates(),
					},
					Vector: []float32{0.123, 0.234, rand.Float32()}, // does not matter for this test
				}
			}
		})

		t.Run("import vectors in batches", func(t *testing.T) {
			for i := 0; i < size; i += batchSize {
				batch := make(objects.BatchObjects, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = objects.BatchObject{
						OriginalIndex: j,
						Object:        objs[i+j],
						Vector:        objs[i+j].Vector,
					}
				}

				res, err := repo.BatchPutObjects(context.Background(), batch)
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

					controlList := bruteForceMaxDist(objs, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
			for i, obj := range objs {
				obj.Properties = map[string]interface{}{
					"location": randGeoCoordinates(),
				}
				objs[i] = obj
			}
		})

		t.Run("import in batches again (as update - same IDs!)", func(t *testing.T) {
			for i := 0; i < size; i += batchSize {
				batch := make(objects.BatchObjects, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = objects.BatchObject{
						OriginalIndex: j,
						Object:        objs[i+j],
						Vector:        objs[i+j].Vector,
					}
				}

				res, err := repo.BatchPutObjects(context.Background(), batch)
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

					controlList := bruteForceMaxDist(objs, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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

func assertAllItemsErrorFree(t *testing.T, res objects.BatchObjects) {
	for _, elem := range res {
		assert.Nil(t, elem.Err)
	}
}

func bruteForceMaxDist(inputs []*models.Object, query []float32, maxDist float32) []strfmt.UUID {
	type distanceAndIndex struct {
		distance float32
		index    int
	}

	distances := make([]distanceAndIndex, len(inputs))

	distancer := distancer.NewGeoProvider().New(query)
	for i, elem := range inputs {
		coord := elem.Properties.(map[string]interface{})["location"].(*models.GeoCoordinates)
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
