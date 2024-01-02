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
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestBatchPutObjectsWithDimensions(t *testing.T) {
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
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	dimBefore := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 0, dimBefore, "Dimensions are empty before import")

	simpleInsertObjects(t, repo, "ThingForBatching", 123)

	dimAfter := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 369, dimAfter, "Dimensions are present after import")
}

func TestBatchPutObjects(t *testing.T) {
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
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	t.Run("batch import things", testBatchImportObjects(repo))
	t.Run("batch import things with geo props", testBatchImportGeoObjects(repo))
}

func TestBatchPutObjectsNoVectorsWithDimensions(t *testing.T) {
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
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator,
		schemaGetter))

	dimensions := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 0, dimensions, "Dimensions are empty before import")

	t.Run("batch import things", testBatchImportObjectsNoVector(repo))

	dimAfter := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 0, dimAfter, "Dimensions are empty after import (no vectors in import)")
}

func TestBatchPutObjectsNoVectors(t *testing.T) {
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

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	t.Run("batch import things", testBatchImportObjectsNoVector(repo))
}

func TestBatchDeleteObjectsWithDimensions(t *testing.T) {
	className := "ThingForBatching"
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   1,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()

	migrator := NewMigrator(repo, logger)

	t.Run("creating the test class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	dimBefore := GetDimensionsFromRepo(repo, className)
	require.Equal(t, 0, dimBefore, "Dimensions are empty before import")

	simpleInsertObjects(t, repo, className, 103)

	dimAfter := GetDimensionsFromRepo(repo, className)
	require.Equal(t, 309, dimAfter, "Dimensions are present before delete")

	delete2Objects(t, repo, className)

	dimFinal := GetDimensionsFromRepo(repo, className)
	require.Equal(t, 303, dimFinal, "2 objects have been deleted")
}

func delete2Objects(t *testing.T, repo *DB, className string) {
	batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), objects.BatchDeleteParams{
		ClassName: "ThingForBatching",
		Filters: &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorOr,
				Operands: []filters.Clause{
					{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "ThingForBatching",
							Property: schema.PropertyName("id"),
						},
						Value: &filters.Value{
							Value: "8d5a3aa2-3c8d-4589-9ae1-3f638f506003",
							Type:  schema.DataTypeText,
						},
					},
					{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "ThingForBatching",
							Property: schema.PropertyName("id"),
						},
						Value: &filters.Value{
							Value: "8d5a3aa2-3c8d-4589-9ae1-3f638f506004",
							Type:  schema.DataTypeText,
						},
					},
				},
			},
		},
		DryRun: false,
		Output: "verbose",
	}, nil, "")
	require.Nil(t, err)
	require.Equal(t, 2, len(batchDeleteRes.Objects), "Objects deleted")
}

func TestBatchDeleteObjects(t *testing.T) {
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
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	t.Run("batch import things", testBatchImportObjects(repo))

	t.Run("batch delete things", testBatchDeleteObjects(repo))
}

func TestBatchDeleteObjects_JourneyWithDimensions(t *testing.T) {
	dirName := t.TempDir()

	queryMaximumResults := int64(200)
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       queryMaximumResults,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator, schemaGetter))

	dimBefore := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 0, dimBefore, "Dimensions are empty before import")

	simpleInsertObjects(t, repo, "ThingForBatching", 103)

	dimAfter := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 309, dimAfter, "Dimensions are present before delete")

	delete2Objects(t, repo, "ThingForBatching")

	dimFinal := GetDimensionsFromRepo(repo, "ThingForBatching")
	require.Equal(t, 303, dimFinal, "Dimensions have been deleted")
}

func TestBatchDeleteObjects_Journey(t *testing.T) {
	dirName := t.TempDir()

	queryMaximumResults := int64(20)
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       queryMaximumResults,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", testAddBatchObjectClass(repo, migrator,
		schemaGetter))
	t.Run("batch import things", testBatchImportObjects(repo))
	t.Run("batch delete journey things", testBatchDeleteObjectsJourney(repo, queryMaximumResults))
}

func testAddBatchObjectClass(repo *DB, migrator *Migrator,
	schemaGetter *fakeSchemaGetter,
) func(t *testing.T) {
	return func(t *testing.T) {
		class := &models.Class{
			Class:               "ThingForBatching",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "location",
					DataType: []string{string(schema.DataTypeGeoCoordinates)},
				},
			},
		}

		require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))

		schemaGetter.schema.Objects = &models.Schema{
			Classes: []*models.Class{class},
		}
	}
}

func testBatchImportObjectsNoVector(repo *DB) func(t *testing.T) {
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
					UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
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
					UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
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
					UUID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
				},
			}

			t.Run("can import", func(t *testing.T) {
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil)
				require.Nil(t, err)

				assert.Nil(t, batchRes[0].Err)
				assert.Nil(t, batchRes[2].Err)
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			_, err := repo.Search(context.Background(), params)
			require.Nil(t, err)
		})
	}
}

func simpleInsertObjects(t *testing.T, repo *DB, class string, count int) {
	batch := make(objects.BatchObjects, count)
	for i := 0; i < count; i++ {
		batch[i] = objects.BatchObject{
			OriginalIndex: i,
			Err:           nil,
			Object: &models.Object{
				Class: class,
				Properties: map[string]interface{}{
					"stringProp": fmt.Sprintf("element %d", i),
				},
				ID: strfmt.UUID(fmt.Sprintf("8d5a3aa2-3c8d-4589-9ae1-3f638f506%03d", i)),
			},
			UUID:   strfmt.UUID(fmt.Sprintf("8d5a3aa2-3c8d-4589-9ae1-3f638f506%03d", i)),
			Vector: []float32{1, 2, 3},
		}
	}

	repo.BatchPutObjects(context.Background(), batch, nil)
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
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil)
				require.Nil(t, err)

				assert.Nil(t, batchRes[0].Err)
				assert.Nil(t, batchRes[2].Err)
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.Search(context.Background(), params)
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
				filter := buildFilter("stringProp", "third", eq, schema.DataTypeText)
				params := dto.GetParams{
					ClassName:  "ThingForBatching",
					Pagination: &filters.Pagination{Limit: 10},
					Filters:    filter,
				}
				res, err := repo.Search(context.Background(), params)
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
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil)
				require.Nil(t, err, "there shouldn't be an overall error, only individual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 3)
					assert.NotNil(t, batchRes[1].Err) // from validation
					assert.NotNil(t, batchRes[2].Err) // from db
				})
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.Search(context.Background(), params)
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

		t.Run("upserting the same objects over and over again", func(t *testing.T) {
			for i := 0; i < 20; i++ {
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
						Err:           nil,
						Object: &models.Object{
							Class: "ThingForBatching",
							Properties: map[string]interface{}{
								"stringProp": "third element",
							},
							ID: "90ade18e-2b99-4903-aa34-1d5d648c932d",
						},
						UUID:   "90ade18e-2b99-4903-aa34-1d5d648c932d",
						Vector: []float32{1, 1, -3},
					},
				}

				t.Run("can import", func(t *testing.T) {
					batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil)
					require.Nil(t, err)

					assert.Nil(t, batchRes[0].Err)
					assert.Nil(t, batchRes[1].Err)
				})

				t.Run("a vector search returns the correct number of elements", func(t *testing.T) {
					res, err := repo.VectorSearch(context.Background(), dto.GetParams{
						ClassName: "ThingForBatching",
						Pagination: &filters.Pagination{
							Offset: 0,
							Limit:  10,
						},
						SearchVector: []float32{1, 2, 3},
					})
					require.Nil(t, err)
					assert.Len(t, res, 2)
				})

			}
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
				batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil)
				require.Nil(t, err, "there shouldn't be an overall error, only individual ones")

				t.Run("element errors are marked correctly", func(t *testing.T) {
					require.Len(t, batchRes, 53)
					assert.NotNil(t, batchRes[51].Err) // from validation
				})
			})

			params := dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10},
				Filters:    nil,
			}
			res, err := repo.Search(context.Background(), params)
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

				batchRes, err := repo.BatchPutObjects(ctx, batch, nil)
				require.Nil(t, err, "there shouldn't be an overall error, only individual ones")

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
	r := getRandomSeed()
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
						"location": randGeoCoordinates(r),
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

				res, err := repo.BatchPutObjects(context.Background(), batch, nil)
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
			queryGeo := randGeoCoordinates(r)

			for _, maxDist := range distances {
				t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
					var relevant int
					var retrieved int

					controlList := bruteForceMaxDist(objs, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.Search(context.Background(), dto.GetParams{
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
					assert.True(t, recall >= 0.99)
				})
			}
		})

		t.Run("renew vector positions to test batch geo updates", func(t *testing.T) {
			for i, obj := range objs {
				obj.Properties = map[string]interface{}{
					"location": randGeoCoordinates(r),
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

				res, err := repo.BatchPutObjects(context.Background(), batch, nil)
				require.Nil(t, err)
				assertAllItemsErrorFree(t, res)
			}
		})

		t.Run("query again to verify updates worked", func(t *testing.T) {
			queryGeo := randGeoCoordinates(r)

			for _, maxDist := range distances {
				t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
					var relevant int
					var retrieved int

					controlList := bruteForceMaxDist(objs, []float32{
						*queryGeo.Latitude,
						*queryGeo.Longitude,
					}, maxDist*km)

					res, err := repo.Search(context.Background(), dto.GetParams{
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

func testBatchDeleteObjects(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		getParams := func(dryRun bool, output string) objects.BatchDeleteParams {
			return objects.BatchDeleteParams{
				ClassName: "ThingForBatching",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLike,
						Value: &filters.Value{
							Value: "*",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Property: schema.PropertyName("id"),
						},
					},
				},
				DryRun: dryRun,
				Output: output,
			}
		}
		performClassSearch := func() ([]search.Result, error) {
			return repo.Search(context.Background(), dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 10000},
			})
		}
		t.Run("batch delete with dryRun set to true", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(true, "verbose"), nil, "")
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, beforeDelete, len(res))
		})

		t.Run("batch delete with dryRun set to true and output to minimal", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(true, "minimal"), nil, "")
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, beforeDelete, len(res))
		})

		t.Run("batch delete only 2 given objects", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), objects.BatchDeleteParams{
				ClassName: "ThingForBatching",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorOr,
						Operands: []filters.Clause{
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    "ThingForBatching",
									Property: schema.PropertyName("id"),
								},
								Value: &filters.Value{
									Value: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
									Type:  schema.DataTypeText,
								},
							},
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    "ThingForBatching",
									Property: schema.PropertyName("id"),
								},
								Value: &filters.Value{
									Value: "90ade18e-2b99-4903-aa34-1d5d648c932d",
									Type:  schema.DataTypeText,
								},
							},
						},
					},
				},
				DryRun: false,
				Output: "verbose",
			}, nil, "")
			require.Nil(t, err)
			require.Equal(t, int64(2), batchDeleteRes.Matches)
			require.Equal(t, 2, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, beforeDelete-2, len(res))
		})

		t.Run("batch delete with dryRun set to false", func(t *testing.T) {
			// get the initial count of the objects
			res, err := performClassSearch()
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true, only test how many objects can be deleted
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(false, "verbose"), nil, "")
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			res, err = performClassSearch()
			require.Nil(t, err)
			assert.Equal(t, 0, len(res))
		})
	}
}

func testBatchDeleteObjectsJourney(repo *DB, queryMaximumResults int64) func(t *testing.T) {
	return func(t *testing.T) {
		getParams := func(dryRun bool, output string) objects.BatchDeleteParams {
			return objects.BatchDeleteParams{
				ClassName: "ThingForBatching",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorLike,
						Value: &filters.Value{
							Value: "*",
							Type:  schema.DataTypeText,
						},
						On: &filters.Path{
							Property: schema.PropertyName("id"),
						},
					},
				},
				DryRun: dryRun,
				Output: output,
			}
		}
		performClassSearch := func() ([]search.Result, error) {
			return repo.Search(context.Background(), dto.GetParams{
				ClassName:  "ThingForBatching",
				Pagination: &filters.Pagination{Limit: 20},
			})
		}
		t.Run("batch delete journey", func(t *testing.T) {
			// delete objects to limit
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(true, "verbose"), nil, "")
			require.Nil(t, err)
			objectsMatches := batchDeleteRes.Matches

			leftToDelete := objectsMatches
			deleteIterationCount := 0
			deletedObjectsCount := 0
			for {
				// delete objects to limit
				batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(false, "verbose"), nil, "")
				require.Nil(t, err)
				matches, deleted := batchDeleteRes.Matches, len(batchDeleteRes.Objects)
				require.Equal(t, leftToDelete, matches)
				require.True(t, deleted > 0)
				deletedObjectsCount += deleted

				batchDeleteRes, err = repo.BatchDeleteObjects(context.Background(), getParams(true, "verbose"), nil, "")
				require.Nil(t, err)
				leftToDelete = batchDeleteRes.Matches

				res, err := performClassSearch()
				require.Nil(t, err)
				afterDelete := len(res)
				require.True(t, afterDelete >= 0)
				if afterDelete == 0 {
					// where have deleted all objects
					break
				}
				deleteIterationCount += 1
				if deleteIterationCount > 100 {
					// something went wrong
					break
				}
			}
			require.False(t, deleteIterationCount > 100, "Batch delete journey tests didn't stop properly")
			require.True(t, objectsMatches/int64(queryMaximumResults) <= int64(deleteIterationCount))
			require.Equal(t, objectsMatches, int64(deletedObjectsCount))
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

func randGeoCoordinates(r *rand.Rand) *models.GeoCoordinates {
	maxLat := float32(90.0)
	minLat := float32(-90.0)
	maxLon := float32(180)
	minLon := float32(-180)

	lat := minLat + (maxLat-minLat)*r.Float32()
	lon := minLon + (maxLon-minLon)*r.Float32()
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
