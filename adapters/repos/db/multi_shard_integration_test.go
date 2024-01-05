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
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func Test_MultiShardJourneys_IndividualImports(t *testing.T) {
	r := getRandomSeed()
	repo, logger := setupMultiShardTest(t)
	defer func() {
		repo.Shutdown(context.Background())
	}()

	t.Run("prepare", makeTestMultiShardSchema(repo, logger, false, testClassesForImporting()...))

	data := multiShardTestData(r)
	queryVec := exampleQueryVec(r)
	groundTruth := bruteForceObjectsByQuery(data, queryVec)
	refData := multiShardRefClassData(r, data)

	t.Run("import all individually", func(t *testing.T) {
		for _, obj := range data {
			require.Nil(t, repo.PutObject(context.Background(), obj, obj.Vector, nil))
		}
	})

	t.Run("nodes api", testNodesAPI(repo))

	t.Run("sorting objects", makeTestSortingClass(repo))

	t.Run("verify objects", makeTestRetrievingBaseClass(repo, data, queryVec,
		groundTruth))

	t.Run("import refs individually", func(t *testing.T) {
		for _, obj := range refData {
			require.Nil(t, repo.PutObject(context.Background(), obj, obj.Vector, nil))
		}
	})

	t.Run("verify refs", makeTestRetrieveRefClass(repo, data, refData))

	t.Run("batch delete", makeTestBatchDeleteAllObjects(repo))
}

func Test_MultiShardJourneys_BatchedImports(t *testing.T) {
	r := getRandomSeed()
	repo, logger := setupMultiShardTest(t)
	defer func() {
		repo.Shutdown(context.Background())
	}()

	t.Run("prepare", makeTestMultiShardSchema(repo, logger, false, testClassesForImporting()...))

	data := multiShardTestData(r)
	queryVec := exampleQueryVec(r)
	groundTruth := bruteForceObjectsByQuery(data, queryVec)
	refData := multiShardRefClassData(r, data)

	t.Run("import in a batch", func(t *testing.T) {
		batch := make(objects.BatchObjects, len(data))
		for i, obj := range data {
			batch[i] = objects.BatchObject{
				OriginalIndex: i,
				Object:        obj,
				Vector:        obj.Vector,
				UUID:          obj.ID,
			}
		}

		_, err := repo.BatchPutObjects(context.Background(), batch, nil)
		require.Nil(t, err)
	})

	t.Run("nodes api", testNodesAPI(repo))

	t.Run("verify objects", makeTestRetrievingBaseClass(repo, data, queryVec,
		groundTruth))

	t.Run("import refs in large batch", func(t *testing.T) {
		// first strip the refs from the objects, so we can import them in a second
		// step as batch ref

		for _, obj := range refData {
			withoutRef := &models.Object{
				ID:         obj.ID,
				Class:      obj.Class,
				Vector:     obj.Vector,
				Properties: map[string]interface{}{}, // empty so we remove the ref
			}

			require.Nil(t, repo.PutObject(context.Background(), withoutRef, withoutRef.Vector, nil))
		}

		index := 0
		refBatch := make(objects.BatchReferences, len(refData)*len(data))
		for _, obj := range refData {
			for _, ref := range obj.Properties.(map[string]interface{})["toOther"].(models.MultipleRef) {
				to, _ := crossref.ParseSingleRef(ref)
				refBatch[index] = objects.BatchReference{
					OriginalIndex: index,
					To:            to,
					From:          crossref.NewSource(schema.ClassName(obj.Class), "toOther", obj.ID),
				}
				index++
			}
		}

		_, err := repo.AddBatchReferences(context.Background(), refBatch, nil)
		require.Nil(t, err)
	})

	t.Run("verify refs", makeTestRetrieveRefClass(repo, data, refData))

	t.Run("batch delete", makeTestBatchDeleteAllObjects(repo))
}

func Test_MultiShardJourneys_BM25_Search(t *testing.T) {
	repo, logger := setupMultiShardTest(t)
	defer func() {
		repo.Shutdown(context.Background())
	}()

	className := "RacecarPosts"

	t.Run("prepare", func(t *testing.T) {
		class := &models.Class{
			Class:             className,
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				CleanupIntervalSeconds: 60,
			},
			Properties: []*models.Property{
				{
					Name:         "contents",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWord,
				},
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "textArrayProp",
					DataType: []string{string(schema.DataTypeTextArray)},
				},
			},
		}

		t.Run("prepare", makeTestMultiShardSchema(repo, logger, true, class))
	})

	t.Run("insert search data", func(t *testing.T) {
		objs := objects.BatchObjects{
			{
				UUID: "c39751ed-ddc2-4c9f-a45b-8b5732ddde56",
				Object: &models.Object{
					ID:    "c39751ed-ddc2-4c9f-a45b-8b5732ddde56",
					Class: className,
					Properties: map[string]interface{}{
						"contents": "Team Lotus was a domineering force in the early 90s",
					},
				},
			},
			{
				UUID: "5d034311-06e1-476e-b446-1306db91d906",
				Object: &models.Object{
					ID:    "5d034311-06e1-476e-b446-1306db91d906",
					Class: className,
					Properties: map[string]interface{}{
						"contents": "When a car becomes unserviceable, the driver must retire early from the race",
					},
				},
			},
			{
				UUID: "01989a8c-e37f-471d-89ca-9a787dbbf5f2",
				Object: &models.Object{
					ID:    "01989a8c-e37f-471d-89ca-9a787dbbf5f2",
					Class: className,
					Properties: map[string]interface{}{
						"contents": "A young driver is better than an old driver",
					},
				},
			},
			{
				UUID: "392614c5-4ca4-4630-a014-61fe868a20fd",
				Object: &models.Object{
					ID:    "392614c5-4ca4-4630-a014-61fe868a20fd",
					Class: className,
					Properties: map[string]interface{}{
						"contents": "an old driver doesn't retire early",
					},
				},
			},
		}

		_, err := repo.BatchPutObjects(context.Background(), objs, nil)
		require.Nil(t, err)
	})

	t.Run("ranked keyword search", func(t *testing.T) {
		type testcase struct {
			expectedResults []string
			rankingParams   *searchparams.KeywordRanking
		}

		tests := []testcase{
			{
				rankingParams: &searchparams.KeywordRanking{
					Query:      "driver",
					Properties: []string{"contents"},
				},
				expectedResults: []string{
					"01989a8c-e37f-471d-89ca-9a787dbbf5f2",
					"392614c5-4ca4-4630-a014-61fe868a20fd",
					"5d034311-06e1-476e-b446-1306db91d906",
				},
			},
		}

		for _, test := range tests {
			res, err := repo.Search(context.Background(), dto.GetParams{
				ClassName:      className,
				Pagination:     &filters.Pagination{Limit: 10},
				KeywordRanking: test.rankingParams,
			})
			require.Nil(t, err)
			require.Equal(t, len(test.expectedResults), len(res))
			for i := range res {
				assert.Equal(t, test.expectedResults[i], res[i].ID.String())
			}
			t.Logf("res: %+v", res)
		}
	})
}

func setupMultiShardTest(t *testing.T) (*DB, *logrus.Logger) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	repo, err := New(logger, Config{
		ServerVersion:             "server-version",
		GitHash:                   "git-hash",
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	return repo, logger
}

func makeTestMultiShardSchema(repo *DB, logger logrus.FieldLogger, fixedShardState bool, classes ...*models.Class) func(t *testing.T) {
	return func(t *testing.T) {
		var shardState *sharding.State
		if fixedShardState {
			shardState = fixedMultiShardState()
		} else {
			shardState = multiShardState()
		}
		schemaGetter := &fakeSchemaGetter{
			schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
			shardState: shardState,
		}
		repo.SetSchemaGetter(schemaGetter)
		err := repo.WaitForStartup(testCtx())
		require.Nil(t, err)
		migrator := NewMigrator(repo, logger)

		t.Run("creating the class", func(t *testing.T) {
			for _, class := range classes {
				require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
			}
		})

		// update schema getter so it's in sync with class
		schemaGetter.schema = schema.Schema{
			Objects: &models.Schema{
				Classes: classes,
			},
		}
	}
}

func makeTestRetrievingBaseClass(repo *DB, data []*models.Object,
	queryVec []float32, groundTruth []*models.Object,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("retrieve all individually", func(t *testing.T) {
			for _, desired := range data {
				res, err := repo.ObjectByID(context.Background(), desired.ID, search.SelectProperties{}, additional.Properties{}, "")
				assert.Nil(t, err)

				require.NotNil(t, res)
				assert.Equal(t, desired.Properties.(map[string]interface{})["boolProp"].(bool),
					res.Object().Properties.(map[string]interface{})["boolProp"].(bool))
				assert.Equal(t, desired.ID, res.Object().ID)
			}
		})

		t.Run("retrieve through filter (object search)", func(t *testing.T) {
			do := func(limit, expected int) {
				filters := &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Value: true,
							Type:  schema.DataTypeBoolean,
						},
						On: &filters.Path{
							Property: "boolProp",
						},
					},
				}
				res, err := repo.ObjectSearch(context.Background(), 0, limit, filters, nil,
					additional.Properties{}, "")
				assert.Nil(t, err)

				assert.Len(t, res, expected)
				for _, obj := range res {
					assert.Equal(t, true, obj.Schema.(map[string]interface{})["boolProp"].(bool))
				}
			}

			t.Run("with high limit", func(t *testing.T) {
				do(100, 10)
			})

			t.Run("with low limit", func(t *testing.T) {
				do(3, 3)
			})
		})

		t.Run("retrieve through filter (class search)", func(t *testing.T) {
			do := func(limit, expected int) {
				filter := &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						Value: &filters.Value{
							Value: true,
							Type:  schema.DataTypeBoolean,
						},
						On: &filters.Path{
							Property: "boolProp",
						},
					},
				}
				res, err := repo.Search(context.Background(), dto.GetParams{
					Filters: filter,
					Pagination: &filters.Pagination{
						Limit: limit,
					},
					ClassName: "TestClass",
				})
				assert.Nil(t, err)

				assert.Len(t, res, expected)
				for _, obj := range res {
					assert.Equal(t, true, obj.Schema.(map[string]interface{})["boolProp"].(bool))
				}
			}

			t.Run("with high limit", func(t *testing.T) {
				do(100, 10)
			})

			t.Run("with low limit", func(t *testing.T) {
				do(3, 3)
			})
		})

		t.Run("retrieve through class-level vector search", func(t *testing.T) {
			do := func(t *testing.T, limit, expected int) {
				res, err := repo.VectorSearch(context.Background(), dto.GetParams{
					SearchVector: queryVec,
					Pagination: &filters.Pagination{
						Limit: limit,
					},
					ClassName: "TestClass",
				})
				assert.Nil(t, err)
				assert.Len(t, res, expected)
				for i, obj := range res {
					assert.Equal(t, groundTruth[i].ID, obj.ID)
				}
			}

			t.Run("with high limit", func(t *testing.T) {
				do(t, 100, 20)
			})

			t.Run("with low limit", func(t *testing.T) {
				do(t, 3, 3)
			})
		})

		t.Run("retrieve through inter-class vector search", func(t *testing.T) {
			do := func(t *testing.T, limit, expected int) {
				res, err := repo.CrossClassVectorSearch(context.Background(), queryVec, 0, limit, nil)
				assert.Nil(t, err)
				assert.Len(t, res, expected)
				for i, obj := range res {
					assert.Equal(t, groundTruth[i].ID, obj.ID)
				}
			}

			t.Run("with high limit", func(t *testing.T) {
				do(t, 100, 20)
			})

			t.Run("with low limit", func(t *testing.T) {
				do(t, 3, 3)
			})
		})
	}
}

func makeTestRetrieveRefClass(repo *DB, data, refData []*models.Object) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("retrieve ref data individually with select props", func(t *testing.T) {
			for _, desired := range refData {
				res, err := repo.ObjectByID(context.Background(), desired.ID, search.SelectProperties{
					search.SelectProperty{
						IsPrimitive: false,
						Name:        "toOther",
						Refs: []search.SelectClass{{
							ClassName: "TestClass",
							RefProperties: search.SelectProperties{{
								Name:        "index",
								IsPrimitive: true,
							}},
						}},
					},
				}, additional.Properties{}, "")
				assert.Nil(t, err)
				refs := res.Schema.(map[string]interface{})["toOther"].([]interface{})
				assert.Len(t, refs, len(data))
				for i, ref := range refs {
					indexField := ref.(search.LocalRef).Fields["index"].(float64)
					assert.Equal(t, i, int(indexField))
				}
			}
		})
	}
}

func makeTestSortingClass(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("sort by property", func(t *testing.T) {
			getIndex := func(res search.Result) float64 {
				if prop := res.Object().Properties.(map[string]interface{})["index"]; prop != nil {
					return prop.(float64)
				}
				return -1
			}
			getBoolProp := func(res search.Result) bool {
				if prop := res.Object().Properties.(map[string]interface{})["boolProp"]; prop != nil {
					return prop.(bool)
				}
				return false
			}
			getStringProp := func(res search.Result) string {
				if prop := res.Object().Properties.(map[string]interface{})["stringProp"]; prop != nil {
					return prop.(string)
				}
				return ""
			}
			getTextArrayProp := func(res search.Result) []string {
				if prop := res.Object().Properties.(map[string]interface{})["textArrayProp"]; prop != nil {
					return prop.([]string)
				}
				return nil
			}
			type test struct {
				name                   string
				sort                   []filters.Sort
				expectedIndexes        []float64
				expectedBoolProps      []bool
				expectedStringProps    []string
				expectedTextArrayProps [][]string
				constainsErrorMsgs     []string
			}
			tests := []test{
				{
					name:            "indexProp desc",
					sort:            []filters.Sort{{Path: []string{"indexProp"}, Order: "desc"}},
					expectedIndexes: []float64{19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
				},
				{
					name:            "indexProp asc",
					sort:            []filters.Sort{{Path: []string{"indexProp"}, Order: "asc"}},
					expectedIndexes: []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
				},
				{
					name:                "stringProp desc",
					sort:                []filters.Sort{{Path: []string{"stringProp"}, Order: "desc"}},
					expectedStringProps: []string{"s19", "s18", "s17", "s16", "s15", "s14", "s13", "s12", "s11", "s10", "s09", "s08", "s07", "s06", "s05", "s04", "s03", "s02", "s01", "s00"},
				},
				{
					name:                "stringProp asc",
					sort:                []filters.Sort{{Path: []string{"stringProp"}, Order: "asc"}},
					expectedStringProps: []string{"s00", "s01", "s02", "s03", "s04", "s05", "s06", "s07", "s08", "s09", "s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19"},
				},
				{
					name:                   "textArrayProp desc",
					sort:                   []filters.Sort{{Path: []string{"textArrayProp"}, Order: "desc"}},
					expectedTextArrayProps: [][]string{{"s19", "19"}, {"s18", "18"}, {"s17", "17"}, {"s16", "16"}, {"s15", "15"}, {"s14", "14"}, {"s13", "13"}, {"s12", "12"}, {"s11", "11"}, {"s10", "10"}, {"s09", "09"}, {"s08", "08"}, {"s07", "07"}, {"s06", "06"}, {"s05", "05"}, {"s04", "04"}, {"s03", "03"}, {"s02", "02"}, {"s01", "01"}, {"s00", "00"}},
				},
				{
					name:                   "textArrayProp asc",
					sort:                   []filters.Sort{{Path: []string{"textArrayProp"}, Order: "asc"}},
					expectedTextArrayProps: [][]string{{"s00", "00"}, {"s01", "01"}, {"s02", "02"}, {"s03", "03"}, {"s04", "04"}, {"s05", "05"}, {"s06", "06"}, {"s07", "07"}, {"s08", "08"}, {"s09", "09"}, {"s10", "10"}, {"s11", "11"}, {"s12", "12"}, {"s13", "13"}, {"s14", "14"}, {"s15", "15"}, {"s16", "16"}, {"s17", "17"}, {"s18", "18"}, {"s19", "19"}},
				},
				{
					name:              "boolProp desc",
					sort:              []filters.Sort{{Path: []string{"boolProp"}, Order: "desc"}},
					expectedBoolProps: []bool{true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false},
				},
				{
					name:              "boolProp asc",
					sort:              []filters.Sort{{Path: []string{"boolProp"}, Order: "asc"}},
					expectedBoolProps: []bool{false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true},
				},
				{
					name:                "boolProp asc stringProp asc",
					sort:                []filters.Sort{{Path: []string{"boolProp"}, Order: "asc"}, {Path: []string{"stringProp"}, Order: "asc"}},
					expectedBoolProps:   []bool{false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true},
					expectedStringProps: []string{"s01", "s03", "s05", "s07", "s09", "s11", "s13", "s15", "s17", "s19", "s00", "s02", "s04", "s06", "s08", "s10", "s12", "s14", "s16", "s18"},
				},
				{
					name:                "boolProp desc stringProp asc",
					sort:                []filters.Sort{{Path: []string{"boolProp"}, Order: "desc"}, {Path: []string{"stringProp"}, Order: "asc"}},
					expectedBoolProps:   []bool{true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false},
					expectedStringProps: []string{"s00", "s02", "s04", "s06", "s08", "s10", "s12", "s14", "s16", "s18", "s01", "s03", "s05", "s07", "s09", "s11", "s13", "s15", "s17", "s19"},
				},
				{
					name:              "boolProp asc indexProp asc",
					sort:              []filters.Sort{{Path: []string{"boolProp"}, Order: "asc"}, {Path: []string{"indexProp"}, Order: "asc"}},
					expectedBoolProps: []bool{false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true},
					expectedIndexes:   []float64{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
				},
				{
					name:              "boolProp asc indexProp desc",
					sort:              []filters.Sort{{Path: []string{"boolProp"}, Order: "asc"}, {Path: []string{"indexProp"}, Order: "desc"}},
					expectedBoolProps: []bool{false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true},
					expectedIndexes:   []float64{19, 17, 15, 13, 11, 9, 7, 5, 3, 1, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0},
				},
				{
					name:            "index property doesn't exist in testrefclass",
					sort:            []filters.Sort{{Path: []string{"index"}, Order: "desc"}},
					expectedIndexes: nil,
					constainsErrorMsgs: []string{
						"no such prop with name 'index' found in class 'TestRefClass' in the schema. " +
							"Check your schema files for which properties in this class are available",
					},
				},
				{
					name:            "non existent property in all classes",
					sort:            []filters.Sort{{Path: []string{"nonexistentproperty"}, Order: "desc"}},
					expectedIndexes: nil,
					constainsErrorMsgs: []string{
						"no such prop with name 'nonexistentproperty' found in class 'TestClass' in the schema. " +
							"Check your schema files for which properties in this class are available",
						"no such prop with name 'nonexistentproperty' found in class 'TestRefClass' in the schema. " +
							"Check your schema files for which properties in this class are available",
					},
				},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					res, err := repo.ObjectSearch(context.Background(), 0, 1000, nil, test.sort,
						additional.Properties{}, "")
					if len(test.constainsErrorMsgs) > 0 {
						require.NotNil(t, err)
						for _, errorMsg := range test.constainsErrorMsgs {
							assert.Contains(t, err.Error(), errorMsg)
						}
					} else {
						require.Nil(t, err)
						if len(test.expectedIndexes) > 0 {
							for i := range res {
								assert.Equal(t, test.expectedIndexes[i], getIndex(res[i]))
							}
						}
						if len(test.expectedBoolProps) > 0 {
							for i := range res {
								assert.Equal(t, test.expectedBoolProps[i], getBoolProp(res[i]))
							}
						}
						if len(test.expectedStringProps) > 0 {
							for i := range res {
								assert.Equal(t, test.expectedStringProps[i], getStringProp(res[i]))
							}
						}
						if len(test.expectedTextArrayProps) > 0 {
							for i := range res {
								assert.EqualValues(t, test.expectedTextArrayProps[i], getTextArrayProp(res[i]))
							}
						}
					}
				})
			}
		})
	}
}

func testNodesAPI(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		nodeStatues, err := repo.GetNodeStatus(context.Background(), "", verbosity.OutputVerbose)
		require.Nil(t, err)
		require.NotNil(t, nodeStatues)

		require.Len(t, nodeStatues, 1)
		nodeStatus := nodeStatues[0]
		assert.NotNil(t, nodeStatus)
		assert.Equal(t, "node1", nodeStatus.Name)
		assert.Equal(t, "server-version", nodeStatus.Version)
		assert.Equal(t, "git-hash", nodeStatus.GitHash)
		assert.Len(t, nodeStatus.Shards, 6)
		var testClassShardsCount, testClassObjectsCount int64
		var testRefClassShardsCount, testRefClassObjectsCount int64
		for _, status := range nodeStatus.Shards {
			if status.Class == "TestClass" {
				testClassShardsCount += 1
				testClassObjectsCount += status.ObjectCount
			}
			if status.Class == "TestRefClass" {
				testRefClassShardsCount += 1
				testRefClassObjectsCount += status.ObjectCount
			}
		}
		assert.Equal(t, int64(3), testClassShardsCount)
		assert.Equal(t, int64(20), testClassObjectsCount)
		assert.Equal(t, int64(3), testRefClassShardsCount)
		assert.Equal(t, int64(0), testRefClassObjectsCount)
		assert.Equal(t, int64(20), nodeStatus.Stats.ObjectCount)
		assert.Equal(t, int64(6), nodeStatus.Stats.ShardCount)
	}
}

func makeTestBatchDeleteAllObjects(repo *DB) func(t *testing.T) {
	return func(t *testing.T) {
		performDelete := func(t *testing.T, className string) {
			getParams := func(className string, dryRun bool) objects.BatchDeleteParams {
				return objects.BatchDeleteParams{
					ClassName: schema.ClassName(className),
					Filters: &filters.LocalFilter{
						Root: &filters.Clause{
							Operator: filters.OperatorLike,
							Value: &filters.Value{
								Value: "*",
								Type:  schema.DataTypeText,
							},
							On: &filters.Path{
								Property: "id",
							},
						},
					},
					DryRun: dryRun,
					Output: "verbose",
				}
			}
			performClassSearch := func(className string) ([]search.Result, error) {
				return repo.Search(context.Background(), dto.GetParams{
					ClassName:  className,
					Pagination: &filters.Pagination{Limit: 10000},
				})
			}
			// get the initial count of the objects
			res, err := performClassSearch(className)
			require.Nil(t, err)
			beforeDelete := len(res)
			require.True(t, beforeDelete > 0)
			// dryRun == true
			batchDeleteRes, err := repo.BatchDeleteObjects(context.Background(), getParams(className, true), nil, "")
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			// check that every object is preserved (not deleted)
			res, err = performClassSearch(className)
			require.Nil(t, err)
			require.Equal(t, beforeDelete, len(res))
			// dryRun == false, perform actual delete
			batchDeleteRes, err = repo.BatchDeleteObjects(context.Background(), getParams(className, false), nil, "")
			require.Nil(t, err)
			require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
			require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
			for _, batchRes := range batchDeleteRes.Objects {
				require.Nil(t, batchRes.Err)
			}
			// check that every object is deleted
			res, err = performClassSearch(className)
			require.Nil(t, err)
			require.Equal(t, 0, len(res))
		}
		t.Run("batch delete TestRefClass", func(t *testing.T) {
			performDelete(t, "TestRefClass")
		})
		t.Run("batch delete TestClass", func(t *testing.T) {
			performDelete(t, "TestClass")
		})
	}
}

func exampleQueryVec(r *rand.Rand) []float32 {
	dim := 10
	vec := make([]float32, dim)
	for j := range vec {
		vec[j] = r.Float32()
	}
	return vec
}

func multiShardTestData(r *rand.Rand) []*models.Object {
	size := 20
	dim := 10
	out := make([]*models.Object, size)
	for i := range out {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = r.Float32()
		}

		out[i] = &models.Object{
			ID:     strfmt.UUID(uuid.New().String()),
			Class:  "TestClass",
			Vector: vec,
			Properties: map[string]interface{}{
				"boolProp":      i%2 == 0,
				"index":         i,
				"indexProp":     i,
				"stringProp":    fmt.Sprintf("s%02d", i),
				"textArrayProp": []string{fmt.Sprintf("s%02d", i), fmt.Sprintf("%02d", i)},
			},
		}
	}

	return out
}

func multiShardRefClassData(r *rand.Rand, targets []*models.Object) []*models.Object {
	// each class will link to all possible targets, so that we can be sure that
	// we hit cross-shard links
	targetLinks := make(models.MultipleRef, len(targets))
	for i, obj := range targets {
		targetLinks[i] = &models.SingleRef{
			Beacon: strfmt.URI(crossref.NewLocalhost("", obj.ID).String()),
		}
	}

	size := 20
	dim := 10
	out := make([]*models.Object, size)
	for i := range out {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = r.Float32()
		}

		out[i] = &models.Object{
			ID:     strfmt.UUID(uuid.New().String()),
			Class:  "TestRefClass",
			Vector: vec,
			Properties: map[string]interface{}{
				"toOther": targetLinks,
			},
		}
	}

	return out
}

func bruteForceObjectsByQuery(objs []*models.Object,
	query []float32,
) []*models.Object {
	type distanceAndObj struct {
		distance float32
		obj      *models.Object
	}

	distProv := distancer.NewDotProductProvider()
	distances := make([]distanceAndObj, len(objs))

	for i := range objs {
		dist, _, _ := distProv.SingleDist(normalize(query), normalize(objs[i].Vector))
		distances[i] = distanceAndObj{
			distance: dist,
			obj:      objs[i],
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]*models.Object, len(objs))
	for i := range out {
		out[i] = distances[i].obj
	}

	return out
}

func testClassesForImporting() []*models.Class {
	return []*models.Class{
		{
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Class:               "TestClass",
			Properties: []*models.Property{
				{
					Name:     "boolProp",
					DataType: []string{string(schema.DataTypeBoolean)},
				},
				{
					Name:     "index",
					DataType: []string{string(schema.DataTypeInt)},
				},
				{
					Name:     "indexProp",
					DataType: []string{string(schema.DataTypeInt)},
				},
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "textArrayProp",
					DataType: []string{string(schema.DataTypeTextArray)},
				},
			},
		},
		{
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Class:               "TestRefClass",
			Properties: []*models.Property{
				{
					Name:     "boolProp",
					DataType: []string{string(schema.DataTypeBoolean)},
				},
				{
					Name:     "toOther",
					DataType: []string{"TestClass"},
				},
				{
					Name:     "indexProp",
					DataType: []string{string(schema.DataTypeInt)},
				},
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "textArrayProp",
					DataType: []string{string(schema.DataTypeTextArray)},
				},
			},
		},
	}
}

func normalize(v []float32) []float32 {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}

	return v
}
