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
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_MultiShardJourneys_IndividualImports(t *testing.T) {
	repo, logger, dirName := setupMultiShardTest()
	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
		}
	}()

	t.Run("prepare", makeTestMultiShardSchema(repo, logger, false, testClassesForImporting()...))

	data := multiShardTestData()
	queryVec := exampleQueryVec()
	groundTruth := bruteForceObjectsByQuery(data, queryVec)
	refData := multiShardRefClassData(data)

	t.Run("import all individually", func(t *testing.T) {
		for _, obj := range data {
			require.Nil(t, repo.PutObject(context.Background(), obj, obj.Vector))
		}
	})

	t.Run("verify objects", makeTestRetrievingBaseClass(repo, data, queryVec,
		groundTruth))

	t.Run("import refs individually", func(t *testing.T) {
		for _, obj := range refData {
			require.Nil(t, repo.PutObject(context.Background(), obj, obj.Vector))
		}
	})

	t.Run("verify refs", makeTestRetrieveRefClass(repo, data, refData))
}

func Test_MultiShardJourneys_BatchedImports(t *testing.T) {
	repo, logger, dirName := setupMultiShardTest()
	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
		}
	}()

	t.Run("prepare", makeTestMultiShardSchema(repo, logger, false, testClassesForImporting()...))

	data := multiShardTestData()
	queryVec := exampleQueryVec()
	groundTruth := bruteForceObjectsByQuery(data, queryVec)
	refData := multiShardRefClassData(data)

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

		_, err := repo.BatchPutObjects(context.Background(), batch)
		require.Nil(t, err)
	})

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

			require.Nil(t, repo.PutObject(context.Background(), withoutRef,
				withoutRef.Vector))
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

		_, err := repo.AddBatchReferences(context.Background(), refBatch)
		require.Nil(t, err)
	})

	t.Run("verify refs", makeTestRetrieveRefClass(repo, data, refData))
}

func Test_MultiShardJourneys_BM25_Search(t *testing.T) {
	repo, logger, dirName := setupMultiShardTest()
	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
		}
	}()

	className := "RacecarPosts"

	t.Run("prepare", func(t *testing.T) {
		class := &models.Class{
			Class:             className,
			VectorIndexConfig: hnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: &models.InvertedIndexConfig{
				CleanupIntervalSeconds: 60,
			},
			Properties: []*models.Property{
				{
					Name:         "contents",
					DataType:     []string{string(schema.DataTypeText)},
					Tokenization: "word",
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

		_, err := repo.BatchPutObjects(context.Background(), objs)
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
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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

func setupMultiShardTest() (*DB, *logrus.Logger, string) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)

	logger, _ := test.NewNullLogger()
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})

	return repo, logger, dirName
}

func makeTestMultiShardSchema(repo *DB, logger logrus.FieldLogger, fixedShardState bool, classes ...*models.Class) func(t *testing.T) {
	return func(t *testing.T) {
		var shardState *sharding.State
		if fixedShardState {
			shardState = fixedMultiShardState()
		} else {
			shardState = multiShardState()
		}
		schemaGetter := &fakeSchemaGetter{shardState: shardState}
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
	queryVec []float32, groundTruth []*models.Object) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("retrieve all individually", func(t *testing.T) {
			for _, desired := range data {
				res, err := repo.ObjectByID(context.Background(), desired.ID,
					search.SelectProperties{}, additional.Properties{})
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
				res, err := repo.ObjectSearch(context.Background(), 0, limit, filters,
					additional.Properties{})
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
				res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
				res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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
				res, err := repo.VectorSearch(context.Background(), queryVec, 0, limit, nil)
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
				res, err := repo.ObjectByID(context.Background(), desired.ID,
					search.SelectProperties{
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
					}, additional.Properties{})
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

func exampleQueryVec() []float32 {
	dim := 10
	vec := make([]float32, dim)
	for j := range vec {
		vec[j] = rand.Float32()
	}
	return vec
}

func multiShardTestData() []*models.Object {
	size := 20
	dim := 10
	out := make([]*models.Object, size)
	for i := range out {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}

		out[i] = &models.Object{
			ID:     strfmt.UUID(uuid.New().String()),
			Class:  "TestClass",
			Vector: vec,
			Properties: map[string]interface{}{
				"boolProp": i%2 == 0,
				"index":    i,
			},
		}
	}

	return out
}

func multiShardRefClassData(targets []*models.Object) []*models.Object {
	// each class will link to all possible targets, so that we can be sure that
	// we hit cross-shard links
	targetLinks := make(models.MultipleRef, len(targets))
	for i, obj := range targets {
		targetLinks[i] = &models.SingleRef{
			Beacon: strfmt.URI(crossref.New("localhost", obj.ID).String()),
		}
	}

	size := 20
	dim := 10
	out := make([]*models.Object, size)
	for i := range out {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
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
	query []float32) []*models.Object {
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
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
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
			},
		},
		{
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
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
