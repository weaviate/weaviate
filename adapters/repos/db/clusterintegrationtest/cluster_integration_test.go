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

package clusterintegrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/objects"
)

const (
	vectorDims       = 20
	numberOfNodes    = 10
	distributedClass = "Distributed"
)

// TestDistributedSetup uses as many real components and only mocks out
// non-essential parts. Essentially we fix the shard/cluster state and schema
// as they aren't critical to this test, but use real repos and real HTTP APIs
// between the repos.
func TestDistributedSetup(t *testing.T) {
	t.Run("individual imports", func(t *testing.T) {
		dirName := setupDirectory(t)
		r := getRandomSeed()
		testDistributed(t, dirName, r, false)
	})
	t.Run("batched imports", func(t *testing.T) {
		dirName := setupDirectory(t)
		r := getRandomSeed()
		testDistributed(t, dirName, r, true)
	})
}

func testDistributed(t *testing.T, dirName string, rnd *rand.Rand, batch bool) {
	var nodes []*node
	numberOfObjects := 200

	t.Run("setup", func(t *testing.T) {
		overallShardState := multiShardState(numberOfNodes)
		shardStateSerialized, err := json.Marshal(overallShardState)
		require.Nil(t, err)

		for i := 0; i < numberOfNodes; i++ {
			node := &node{
				name: fmt.Sprintf("node-%d", i),
			}

			node.init(dirName, shardStateSerialized, &nodes)
			nodes = append(nodes, node)
		}
	})

	t.Run("apply schema", func(t *testing.T) {
		for i := range nodes {
			err := nodes[i].migrator.AddClass(context.Background(), class(),
				nodes[i].schemaManager.shardState)
			require.Nil(t, err)
			err = nodes[i].migrator.AddClass(context.Background(), secondClassWithRef(),
				nodes[i].schemaManager.shardState)
			require.Nil(t, err)
			nodes[i].schemaManager.schema.Objects.Classes = append(nodes[i].schemaManager.schema.Objects.Classes,
				class(), secondClassWithRef())
		}
	})

	data := exampleData(numberOfObjects)
	refData := exampleDataWithRefs(numberOfObjects, 5, data)

	if batch {
		t.Run("import large batch from random node", func(t *testing.T) {
			// pick a random node, but send the entire batch to this node
			node := nodes[rnd.Intn(len(nodes))]

			batchObjs := dataAsBatch(data)
			res, err := node.repo.BatchPutObjects(context.Background(), batchObjs, nil, 0)
			require.Nil(t, err)
			for _, ind := range res {
				require.Nil(t, ind.Err)
			}
		})

		t.Run("import second class without refs", func(t *testing.T) {
			// pick a random node, but send the entire batch to this node
			node := nodes[rnd.Intn(len(nodes))]

			batchObjs := dataAsBatchWithProps(refData, []string{"description"})
			res, err := node.repo.BatchPutObjects(context.Background(), batchObjs, nil, 0)
			require.Nil(t, err)
			for _, ind := range res {
				require.Nil(t, ind.Err)
			}
		})

		t.Run("import refs as batch", func(t *testing.T) {
			// pick a random node, but send the entire batch to this node
			node := nodes[rnd.Intn(len(nodes))]

			batch := refsAsBatch(refData, "toFirst")
			res, err := node.repo.AddBatchReferences(context.Background(), batch, nil, 0)
			require.Nil(t, err)
			for _, ind := range res {
				require.Nil(t, ind.Err)
			}
		})
	} else {
		t.Run("import first class by picking a random node", func(t *testing.T) {
			for _, obj := range data {
				node := nodes[rnd.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector, nil, nil, nil, 0)
				require.Nil(t, err)
			}
		})
		t.Run("import second class with refs by picking a random node", func(t *testing.T) {
			for _, obj := range refData {
				node := nodes[rnd.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector, nil, nil, nil, 0)
				require.Nil(t, err)

			}
		})
	}

	t.Run("wait for indexing to finish", func(t *testing.T) {
		for _, node := range nodes {
			time.Sleep(100 * time.Millisecond)
			node.repo.GetScheduler().Schedule(context.Background())
			node.repo.GetScheduler().WaitAll()
		}
	})

	t.Run("query individually to check if all exist using random nodes", func(t *testing.T) {
		for _, obj := range data {
			node := nodes[rnd.Intn(len(nodes))]

			ok, err := node.repo.Exists(context.Background(), distributedClass, obj.ID, nil, "")
			require.Nil(t, err)
			assert.True(t, ok)
		}
	})

	t.Run("query individually using random node", func(t *testing.T) {
		for _, obj := range data {
			node := nodes[rnd.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID, search.SelectProperties{}, additional.Properties{}, "")
			require.Nil(t, err)
			require.NotNil(t, res)

			// only compare string prop to avoid having to deal with parsing time
			// props
			assert.Equal(t, obj.Properties.(map[string]interface{})["description"],
				res.Object().Properties.(map[string]interface{})["description"])
		}
	})

	t.Run("perform vector searches", func(t *testing.T) {
		// note this test assumes a recall of 100% which only works with HNSW on
		// small sizes, so if we use this test suite with massive sizes, we should
		// not expect this test to succeed 100% of times anymore.
		runs := 10

		for i := 0; i < runs; i++ {
			query := make([]float32, vectorDims)
			for i := range query {
				query[i] = rnd.Float32()
			}

			groundTruth := bruteForceObjectsByQuery(data, query)

			node := nodes[rnd.Intn(len(nodes))]
			res, err := node.repo.VectorSearch(context.Background(), dto.GetParams{
				Pagination: &filters.Pagination{
					Limit: 25,
				},
				ClassName: distributedClass,
			}, []string{""}, []models.Vector{query})
			assert.Nil(t, err)
			for i, obj := range res {
				assert.Equal(t, groundTruth[i].ID, obj.ID, fmt.Sprintf("at pos %d", i))
			}
		}

		for _, obj := range data {
			node := nodes[rnd.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID, search.SelectProperties{}, additional.Properties{}, "")
			require.Nil(t, err)
			require.NotNil(t, res)

			// only compare string prop to avoid having to deal with parsing time
			// props
			assert.Equal(t, obj.Properties.(map[string]interface{})["description"],
				res.Object().Properties.(map[string]interface{})["description"])
		}
	})

	t.Run("query individually and resolve references", func(t *testing.T) {
		for _, obj := range refData {
			// if i == 5 {
			// 	break
			// }
			node := nodes[rnd.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID, search.SelectProperties{
				search.SelectProperty{
					Name:        "toFirst",
					IsPrimitive: false,
					Refs: []search.SelectClass{
						{
							ClassName: distributedClass,
							RefProperties: search.SelectProperties{
								search.SelectProperty{
									Name:        "description",
									IsPrimitive: true,
								},
							},
						},
					},
				},
			}, additional.Properties{}, "")
			require.Nil(t, err)
			require.NotNil(t, res)
			props := res.Object().Properties.(map[string]interface{})
			refProp, ok := props["toFirst"].([]interface{})
			require.True(t, ok)

			var refPayload []map[string]interface{}
			for _, res := range refProp {
				parsed, ok := res.(search.LocalRef)
				require.True(t, ok)
				refPayload = append(refPayload, map[string]interface{}{
					"description": parsed.Fields["description"],
				})
			}

			actual := manuallyResolveRef(t, obj, data, "toFirst", "description", nil)
			assert.Equal(t, actual, refPayload)
		}
	})

	t.Run("query individually with cross-ref vectors and resolve references", func(t *testing.T) {
		for _, obj := range refData {
			// if i == 1 {
			// 	break
			// }
			node := nodes[rnd.Intn(len(nodes))]

			res, err := node.repo.Object(context.Background(), obj.Class, obj.ID, search.SelectProperties{
				search.SelectProperty{
					Name:        "toFirst",
					IsPrimitive: false,
					Refs: []search.SelectClass{
						{
							ClassName: distributedClass,
							RefProperties: search.SelectProperties{
								search.SelectProperty{
									Name:        "description",
									IsPrimitive: true,
								},
							},
							AdditionalProperties: additional.Properties{
								Vector: true,
							},
						},
					},
				},
			}, additional.Properties{}, nil, "")
			require.Nil(t, err)
			require.NotNil(t, res)
			props := res.Object().Properties.(map[string]interface{})
			refProp, ok := props["toFirst"].([]interface{})
			require.True(t, ok)

			var refPayload []map[string]interface{}
			var refVector []map[string]interface{}
			for _, ref := range refProp {
				parsed, ok := ref.(search.LocalRef)
				require.True(t, ok)
				refPayload = append(refPayload, map[string]interface{}{
					"description": parsed.Fields["description"],
				})
				vector, ok := parsed.Fields["vector"].([]float32)
				require.True(t, ok)
				require.NotEmpty(t, vector)
				refVector = append(refVector, map[string]interface{}{
					"vector": vector,
				})
			}

			actual := manuallyResolveRef(t, obj, data, "toFirst", "description", nil)
			assert.Equal(t, actual, refPayload)
			actual = manuallyResolveRef(t, obj, data, "toFirst", "vector", node.repo)
			assert.Equal(t, actual, refVector)
		}
	})

	t.Run("ranked keyword search", func(t *testing.T) {
		for i := 0; i < numberOfObjects; i++ {
			description := fmt.Sprintf("object %d", i)
			keywordRanking := &searchparams.KeywordRanking{
				Query:      description,
				Properties: []string{"description"},
			}

			params := dto.GetParams{
				ClassName:      distributedClass,
				KeywordRanking: keywordRanking,
				Pagination:     &filters.Pagination{Limit: 100},
			}

			node := nodes[rnd.Intn(len(nodes))]
			res, err := node.repo.Search(context.Background(), params)
			require.Nil(t, err)
			require.NotEmpty(t, res)

			expected := strings.Join(strings.Split(description, " "), "-")
			received := res[0].Object().Properties.(map[string]interface{})["description"]
			assert.Equal(t, expected, received)
		}
	})

	t.Run("aggregate count", func(t *testing.T) {
		params := aggregation.Params{
			ClassName:        schema.ClassName(distributedClass),
			IncludeMetaCount: true,
		}

		logger, _ := test.NewNullLogger()
		node := nodes[rnd.Intn(len(nodes))]
		res, err := node.repo.Aggregate(context.Background(), params, modules.NewProvider(logger, config.Config{}))
		require.Nil(t, err)

		expectedResult := &aggregation.Result{
			Groups: []aggregation.Group{
				{
					Count: numberOfObjects,
				},
			},
		}

		assert.Equal(t, expectedResult, res)
	})

	t.Run("modify an object using patch", func(t *testing.T) {
		obj := data[0]

		node := nodes[rnd.Intn(len(nodes))]
		err := node.repo.Merge(context.Background(), objects.MergeDocument{
			Class: distributedClass,
			ID:    obj.ID,
			PrimitiveSchema: map[string]interface{}{
				"other_property": "a-value-inserted-through-merge",
			},
		}, nil, "", 0)

		require.Nil(t, err)
	})

	t.Run("verify the patched object contains the additions and orig", func(t *testing.T) {
		obj := data[0]

		node := nodes[rnd.Intn(len(nodes))]
		res, err := node.repo.ObjectByID(context.Background(), obj.ID, search.SelectProperties{}, additional.Properties{}, "")

		require.Nil(t, err)
		previousMap := obj.Properties.(map[string]interface{})
		assert.Equal(t, "a-value-inserted-through-merge", res.Object().Properties.(map[string]interface{})["other_property"])
		assert.Equal(t, previousMap["description"], res.Object().Properties.(map[string]interface{})["description"])
	})

	// This test prevents a regression on
	// https://github.com/weaviate/weaviate/issues/1775
	t.Run("query items by date filter with regular field", func(t *testing.T) {
		count := len(data) / 2 // try to match half the data objects present
		cutoff := time.Unix(0, 0).Add(time.Duration(count) * time.Hour)
		node := nodes[rnd.Intn(len(nodes))]
		res, err := node.repo.Search(context.Background(), dto.GetParams{
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorLessThan,
					On: &filters.Path{
						Class:    distributedClass,
						Property: schema.PropertyName("date_property"),
					},
					Value: &filters.Value{
						Value: cutoff,
						Type:  schema.DataTypeDate,
					},
				},
			},
			ClassName: distributedClass,
			Pagination: &filters.Pagination{
				Limit: len(data),
			},
		})

		require.Nil(t, err)
		assert.Equal(t, count, len(res))
	})

	// This test prevents a regression on
	// https://github.com/weaviate/weaviate/issues/1775
	t.Run("query items by date filter with array field", func(t *testing.T) {
		count := len(data) / 2 // try to match half the data objects present
		cutoff := time.Unix(0, 0).Add(time.Duration(count) * time.Hour)
		node := nodes[rnd.Intn(len(nodes))]
		res, err := node.repo.Search(context.Background(), dto.GetParams{
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorLessThan,
					On: &filters.Path{
						Class:    distributedClass,
						Property: schema.PropertyName("date_array_property"),
					},
					Value: &filters.Value{
						Value: cutoff,
						Type:  schema.DataTypeDate,
					},
				},
			},
			ClassName: distributedClass,
			Pagination: &filters.Pagination{
				Limit: len(data),
			},
		})

		require.Nil(t, err)
		assert.Equal(t, count, len(res))
	})

	t.Run("sort by", func(t *testing.T) {
		getPhoneNumber := func(a search.Result) *float64 {
			prop := a.Object().Properties.(map[string]interface{})["phone_property"]
			if phoneNumber, ok := prop.(*models.PhoneNumber); ok {
				phoneStr := fmt.Sprintf("%v%v", phoneNumber.CountryCode, phoneNumber.National)
				if phone, err := strconv.ParseFloat(phoneStr, 64); err == nil {
					return &phone
				}
			}
			return nil
		}
		getDate := func(a search.Result) *time.Time {
			asString := a.Object().Properties.(map[string]interface{})["date_property"].(string)
			if date, err := time.Parse(time.RFC3339, asString); err == nil {
				return &date
			}
			return nil
		}
		testData := []struct {
			name      string
			sort      []filters.Sort
			compareFn func(a, b search.Result) bool
		}{
			{
				name: "description asc",
				sort: []filters.Sort{{Path: []string{"description"}, Order: "asc"}},
				compareFn: func(a, b search.Result) bool {
					descriptionA := a.Object().Properties.(map[string]interface{})["description"].(string)
					descriptionB := b.Object().Properties.(map[string]interface{})["description"].(string)
					return strings.ToLower(descriptionA) <= strings.ToLower(descriptionB)
				},
			},
			{
				name: "description desc",
				sort: []filters.Sort{{Path: []string{"description"}, Order: "desc"}},
				compareFn: func(a, b search.Result) bool {
					descriptionA := a.Object().Properties.(map[string]interface{})["description"].(string)
					descriptionB := b.Object().Properties.(map[string]interface{})["description"].(string)
					return strings.ToLower(descriptionA) >= strings.ToLower(descriptionB)
				},
			},
			{
				name: "date_property asc",
				sort: []filters.Sort{{Path: []string{"date_property"}, Order: "asc"}},
				compareFn: func(a, b search.Result) bool {
					datePropA, datePropB := getDate(a), getDate(b)
					if datePropA != nil && datePropB != nil {
						return datePropA.Before(*datePropB)
					}
					return false
				},
			},
			{
				name: "date_property desc",
				sort: []filters.Sort{{Path: []string{"date_property"}, Order: "desc"}},
				compareFn: func(a, b search.Result) bool {
					datePropA, datePropB := getDate(a), getDate(b)
					if datePropA != nil && datePropB != nil {
						return datePropA.After(*datePropB)
					}
					return false
				},
			},
			{
				name: "int_property asc",
				sort: []filters.Sort{{Path: []string{"int_property"}, Order: "asc"}},
				compareFn: func(a, b search.Result) bool {
					intPropertyA := a.Object().Properties.(map[string]interface{})["int_property"].(float64)
					intPropertyB := b.Object().Properties.(map[string]interface{})["int_property"].(float64)
					return intPropertyA <= intPropertyB
				},
			},
			{
				name: "int_property desc",
				sort: []filters.Sort{{Path: []string{"int_property"}, Order: "desc"}},
				compareFn: func(a, b search.Result) bool {
					intPropertyA := a.Object().Properties.(map[string]interface{})["int_property"].(float64)
					intPropertyB := b.Object().Properties.(map[string]interface{})["int_property"].(float64)
					return intPropertyA >= intPropertyB
				},
			},
			{
				name: "phone_property asc",
				sort: []filters.Sort{{Path: []string{"phone_property"}, Order: "asc"}},
				compareFn: func(a, b search.Result) bool {
					phoneA, phoneB := getPhoneNumber(a), getPhoneNumber(b)
					if phoneA != nil && phoneB != nil {
						return *phoneA <= *phoneB
					}
					return false
				},
			},
			{
				name: "phone_property desc",
				sort: []filters.Sort{{Path: []string{"phone_property"}, Order: "desc"}},
				compareFn: func(a, b search.Result) bool {
					phoneA, phoneB := getPhoneNumber(a), getPhoneNumber(b)
					if phoneA != nil && phoneB != nil {
						return *phoneA >= *phoneB
					}
					return false
				},
			},
		}
		for _, td := range testData {
			t.Run(td.name, func(t *testing.T) {
				params := dto.GetParams{
					ClassName:  distributedClass,
					Sort:       td.sort,
					Pagination: &filters.Pagination{Limit: 100},
					Properties: search.SelectProperties{{Name: "description"}},
				}

				node := nodes[rnd.Intn(len(nodes))]
				res, err := node.repo.Search(context.Background(), params)
				require.Nil(t, err)
				require.NotEmpty(t, res)

				if len(res) > 1 {
					for i := 1; i < len(res); i++ {
						assert.True(t, td.compareFn(res[i-1], res[i]))
					}
				}
			})
		}
	})

	t.Run("node names by shard", func(t *testing.T) {
		for _, n := range nodes {
			nodeSet := make(map[string]bool)
			foundNodes, err := n.repo.Shards(context.Background(), distributedClass)
			assert.NoError(t, err)
			for _, found := range foundNodes {
				nodeSet[found] = true
			}
			assert.Len(t, nodeSet, numberOfNodes, "expected %d nodes, got %d",
				numberOfNodes, len(foundNodes))
		}
	})

	t.Run("delete a third of the data from random nodes", func(t *testing.T) {
		for i, obj := range data {
			if i%3 != 0 {
				// keep this item
				continue
			}

			node := nodes[rnd.Intn(len(nodes))]
			err := node.repo.DeleteObject(context.Background(), distributedClass, obj.ID, time.Now(), nil, "", 0)
			require.Nil(t, err)
		}
	})

	t.Run("make sure 2/3 exist, 1/3 no longer exists", func(t *testing.T) {
		for i, obj := range data {
			expected := true
			if i%3 == 0 {
				expected = false
			}

			node := nodes[rnd.Intn(len(nodes))]
			actual, err := node.repo.Exists(context.Background(), distributedClass, obj.ID, nil, "")
			require.Nil(t, err)
			assert.Equal(t, expected, actual)
		}
	})

	t.Run("batch delete the remaining 2/3 of data", func(t *testing.T) {
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
		performClassSearch := func(repo *db.DB, className string) ([]search.Result, error) {
			return repo.Search(context.Background(), dto.GetParams{
				ClassName:  className,
				Pagination: &filters.Pagination{Limit: 10000},
			})
		}
		node := nodes[rnd.Intn(len(nodes))]
		// get the initial count of the objects
		res, err := performClassSearch(node.repo, distributedClass)
		require.Nil(t, err)
		beforeDelete := len(res)
		require.True(t, beforeDelete > 0)
		// dryRun == false, perform actual delete
		batchDeleteRes, err := node.repo.BatchDeleteObjects(context.Background(), getParams(distributedClass, false), time.Now(), nil, "", 0)
		require.Nil(t, err)
		require.Equal(t, int64(beforeDelete), batchDeleteRes.Matches)
		require.Equal(t, beforeDelete, len(batchDeleteRes.Objects))
		for _, batchRes := range batchDeleteRes.Objects {
			require.Nil(t, batchRes.Err)
		}
		// check that every object is deleted
		res, err = performClassSearch(node.repo, distributedClass)
		require.Nil(t, err)
		require.Equal(t, 0, len(res))
	})

	t.Run("shutdown", func(t *testing.T) {
		for _, node := range nodes {
			node.repo.Shutdown(context.Background())
		}
	})
}

func TestDistributedVectorDistance(t *testing.T) {
	dirName := t.TempDir()
	rnd := getRandomSeed()
	ctx := context.Background()
	cases := []struct {
		asyncIndexing bool
	}{
		{asyncIndexing: true},
		{asyncIndexing: false},
	}
	for _, tt := range cases {
		t.Run("async indexing:"+strconv.FormatBool(tt.asyncIndexing), func(t *testing.T) {
			os.Setenv("ASYNC_INDEXING", strconv.FormatBool(tt.asyncIndexing))

			collection := multiVectorClass(tt.asyncIndexing)

			overallShardState := multiShardState(numberOfNodes)
			shardStateSerialized, err := json.Marshal(overallShardState)
			require.Nil(t, err)

			var nodes []*node
			for i := 0; i < numberOfNodes; i++ {
				node := &node{
					name: fmt.Sprintf("node-%d", i),
				}

				node.init(dirName, shardStateSerialized, &nodes)
				nodes = append(nodes, node)
			}

			for i := range nodes {
				require.Nil(t, nodes[i].migrator.AddClass(context.Background(), collection,
					nodes[i].schemaManager.shardState))
				nodes[i].schemaManager.schema.Objects.Classes = append(nodes[i].schemaManager.schema.Objects.Classes,
					collection)
			}

			uid := strfmt.UUID(uuid.New().String())

			vectors := [][]float32{
				{1, 0, 0, 0},
				{0, 1, 0, 0},
				{0, 0, 1, 0},
				{0, 0, 0, 1},
			}

			t.Run("get all targets", func(t *testing.T) {
				obj := &models.Object{
					ID:      uid,
					Class:   collection.Class,
					Vectors: map[string]models.Vector{"custom1": vectors[0], "custom2": vectors[1], "custom3": vectors[2]},
				}
				objVectors, _, err := dto.GetVectors(obj.Vectors)
				require.NoError(t, err)
				require.Nil(t, nodes[rnd.Intn(len(nodes))].repo.PutObject(context.Background(), obj, nil, objVectors, nil, nil, 0))

				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					res, err := nodes[rnd.Intn(len(nodes))].repo.VectorSearch(ctx, createParams(collection.Class, nil), []string{"custom1", "custom2", "custom3"}, []models.Vector{vectors[1], vectors[2], vectors[3]})
					if !assert.Nil(collect, err) {
						return
					}
					if !assert.Len(collect, res, 1) {
						return
					}
					if !assert.Equal(collect, res[0].ID, obj.ID) {
						return
					}
					if !assert.Equal(collect, res[0].Dist, float32(1)) {
						return
					}

					assert.Nil(collect, nodes[rnd.Intn(len(nodes))].repo.DeleteObject(context.Background(), collection.Class, obj.ID, time.Now(), nil, "", 0))
				}, 20*time.Second, 1*time.Second)
			})

			t.Run("get some targets", func(t *testing.T) {
				obj := &models.Object{
					ID:      uid,
					Class:   collection.Class,
					Vectors: map[string]models.Vector{"custom1": vectors[0], "custom2": vectors[1], "custom3": vectors[2]},
				}
				objVectors, _, err := dto.GetVectors(obj.Vectors)
				require.NoError(t, err)
				require.Nil(t, nodes[rnd.Intn(len(nodes))].repo.PutObject(context.Background(), obj, nil, objVectors, nil, nil, 0))

				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					res, err := nodes[rnd.Intn(len(nodes))].repo.VectorSearch(ctx, createParams(collection.Class, nil), []string{"custom1", "custom2"}, []models.Vector{vectors[1], vectors[2]})
					if !assert.Nil(collect, err) {
						return
					}
					if !assert.Equal(collect, res[0].ID, obj.ID) {
						return
					}
					if !assert.Equal(collect, res[0].Dist, float32(1)) {
						return
					}

					assert.Nil(collect, nodes[rnd.Intn(len(nodes))].repo.DeleteObject(context.Background(), collection.Class, obj.ID, time.Now(), nil, "", 0))
				}, 20*time.Second, 1*time.Second)
			})

			t.Run("get non-existing target", func(t *testing.T) {
				start := time.Now()
				obj := &models.Object{
					ID:      uid,
					Class:   collection.Class,
					Vectors: map[string]models.Vector{"custom1": vectors[0], "custom2": vectors[1]},
				}
				objVectors, _, err := dto.GetVectors(obj.Vectors)
				require.NoError(t, err)
				require.Nil(t, nodes[rnd.Intn(len(nodes))].repo.PutObject(context.Background(), obj, nil, objVectors, nil, nil, 0))

				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					res, err := nodes[rnd.Intn(len(nodes))].repo.VectorSearch(ctx, createParams(collection.Class, []float32{1, 1}), []string{"custom1", "custom3"}, []models.Vector{vectors[1], vectors[2]})
					if !assert.Nil(collect, err) {
						return
					}
					if !assert.Len(collect, res, 0) { // no results because we are searching for target custom3 which the only object does not have
						return
					}

					assert.True(collect, time.Since(start) < 19*time.Second) // this will fail if the remote call is retried and should be long enough to never be triggerd in normal conditions
				}, 20*time.Second, 1*time.Second)
			})

			t.Run("Multiple objects", func(t *testing.T) {
				ids := make([]strfmt.UUID, 50)
				for i := range ids {
					obj := &models.Object{
						ID:      strfmt.UUID(uuid.New().String()),
						Class:   collection.Class,
						Vectors: map[string]models.Vector{"custom1": vectors[i%len(vectors)], "custom2": vectors[(i+1)%len(vectors)], "custom3": vectors[(i+2)%len(vectors)]},
					}
					ids[i] = obj.ID
					vectors, _, err := dto.GetVectors(obj.Vectors)
					require.NoError(t, err)
					require.Nil(t, nodes[rnd.Intn(len(nodes))].repo.PutObject(context.Background(), obj, nil, vectors, nil, nil, 0))
				}

				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					res, err := nodes[rnd.Intn(len(nodes))].repo.VectorSearch(ctx, createParams(collection.Class, []float32{1, 1}), []string{"custom1", "custom3"}, []models.Vector{vectors[1], vectors[2]})
					assert.Nil(collect, err)
					assert.Greater(collect, len(res), 0)
				}, 20*time.Second, 1*time.Second)
			})

			for _, node := range nodes {
				node.repo.Shutdown(context.Background())
			}
		})
	}
}
