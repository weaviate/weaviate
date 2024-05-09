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
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/get"
	test_helper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	text2vecadditional "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional"

	text2vecadditionalsempath "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/sempath"

	text2vecadditionalprojector "github.com/weaviate/weaviate/usecases/modulecomponents/additional/projector"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/objects"

	text2vecneartext "github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
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

				err := node.repo.PutObject(context.Background(), obj, obj.Vector, nil, nil, 0)
				require.Nil(t, err)
			}
		})
		t.Run("import second class with refs by picking a random node", func(t *testing.T) {
			for _, obj := range refData {
				node := nodes[rnd.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector, nil, nil, 0)
				require.Nil(t, err)

			}
		})
	}
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
				SearchVector: query,
				Pagination: &filters.Pagination{
					Limit: 25,
				},
				ClassName: distributedClass,
			})
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

		node := nodes[rnd.Intn(len(nodes))]
		res, err := node.repo.Aggregate(context.Background(), params, getFakeModulesProvider().(*modules.Provider))
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
			err := node.repo.DeleteObject(context.Background(), distributedClass, obj.ID, nil, "", 0)
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
		batchDeleteRes, err := node.repo.BatchDeleteObjects(context.Background(), getParams(distributedClass, false), nil, "", 0)
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

type mockText2vecContextionaryModule struct{}

func (m *mockText2vecContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *mockText2vecContextionaryModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *mockText2vecContextionaryModule) RootHandler() http.Handler {
	return nil
}

// graphql arguments
func (m *mockText2vecContextionaryModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return text2vecneartext.New(nil).Arguments()
}

// additional properties
func (m *mockText2vecContextionaryModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return text2vecadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}, &fakeInterpretation{}).AdditionalProperties()
}

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	test_helper.MockResolver
}

type fakeInterpretation struct{}

func (f *fakeInterpretation) AdditonalPropertyDefaultValue() interface{} {
	return true
}

func (f *fakeInterpretation) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return in, nil
}

func (f *fakeInterpretation) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeInterpretation) AdditionalPropertyDefaultValue() interface{} {
	return true
}



type fakeProjectorParams struct {
	Enabled          bool
	Algorithm        string
	Dimensions       int
	Perplexity       int
	Iterations       int
	LearningRate     int
	IncludeNeighbors bool
}

type fakeExtender struct {
	returnArgs []search.Result
}

func (f *fakeExtender) AdditonalPropertyDefaultValue() interface{} {
	return true
}


func (f *fakeExtender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeExtender) AdditionalPropertyDefaultValue() interface{} {
	return true
}

type fakeProjector struct {
	returnArgs []search.Result
}
func (f *fakeProjector) AdditonalPropertyDefaultValue() interface{} {
	return &fakeProjectorParams{}
}


func (f *fakeProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	if len(param) > 0 {
		p := &text2vecadditionalprojector.Params{}
		err := p.SetDefaultsAndValidate(100, 4)
		if err != nil {
			return nil
		}
		return p
	}
	return &text2vecadditionalprojector.Params{
		Enabled: true,
	}
}

func (f *fakeProjector) AdditionalPropertyDefaultValue() interface{} {
	return &text2vecadditionalprojector.Params{}
}

type fakePathBuilder struct {
	returnArgs []search.Result
}

type pathBuilderParams struct{}


func (f *fakePathBuilder) AdditonalPropertyDefaultValue() interface{} {
	return &pathBuilderParams{}
}

func (f *fakePathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &text2vecadditionalsempath.Params{}
}

func (f *fakePathBuilder) AdditionalPropertyDefaultValue() interface{} {
	return &text2vecadditionalsempath.Params{}
}

type nearCustomTextParams struct {
	Values        []string
	MoveTo        nearExploreMove
	MoveAwayFrom  nearExploreMove
	Certainty     float64
	Distance      float64
	WithDistance  bool
	TargetVectors []string
}

// implements the modulecapabilities.NearParam interface
func (n *nearCustomTextParams) GetCertainty() float64 {
	return n.Certainty
}

func (n nearCustomTextParams) GetDistance() float64 {
	return n.Distance
}

func (n nearCustomTextParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (n nearCustomTextParams) GetTargetVectors() []string {
	return n.TargetVectors
}

type nearExploreMove struct {
	Values  []string
	Force   float32
	Objects []nearObjectMove
}

type nearObjectMove struct {
	ID     string
	Beacon string
}

type nearCustomTextModule struct {
	fakePathBuilder    *fakePathBuilder
	fakeProjector      *fakeProjector
	fakeExtender       *fakeExtender
	fakeInterpretation *fakeInterpretation
}

func newNearCustomTextModule() *nearCustomTextModule {
	return &nearCustomTextModule{
		fakePathBuilder:    &fakePathBuilder{},
		fakeProjector:      &fakeProjector{},
		fakeExtender:       &fakeExtender{},
		fakeInterpretation: &fakeInterpretation{},
	}
}

func (m *nearCustomTextModule) Name() string {
	return "mock-custom-near-text-module"
}

func (m *nearCustomTextModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *nearCustomTextModule) RootHandler() http.Handler {
	return nil
}

func (m *nearCustomTextModule) getNearCustomTextArgument(classname string) *graphql.ArgumentConfig {
	prefix := classname
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: fmt.Sprintf("%sNearCustomTextInpObj", prefix),
				Fields: graphql.InputObjectConfigFieldMap{
					"concepts": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
					},
					"moveTo": &graphql.InputObjectFieldConfig{
						Description: descriptions.VectorMovement,
						Type: graphql.NewInputObject(
							graphql.InputObjectConfig{
								Name: fmt.Sprintf("%sMoveTo", prefix),
								Fields: graphql.InputObjectConfigFieldMap{
									"concepts": &graphql.InputObjectFieldConfig{
										Description: descriptions.Keywords,
										Type:        graphql.NewList(graphql.String),
									},
									"objects": &graphql.InputObjectFieldConfig{
										Description: "objects",
										Type: graphql.NewList(graphql.NewInputObject(
											graphql.InputObjectConfig{
												Name: fmt.Sprintf("%sMovementObjectsToInpObj", prefix),
												Fields: graphql.InputObjectConfigFieldMap{
													"id": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: "id of an object",
													},
													"beacon": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: descriptions.Beacon,
													},
												},
												Description: "Movement Object",
											},
										)),
									},
									"force": &graphql.InputObjectFieldConfig{
										Description: descriptions.Force,
										Type:        graphql.NewNonNull(graphql.Float),
									},
								},
							}),
					},
					"moveAwayFrom": &graphql.InputObjectFieldConfig{
						Description: descriptions.VectorMovement,
						Type: graphql.NewInputObject(
							graphql.InputObjectConfig{
								Name: fmt.Sprintf("%sMoveAway", prefix),
								Fields: graphql.InputObjectConfigFieldMap{
									"concepts": &graphql.InputObjectFieldConfig{
										Description: descriptions.Keywords,
										Type:        graphql.NewList(graphql.String),
									},
									"objects": &graphql.InputObjectFieldConfig{
										Description: "objects",
										Type: graphql.NewList(graphql.NewInputObject(
											graphql.InputObjectConfig{
												Name: fmt.Sprintf("%sMovementObjectsAwayInpObj", prefix),
												Fields: graphql.InputObjectConfigFieldMap{
													"id": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: "id of an object",
													},
													"beacon": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: descriptions.Beacon,
													},
												},
												Description: "Movement Object",
											},
										)),
									},
									"force": &graphql.InputObjectFieldConfig{
										Description: descriptions.Force,
										Type:        graphql.NewNonNull(graphql.Float),
									},
								},
							}),
					},
					"certainty": &graphql.InputObjectFieldConfig{
						Description: descriptions.Certainty,
						Type:        graphql.Float,
					},
					"distance": &graphql.InputObjectFieldConfig{
						Description: descriptions.Distance,
						Type:        graphql.Float,
					},
					"targetVectors": &graphql.InputObjectFieldConfig{
						Description: "Target vectors",
						Type:        graphql.NewList(graphql.String),
					},
				},
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func (m *nearCustomTextModule) extractNearCustomTextArgument(source map[string]interface{}) *nearCustomTextParams {
	var args nearCustomTextParams

	concepts := source["concepts"].([]interface{})
	args.Values = make([]string, len(concepts))
	for i, value := range concepts {
		args.Values[i] = value.(string)
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	distance, ok := source["distance"]
	if ok {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	// moveTo is an optional arg, so it could be nil
	moveTo, ok := source["moveTo"]
	if ok {
		moveToMap := moveTo.(map[string]interface{})
		args.MoveTo = m.parseMoveParam(moveToMap)
	}

	moveAwayFrom, ok := source["moveAwayFrom"]
	if ok {
		moveAwayFromMap := moveAwayFrom.(map[string]interface{})
		args.MoveAwayFrom = m.parseMoveParam(moveAwayFromMap)
	}

	return &args
}

func (m *nearCustomTextModule) parseMoveParam(source map[string]interface{}) nearExploreMove {
	res := nearExploreMove{}
	res.Force = float32(source["force"].(float64))

	concepts, ok := source["concepts"].([]interface{})
	if ok {
		res.Values = make([]string, len(concepts))
		for i, value := range concepts {
			res.Values[i] = value.(string)
		}
	}

	objects, ok := source["objects"].([]interface{})
	if ok {
		res.Objects = make([]nearObjectMove, len(objects))
		for i, value := range objects {
			v, ok := value.(map[string]interface{})
			if ok {
				if v["id"] != nil {
					res.Objects[i].ID = v["id"].(string)
				}
				if v["beacon"] != nil {
					res.Objects[i].Beacon = v["beacon"].(string)
				}
			}
		}
	}

	return res
}

func (m *nearCustomTextModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	// define nearCustomText argument
	arguments["nearCustomText"] = modulecapabilities.GraphQLArgument{
		GetArgumentsFunction: func(classname string) *graphql.ArgumentConfig {
			return m.getNearCustomTextArgument(classname)
		},
		ExtractFunction: func(source map[string]interface{}) interface{} {
			return m.extractNearCustomTextArgument(source)
		},
		ValidateFunction: func(param interface{}) error {
			// all is valid
			return nil
		},
	}
	return arguments
}

// additional properties
func (m *nearCustomTextModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["featureProjection"] = m.getFeatureProjection()
	additionalProperties["nearestNeighbors"] = m.getNearestNeighbors()
	additionalProperties["semanticPath"] = m.getSemanticPath()
	additionalProperties["interpretation"] = m.getInterpretation()
	return additionalProperties
}

func (m *nearCustomTextModule) getFeatureProjection() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeProjector.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"featureProjection"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Args: graphql.FieldConfigArgument{
					"algorithm": &graphql.ArgumentConfig{
						Type:         graphql.String,
						DefaultValue: nil,
					},
					"dimensions": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
					"learningRate": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
					"iterations": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
					"perplexity": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
				},
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalFeatureProjection", classname),
					Fields: graphql.Fields{
						"vector": &graphql.Field{Type: graphql.NewList(graphql.Float)},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakeProjector.ExtractAdditionalFn,
	}
}

func (m *nearCustomTextModule) getNearestNeighbors() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeExtender.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"nearestNeighbors"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalNearestNeighbors", classname),
					Fields: graphql.Fields{
						"neighbors": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("%sAdditionalNearestNeighborsNeighbors", classname),
							Fields: graphql.Fields{
								"concept":  &graphql.Field{Type: graphql.String},
								"distance": &graphql.Field{Type: graphql.Float},
							},
						}))},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakeExtender.ExtractAdditionalFn,
	}
}

func (m *nearCustomTextModule) getSemanticPath() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakePathBuilder.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"semanticPath"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalSemanticPath", classname),
					Fields: graphql.Fields{
						"path": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("%sAdditionalSemanticPathElement", classname),
							Fields: graphql.Fields{
								"concept":            &graphql.Field{Type: graphql.String},
								"distanceToQuery":    &graphql.Field{Type: graphql.Float},
								"distanceToResult":   &graphql.Field{Type: graphql.Float},
								"distanceToNext":     &graphql.Field{Type: graphql.Float},
								"distanceToPrevious": &graphql.Field{Type: graphql.Float},
							},
						}))},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakePathBuilder.ExtractAdditionalFn,
	}
}

func (m *nearCustomTextModule) getInterpretation() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeInterpretation.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"interpretation"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalInterpretation", classname),
					Fields: graphql.Fields{
						"source": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("%sAdditionalInterpretationSource", classname),
							Fields: graphql.Fields{
								"concept":    &graphql.Field{Type: graphql.String},
								"weight":     &graphql.Field{Type: graphql.Float},
								"occurrence": &graphql.Field{Type: graphql.Int},
							},
						}))},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakeInterpretation.ExtractAdditionalFn,
	}
}

type fakeModulesProvider struct {
	nearCustomTextModule *nearCustomTextModule
}

func newFakeModulesProvider() *fakeModulesProvider {
	return &fakeModulesProvider{newNearCustomTextModule()}
}

func (fmp *fakeModulesProvider) GetAll() []modulecapabilities.Module {
	panic("implement me")
}

func (fmp *fakeModulesProvider) AggregateArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	panic("not implemented")
}

func (fmp *fakeModulesProvider) VectorFromInput(ctx context.Context, className, input, targetVector string) ([]float32, error) {
	panic("not implemented")
}

func (fmp *fakeModulesProvider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	args := map[string]*graphql.ArgumentConfig{}
	if class.Vectorizer == fmp.nearCustomTextModule.Name() {
		for name, argument := range fmp.nearCustomTextModule.Arguments() {
			args[name] = argument.GetArgumentsFunction(class.Class)
		}
	}
	return args
}

func (fmp *fakeModulesProvider) ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	if param, ok := arguments["nearCustomText"]; ok {
		exractedParams["nearCustomText"] = extractNearTextParam(param.(map[string]interface{}))
	}
	return exractedParams
}

func (fmp *fakeModulesProvider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	for name, additionalProperty := range fmp.nearCustomTextModule.AdditionalProperties() {
		if additionalProperty.GraphQLFieldFunction != nil {
			additionalProperties[name] = additionalProperty.GraphQLFieldFunction(class.Class)
		}
	}
	return additionalProperties
}

func (fmp *fakeModulesProvider) ExtractAdditionalField(className, name string, params []*ast.Argument) interface{} {
	if additionalProperties := fmp.nearCustomTextModule.AdditionalProperties(); len(additionalProperties) > 0 {
		if additionalProperty, ok := additionalProperties[name]; ok {
			if additionalProperty.GraphQLExtractFunction != nil {
				return additionalProperty.GraphQLExtractFunction(params)
			}
		}
	}
	return nil
}

func (fmp *fakeModulesProvider) GraphQLAdditionalFieldNames() []string {
	additionalPropertiesNames := []string{}
	for _, additionalProperty := range fmp.nearCustomTextModule.AdditionalProperties() {
		if additionalProperty.GraphQLNames != nil {
			additionalPropertiesNames = append(additionalPropertiesNames, additionalProperty.GraphQLNames...)
		}
	}
	return additionalPropertiesNames
}

func extractNearTextParam(param map[string]interface{}) interface{} {
	nearCustomTextModule := newNearCustomTextModule()
	argument := nearCustomTextModule.Arguments()["nearCustomText"]
	return argument.ExtractFunction(param)
}

func createArg(name string, value string) *ast.Argument {
	n := ast.Name{
		Value: name,
	}
	val := ast.StringValue{
		Kind:  "Kind",
		Value: value,
	}
	arg := ast.Argument{
		Name:  ast.NewName(&n),
		Kind:  "Kind",
		Value: ast.NewStringValue(&val),
	}
	a := ast.NewArgument(&arg)
	return a
}

func extractAdditionalParam(name string, args []*ast.Argument) interface{} {
	nearCustomTextModule := newNearCustomTextModule()
	additionalProperties := nearCustomTextModule.AdditionalProperties()
	switch name {
	case "semanticPath", "featureProjection":
		if ap, ok := additionalProperties[name]; ok {
			return ap.GraphQLExtractFunction(args)
		}
		return nil
	default:
		return nil
	}
}

func getFakeModulesProvider() ModulesProvider {
	return newFakeModulesProvider()
}

func newMockResolver() *mockResolver {
	return newMockResolverWithVectorizer(config.VectorizerModuleText2VecContextionary)
}

func newMockResolverWithVectorizer(vectorizer string) *mockResolver {
	logger, _ := test.NewNullLogger()
	simpleSchema := test_helper.CreateSimpleSchema(vectorizer)
	field, err := get.Build(&simpleSchema, logger, getFakeModulesProvider().(*modules.Provider))
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": get.Resolver(mocker), "RequestsLog": get.RequestsLog(mockLog)}
	return mocker
}

func newMockResolverWithNoModules() *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := get.Build(&test_helper.SimpleSchema, logger, nil)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": get.Resolver(mocker), "RequestsLog": get.RequestsLog(mockLog)}
	return mocker
}

func (m *mockResolver) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]interface{}, error) {
	args := m.Called(params)
	return args.Get(0).([]interface{}), args.Error(1)
}


type ModulesProvider interface {
	AggregateArguments(class *models.Class) map[string]*graphql.ArgumentConfig
	ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{}
}


type RequestsLogger interface {
	get.RequestsLog
}