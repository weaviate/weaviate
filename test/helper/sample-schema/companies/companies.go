//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package companies

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const DefaultTimeout = 2 * time.Minute

const (
	OpenAI strfmt.UUID = "00000000-0000-0000-0000-000000000001"
	SpaceX strfmt.UUID = "00000000-0000-0000-0000-000000000002"
)

var Companies = []struct {
	ID                strfmt.UUID
	Name, Description string
}{
	{
		ID:   OpenAI,
		Name: "OpenAI",
		Description: `OpenAI is a research organization and AI development company that focuses on artificial intelligence (AI) and machine learning (ML).
				Founded in December 2015, OpenAI's mission is to ensure that artificial general intelligence (AGI) benefits all of humanity.
				The organization has been at the forefront of AI research, producing cutting-edge advancements in natural language processing,
				reinforcement learning, robotics, and other AI-related fields.

				OpenAI has garnered attention for its work on various projects, including the development of the GPT (Generative Pre-trained Transformer)
				series of models, such as GPT-2 and GPT-3, which have demonstrated remarkable capabilities in generating human-like text.
				Additionally, OpenAI has contributed to advancements in reinforcement learning through projects like OpenAI Five, an AI system
				capable of playing the complex strategy game Dota 2 at a high level.`,
	},
	{
		ID:   SpaceX,
		Name: "SpaceX",
		Description: `SpaceX, short for Space Exploration Technologies Corp., is an American aerospace manufacturer and space transportation company
				founded by Elon Musk in 2002. The company's primary goal is to reduce space transportation costs and enable the colonization of Mars,
				among other ambitious objectives.

				SpaceX has made significant strides in the aerospace industry by developing advanced rocket technology, spacecraft,
				and satellite systems. The company is best known for its Falcon series of rockets, including the Falcon 1, Falcon 9, 
				and Falcon Heavy, which have been designed with reusability in mind. Reusability has been a key innovation pioneered by SpaceX,
				aiming to drastically reduce the cost of space travel by reusing rocket components multiple times.`,
	},
}

func BaseClass(className string) *models.Class {
	return &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name: "name", DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name: "description", DataType: []string{schema.DataTypeText.String()},
			},
		},
	}
}

func InsertObjects(t *testing.T, host string, className string) {
	for _, company := range Companies {
		obj := &models.Object{
			Class: className,
			ID:    company.ID,
			Properties: map[string]any{
				"name":        company.Name,
				"description": company.Description,
			},
		}
		insertObject(t, host, obj)
	}
}

func InsertObjectsWithEmpty(t *testing.T, host string, className string) {
	for _, company := range Companies {
		var empty string
		if company.Name == "SpaceX" {
			empty = "not empty for spacex"
		}
		obj := &models.Object{
			Class: className,
			ID:    company.ID,
			Properties: map[string]any{
				"name":        company.Name,
				"description": company.Description,
				"empty":       empty,
			},
		}
		insertObject(t, host, obj)
	}
}

func insertObject(t *testing.T, host string, obj *models.Object) {
	helper.SetupClient(host)
	err := helper.CreateObjectWithTimeout(t, obj, DefaultTimeout)
	require.NoError(t, err)
	getObj := helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
	require.NotNil(t, getObj)
}

func BatchInsertObjects(t *testing.T, host string, className string) {
	var objects []*models.Object
	for _, company := range Companies {
		objects = append(objects, &models.Object{
			Class: className,
			ID:    company.ID,
			Properties: map[string]any{
				"name":        company.Name,
				"description": company.Description,
			},
		})
	}
	batchInsertObjects(t, host, objects)
}

func BatchInsertObjectsWithEmpty(t *testing.T, host string, className string) {
	var objects []*models.Object
	for _, company := range Companies {
		var empty string
		if company.Name == "SpaceX" {
			empty = "not empty for spacex"
		}
		objects = append(objects, &models.Object{
			Class: className,
			ID:    company.ID,
			Properties: map[string]any{
				"name":        company.Name,
				"description": company.Description,
				"empty":       empty,
			},
		})
	}
	batchInsertObjects(t, host, objects)
}

func batchInsertObjects(t *testing.T, host string, objects []*models.Object) {
	helper.SetupClient(host)

	returnedFields := "ALL"
	params := batch.NewBatchObjectsCreateParams().WithBody(
		batch.BatchObjectsCreateBody{
			Objects: objects,
			Fields:  []*string{&returnedFields},
		})

	resp, err := helper.BatchClient(t).BatchObjectsCreate(params, nil)

	// ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		objectsCreateResponse := resp.Payload

		// check if the batch response contains two batched responses
		assert.Equal(t, 2, len(objectsCreateResponse))

		for _, elem := range resp.Payload {
			assert.Nil(t, elem.Result.Errors)
		}
	})
}

func PerformAllSearchTests(t *testing.T, rest, grpc string, className string) {
	PerformAllSearchTestsWithTargetVector(t, rest, grpc, className, "")
}

func PerformAllSearchTestsWithTargetVector(t *testing.T, rest, grpc, className, targetVector string) {
	// vector search with gql
	t.Run("perform vector search with gql", func(t *testing.T) {
		PerformVectorSearchTestWithTargetVector(t, rest, className, targetVector)
	})
	// vector search with grpc
	t.Run("perform vector search with grpc", func(t *testing.T) {
		PerformVectorSearchGRPCTestWithTargetVector(t, grpc, className, targetVector)
	})
	// hybrid search with gql
	t.Run("perform hybrid search with gql", func(t *testing.T) {
		PerformHybridSearchTestWithTargetVector(t, rest, className, targetVector)
	})
	// hybrid search with grpc
	t.Run("perform hybrid search with grpc", func(t *testing.T) {
		PerformHybridSearchGRPCTestWithTargetVector(t, grpc, className, targetVector)
	})
}

func PerformVectorSearchTest(t *testing.T, host, className string) {
	PerformVectorSearchTestWithTargetVector(t, host, className, "")
}

func PerformVectorSearchTestWithTargetVector(t *testing.T, host, className, targetVector string) {
	targetVectors := ""
	if targetVector != "" {
		targetVectors = fmt.Sprintf(`targetVectors:["%s"]`, targetVector)
	}
	query := fmt.Sprintf(`
				{
					Get {
						%s(
							nearText:{
								concepts:["SpaceX"]
								%s
							}
						){
							name
							_additional {
								id
							}
						}
					}
				}
			`, className, targetVectors)
	assertResults(t, host, className, query)
}

func PerformVectorSearchGRPCTest(t *testing.T, host, className string) {
	PerformVectorSearchGRPCTestWithTargetVector(t, host, className, "")
}

func PerformVectorSearchGRPCTestWithTargetVector(t *testing.T, host, className, targetVector string) {
	var targets *protocol.Targets
	if targetVector != "" {
		targets = &protocol.Targets{TargetVectors: []string{targetVector}}
	}
	req := protocol.SearchRequest{
		Collection: className,
		NearText: &protocol.NearTextSearch{
			Query:   []string{"SpaceX"},
			Targets: targets,
		},
		Properties: &protocol.PropertiesRequest{
			NonRefProperties: []string{"name"},
		},
		Metadata: &protocol.MetadataRequest{
			Uuid: true,
		},
		Uses_127Api: true,
	}
	assertResultsGRPC(t, host, &req)
}

func PerformHybridSearchWithTextTestWithTargetVector(t *testing.T, host string, className, text, targetVector string) {
	targetVectors := ""
	if targetVector != "" {
		targetVectors = fmt.Sprintf(`targetVectors:["%s"]`, targetVector)
	}
	query := fmt.Sprintf(`
				{
					Get {
						%s(
							hybrid:{
								query:"%s"
								alpha:0.75
								%s
							}
						){
							name
							_additional {
								id
							}
						}
					}
				}
			`, className, text, targetVectors)
	assertResults(t, host, className, query)
}

func PerformHybridSearchTest(t *testing.T, host, className string) {
	PerformHybridSearchWithTextTestWithTargetVector(t, host, className, "SpaceX", "")
}

func PerformHybridSearchTestWithTargetVector(t *testing.T, host, className, targetVector string) {
	PerformHybridSearchWithTextTestWithTargetVector(t, host, className, "SpaceX", targetVector)
}

func PerformHybridSearchGRPCTest(t *testing.T, host string, className string) {
	req := protocol.SearchRequest{
		Collection: className,
		HybridSearch: &protocol.Hybrid{
			Query:   "SpaceX",
			Alpha:   0.75,
			Targets: &protocol.Targets{TargetVectors: []string{"description"}},
		},
		Properties: &protocol.PropertiesRequest{
			NonRefProperties: []string{"name"},
		},
		Metadata: &protocol.MetadataRequest{
			Uuid: true,
		},
		Uses_127Api: true,
	}
	assertResultsGRPC(t, host, &req)
}

func PerformHybridSearchGRPCTestWithTargetVector(t *testing.T, host, className, targetVector string) {
	var targets *protocol.Targets
	if targetVector != "" {
		targets = &protocol.Targets{TargetVectors: []string{targetVector}}
	}
	req := protocol.SearchRequest{
		Collection: className,
		HybridSearch: &protocol.Hybrid{
			Query:   "SpaceX",
			Alpha:   0.75,
			Targets: targets,
		},
		Properties: &protocol.PropertiesRequest{
			NonRefProperties: []string{"name"},
		},
		Metadata: &protocol.MetadataRequest{
			Uuid: true,
		},
		Uses_127Api: true,
	}
	assertResultsGRPC(t, host, &req)
}

func assertResults(t *testing.T, host string, className, query string) {
	helper.SetupClient(host)
	result := graphqlhelper.AssertGraphQLWithTimeout(t, helper.RootAuth, DefaultTimeout, query)
	objs := result.Get("Get", className).AsSlice()
	require.Len(t, objs, 2)
	for _, obj := range objs {
		name := obj.(map[string]any)["name"]
		assert.NotEmpty(t, name)
		additional, ok := obj.(map[string]any)["_additional"].(map[string]any)
		require.True(t, ok)
		require.NotNil(t, additional)
		id, ok := additional["id"].(string)
		require.True(t, ok)
		require.NotEmpty(t, id)
	}
}

func assertResultsGRPC(t *testing.T, host string, req *protocol.SearchRequest) {
	helper.SetupGRPCClient(t, host)
	client := helper.ClientGRPC(t)
	resp, err := client.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("search request failed: %v", err)
	}
	require.Len(t, resp.Results, 2)
	for _, res := range resp.Results {
		assert.NotEmpty(t, res.GetProperties().GetNonRefProps().GetFields()["name"].GetTextValue())
		assert.NotEmpty(t, res.GetMetadata().GetId())
	}
}

func TestSuite(t *testing.T, rest, grpc, className string,
	vectorizerSettings map[string]any,
) {
	tests := []struct {
		name     string
		useBatch bool
	}{
		{
			name: "with batch", useBatch: true,
		},
		{
			name: "without batch", useBatch: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := BaseClass(className)
			class.VectorConfig = map[string]models.VectorConfig{
				"description": {
					Vectorizer:      vectorizerSettings,
					VectorIndexType: "hnsw",
				},
			}
			helper.CreateClass(t, class)
			defer helper.DeleteClass(t, class.Class)
			if tt.useBatch {
				t.Run("batch create objects", func(t *testing.T) {
					BatchInsertObjects(t, rest, class.Class)
				})
			} else {
				t.Run("create objects", func(t *testing.T) {
					InsertObjects(t, rest, class.Class)
				})
			}
			t.Run("check objects existence", func(t *testing.T) {
				for _, company := range Companies {
					t.Run(company.ID.String(), func(t *testing.T) {
						obj, err := helper.GetObject(t, class.Class, company.ID, "vector")
						require.NoError(t, err)
						require.NotNil(t, obj)
						require.GreaterOrEqual(t, len(obj.Vectors), 1)
						checkVector(t, obj, "description")
					})
				}
			})
			t.Run("search tests", func(t *testing.T) {
				PerformAllSearchTests(t, rest, grpc, class.Class)
			})
		})
	}
}

func TestSuiteWithEmptyValues(t *testing.T, rest, grpc, className string,
	vectorizerSettings, emptyVectorizerSettings map[string]any,
) {
	tests := []struct {
		name     string
		useBatch bool
	}{
		{
			name: "with batch", useBatch: true,
		},
		{
			name: "without batch", useBatch: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := BaseClass(className)
			class.VectorConfig = map[string]models.VectorConfig{
				"description": {
					Vectorizer:      vectorizerSettings,
					VectorIndexType: "hnsw",
				},
				"empty": {
					Vectorizer:      emptyVectorizerSettings,
					VectorIndexType: "hnsw",
				},
			}
			helper.CreateClass(t, class)
			defer helper.DeleteClass(t, class.Class)
			if tt.useBatch {
				t.Run("batch create objects", func(t *testing.T) {
					BatchInsertObjectsWithEmpty(t, rest, class.Class)
				})
			} else {
				t.Run("create objects", func(t *testing.T) {
					InsertObjectsWithEmpty(t, rest, class.Class)
				})
			}
			t.Run("check objects existence", func(t *testing.T) {
				for _, company := range Companies {
					t.Run(company.ID.String(), func(t *testing.T) {
						obj, err := helper.GetObject(t, class.Class, company.ID, "vector")
						require.NoError(t, err)
						require.NotNil(t, obj)
						require.GreaterOrEqual(t, len(obj.Vectors), 1)
						checkVector(t, obj, "description")
						if obj.ID == SpaceX {
							checkVector(t, obj, "empty")
						} else {
							assert.Nil(t, obj.Vectors["empty"])
						}
					})
				}
			})
			t.Run("search tests", func(t *testing.T) {
				PerformAllSearchTestsWithTargetVector(t, rest, grpc, class.Class, "description")
			})
		})
	}
}

func checkVector(t *testing.T, obj *models.Object, targetVector string) {
	vector, ok := obj.Vectors[targetVector]
	require.True(t, ok)
	switch vector.(type) {
	case []float32:
		require.IsType(t, []float32{}, obj.Vectors[targetVector])
		assert.True(t, len(obj.Vectors[targetVector].([]float32)) > 0)
	case [][]float32:
		require.IsType(t, [][]float32{}, obj.Vectors[targetVector])
		assert.True(t, len(obj.Vectors[targetVector].([][]float32)) > 0)
	default:
		t.Fatalf("unsupported vector type: %T", vector)
	}
}
