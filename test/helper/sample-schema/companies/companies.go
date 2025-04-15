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

package companies

import (
	"context"
	"fmt"
	"testing"

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
			Properties: map[string]interface{}{
				"name":        company.Name,
				"description": company.Description,
			},
		}
		helper.SetupClient(host)
		helper.CreateObject(t, obj)
		helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
	}
}

func BatchInsertObjects(t *testing.T, host string, className string) {
	var objects []*models.Object
	for _, company := range Companies {
		objects = append(objects, &models.Object{
			Class: className,
			ID:    company.ID,
			Properties: map[string]interface{}{
				"name":        company.Name,
				"description": company.Description,
			},
		})
	}
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
	// vector search with gql
	t.Run("perform vector search with gql", func(t *testing.T) {
		PerformVectorSearchTest(t, rest, className)
	})
	// vector search with grpc
	t.Run("perform vector search with grpc", func(t *testing.T) {
		PerformVectorSearchGRPCTest(t, grpc, className)
	})
	// hybrid search with gql
	t.Run("perform hybrid search with gql", func(t *testing.T) {
		PerformHybridSearchTest(t, rest, className)
	})
	// hybrid search with grpc
	t.Run("perform hybrid search with grpc", func(t *testing.T) {
		PerformHybridSearchGRPCTest(t, grpc, className)
	})
}

func PerformVectorSearchTest(t *testing.T, host string, className string) {
	query := fmt.Sprintf(`
				{
					Get {
						%s(
							nearText:{
								concepts:["SpaceX"]
							}
						){
							name
							_additional {
								id
							}
						}
					}
				}
			`, className)
	assertResults(t, host, className, query)
}

func PerformVectorSearchGRPCTest(t *testing.T, host string, className string) {
	req := protocol.SearchRequest{
		Collection: className,
		NearText: &protocol.NearTextSearch{
			Query: []string{"SpaceX"},
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

func PerformHybridSearchWithTextTest(t *testing.T, host string, className, text string) {
	query := fmt.Sprintf(`
				{
					Get {
						%s(
							hybrid:{
								query:"%s"
								alpha:0.75
							}
						){
							name
							_additional {
								id
							}
						}
					}
				}
			`, className, text)
	assertResults(t, host, className, query)
}

func PerformHybridSearchTest(t *testing.T, host string, className string) {
	PerformHybridSearchWithTextTest(t, host, className, "SpaceX")
}

func PerformHybridSearchGRPCTest(t *testing.T, host string, className string) {
	req := protocol.SearchRequest{
		Collection: className,
		HybridSearch: &protocol.Hybrid{
			Query: "SpaceX",
			Alpha: 0.75,
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
	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	objs := result.Get("Get", className).AsSlice()
	require.Len(t, objs, 2)
	for _, obj := range objs {
		name := obj.(map[string]interface{})["name"]
		assert.NotEmpty(t, name)
		additional, ok := obj.(map[string]interface{})["_additional"].(map[string]interface{})
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
