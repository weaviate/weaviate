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

package planets

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"
)

var Planets = []struct {
	ID                strfmt.UUID
	Name, Description string
}{
	{
		ID:   strfmt.UUID("00000000-0000-0000-0000-000000000001"),
		Name: "Earth",
		Description: `
		The Earth's surface is predominantly covered by oceans, accounting for about 71% of its total area, while continents provide 
		the stage for bustling cities, towering mountains, and sprawling forests. Its atmosphere, composed mostly of nitrogen and oxygen, 
		protects life from harmful solar radiation and regulates the planet's climate, creating the conditions necessary for life to flourish.

		Humans, as the dominant species, have left an indelible mark on Earth, shaping its landscapes and ecosystems in profound ways. 
		However, with this influence comes the responsibility to steward and preserve our planet for future generations.
		`,
	},
	{
		ID:   strfmt.UUID("00000000-0000-0000-0000-000000000002"),
		Name: "Mars",
		Description: `
		Mars, often called the "Red Planet" due to its rusty reddish hue, is the fourth planet from the Sun in our solar system. 
		It's a world of stark contrasts and mysterious allure, captivating the imaginations of scientists, explorers, and dreamers alike.

		With its barren, rocky terrain and thin atmosphere primarily composed of carbon dioxide, Mars presents a harsh environment vastly 
		different from Earth. Yet, beneath its desolate surface lie tantalizing clues about its past, including evidence of ancient rivers, 
		lakes, and even the possibility of microbial life.
		`,
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
			{
				Name: "image", DataType: []string{schema.DataTypeBlob.String()},
			},
		},
	}
}

func InsertObjects(t *testing.T, className string) {
	InsertObjectsWithImages(t, className, "")
}

func InsertObjectsWithImages(t *testing.T, className, dataFolderPath string) {
	getProperties := func(t *testing.T, name, description, dataFolderPath string) map[string]interface{} {
		properties := map[string]interface{}{
			"name":        name,
			"description": description,
		}
		if dataFolderPath != "" {
			imageBase64, err := GetImageBlob(dataFolderPath, strings.ToLower(name))
			require.NoError(t, err)
			properties["image"] = imageBase64
		}
		return properties
	}
	for _, planet := range Planets {
		obj := &models.Object{
			Class:      className,
			ID:         planet.ID,
			Properties: getProperties(t, planet.Name, planet.Description, dataFolderPath),
		}
		helper.CreateObject(t, obj)
		helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
	}
}

func CreateTweetTest(t *testing.T, className string) {
	CreateTweetTestWithParams(t, className, "")
}

func CreateTweetTestWithParams(t *testing.T, className, params string) {
	CreatePromptTestWithParams(t, className, "Write a short tweet about planet {name}", params)
}

func CreatePromptTestWithParams(t *testing.T, className, prompt, params string) {
	query := fmt.Sprintf(`
			{
				Get {
					%s{
						name
						_additional {
							generate(
								singleResult: {
									prompt: """
										%s
									"""
									%s
								}
							) {
								singleResult
								error
							}
						}
					}
				}
			}
		`, className, prompt, params)
	result := graphqlhelper.AssertGraphQLWithTimeout(t, helper.RootAuth, 10*time.Minute, query)
	objs := result.Get("Get", className).AsSlice()
	require.Len(t, objs, 2)
	for i, obj := range objs {
		name := obj.(map[string]interface{})["name"]
		assert.NotEmpty(t, name)
		additional, ok := obj.(map[string]interface{})["_additional"].(map[string]interface{})
		require.True(t, ok)
		require.NotNil(t, additional)
		generate, ok := additional["generate"].(map[string]interface{})
		require.True(t, ok)
		require.NotNil(t, generate)
		require.Nil(t, generate["error"])
		require.NotNil(t, generate["singleResult"])
		singleResult, ok := generate["singleResult"].(string)
		require.True(t, ok)
		require.NotEmpty(t, singleResult)
		// print the results of the prompt
		t.Logf("[%v]Prompt: %s\nResult: %s\n", i, prompt, singleResult)
	}
}

func CreateTweetTestGRPC(t *testing.T, className string) {
	CreateTweetTestWithParamsGRPC(t, className, nil)
}

func CreateTweetTestWithParamsGRPC(t *testing.T, className string, params *pb.GenerativeProvider) {
	CreatePromptTestWithParamsGRPC(t, className, "Write a short tweet about planet {name}", "Write a short tweet about the following planets", params)
}

func CreatePromptTestWithParamsGRPC(t *testing.T, className, singlePrompt, groupPrompt string, params *pb.GenerativeProvider) {
	var queries []*pb.GenerativeProvider
	if params != nil {
		queries = []*pb.GenerativeProvider{params}
	}
	req := &pb.SearchRequest{
		Collection: className,
		Limit:      2,
		Generative: &pb.GenerativeSearch{
			Single: &pb.GenerativeSearch_Single{
				Prompt:  singlePrompt,
				Queries: queries,
			},
			Grouped: &pb.GenerativeSearch_Grouped{
				Task:       groupPrompt,
				Properties: &pb.TextArray{Values: []string{"name"}},
				Queries:    queries,
			},
		},
		Uses_127Api: true,
	}
	resp := grpchelper.AssertSearchWithTimeout(t, req, 10*time.Minute)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 2)
	for i, res := range resp.Results {
		assertGenerative(t, res.Generative, params)
		t.Logf("[%v]Single Prompt: %s\nResult: %s\n", i, singlePrompt, res.Generative.GetValues()[0].Result)
	}
	assertGenerative(t, resp.GenerativeGroupedResults, params)
	t.Logf("Grouped Prompt: %s\nResult: %s\n", groupPrompt, resp.GenerativeGroupedResults.GetValues()[0].Result)
}

func assertGenerative(t *testing.T, generative *pb.GenerativeResult, params *pb.GenerativeProvider) {
	require.Len(t, generative.GetValues(), 1)
	require.NotEmpty(t, generative.GetValues()[0].Result)
	if params.GetReturnMetadata() {
		metadata := generative.GetValues()[0].GetMetadata()
		require.NotEmpty(t, metadata)
		if params.GetAnthropic() != nil {
			anthropic := metadata.GetAnthropic()
			require.NotEmpty(t, anthropic)
			usage := anthropic.GetUsage()
			require.NotEmpty(t, usage)
			require.NotEmpty(t, usage.GetInputTokens())
			require.NotEmpty(t, usage.GetOutputTokens())
		}
		if params.GetCohere() != nil {
			require.NotEmpty(t, metadata.GetCohere())
		}
		if params.GetMistral() != nil {
			require.NotEmpty(t, metadata.GetMistral())
		}
		if params.GetOpenai() != nil {
			require.NotEmpty(t, metadata.GetOpenai())
		}
		if params.GetGoogle() != nil {
			require.NotEmpty(t, metadata.GetGoogle())
		}
	}
}

func GetImageBlob(dataFolderPath, name string) (string, error) {
	path := fmt.Sprintf("%s/images/%s.jpg", dataFolderPath, name)
	return helper.GetBase64EncodedData(path)
}
