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

package tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"
	"github.com/weaviate/weaviate/test/helper/sample-schema/planets"
)

func testGenerativeAnthropic(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		helper.SetupGRPCClient(t, grpc)
		// Define path to test/helper/sample-schema/planets/data folder
		dataFolderPath := "../../../test/helper/sample-schema/planets/data"
		// Data
		data := planets.Planets
		// Define class
		class := planets.BaseClass("PlanetsGenerativeTest")
		class.VectorConfig = map[string]models.VectorConfig{
			"description": {
				Vectorizer: map[string]interface{}{
					"text2vec-transformers": map[string]interface{}{
						"properties":         []interface{}{"description"},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
		}
		tests := []struct {
			name               string
			generativeModel    string
			absentModuleConfig bool
			withImages         bool
		}{
			{
				name:            "claude-3-5-sonnet-20240620",
				generativeModel: "claude-3-5-sonnet-20240620",
			},
			{
				name:            "claude-3-opus-20240229",
				generativeModel: "claude-3-opus-20240229",
			},
			{
				name:            "claude-3-sonnet-20240229",
				generativeModel: "claude-3-sonnet-20240229",
			},
			{
				name:            "claude-3-haiku-20240307",
				generativeModel: "claude-3-haiku-20240307",
			},
			{
				name:               "absent module config",
				generativeModel:    "claude-3-5-sonnet-20240620",
				absentModuleConfig: true,
			},
			{
				name:            "claude-3-5-sonnet-20241022",
				generativeModel: "claude-3-5-sonnet-20241022",
				withImages:      true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.absentModuleConfig {
					t.Log("skipping adding module config configuration to class")
				} else {
					class.ModuleConfig = map[string]interface{}{
						"generative-anthropic": map[string]interface{}{
							"model": tt.generativeModel,
						},
					}
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					if tt.withImages {
						planets.InsertObjectsWithImages(t, class.Class, dataFolderPath)
					} else {
						planets.InsertObjects(t, class.Class)
					}
				})
				t.Run("check objects existence", func(t *testing.T) {
					for _, planet := range data {
						t.Run(planet.ID.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, planet.ID, "vector")
							require.NoError(t, err)
							require.NotNil(t, obj)
							require.Len(t, obj.Vectors, 1)
							require.IsType(t, []float32{}, obj.Vectors["description"])
							assert.True(t, len(obj.Vectors["description"].([]float32)) > 0)
						})
					}
				})
				// generative task
				t.Run("create a tweet", func(t *testing.T) {
					planets.CreateTweetTest(t, class.Class)
				})
				t.Run("create a tweet with params", func(t *testing.T) {
					params := fmt.Sprintf("anthropic:{topP:0.9 topK:90 model:%q}", tt.generativeModel)
					if tt.absentModuleConfig {
						params = fmt.Sprintf("anthropic:{topP:0.9 topK:90 model:%q baseURL:\"%s\"}", tt.generativeModel, "https://api.anthropic.com")
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
				t.Run("create a tweet using grpc", func(t *testing.T) {
					planets.CreateTweetTestGRPC(t, class.Class)
				})

				params := func() *pb.GenerativeAnthropic {
					params := &pb.GenerativeAnthropic{
						MaxTokens:     grpchelper.ToPtr(int64(90)),
						Model:         grpchelper.ToPtr(tt.generativeModel),
						Temperature:   grpchelper.ToPtr(0.9),
						TopK:          grpchelper.ToPtr(int64(90)),
						TopP:          grpchelper.ToPtr(0.9),
						StopSequences: &pb.TextArray{Values: []string{"stop"}},
					}
					if tt.absentModuleConfig {
						params.BaseUrl = grpchelper.ToPtr("https://api.anthropic.com")
					}
					return params
				}

				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{
						ReturnMetadata: true,
						Kind:           &pb.GenerativeProvider_Anthropic{Anthropic: params()},
					})
				})
				if tt.withImages {
					t.Run("image prompt", func(t *testing.T) {
						t.Run("graphql", func(t *testing.T) {
							prompt := "Describe image"
							params := "anthropic:{imageProperties:\"image\"}"
							planets.CreatePromptTestWithParams(t, class.Class, prompt, params)
						})

						singlePrompt := "Give a short answer: What's on the image?"
						groupPrompt := "Give a short answer: What are on the following images?"

						t.Run("grpc server stored images", func(t *testing.T) {
							params := params()
							params.ImageProperties = &pb.TextArray{Values: []string{"image"}}
							planets.CreatePromptTestWithParamsGRPC(t, class.Class, singlePrompt, groupPrompt, &pb.GenerativeProvider{
								ReturnMetadata: true,
								Kind:           &pb.GenerativeProvider_Anthropic{Anthropic: params},
							})
						})

						t.Run("grpc user provided images", func(t *testing.T) {
							earth, err := planets.GetImageBlob(dataFolderPath, "earth")
							require.NoError(t, err)
							mars, err := planets.GetImageBlob(dataFolderPath, "mars")
							require.NoError(t, err)

							params := params()
							params.Images = &pb.TextArray{Values: []string{earth, mars}}
							planets.CreatePromptTestWithParamsGRPC(t, class.Class, singlePrompt, groupPrompt, &pb.GenerativeProvider{
								ReturnMetadata: true,
								Kind:           &pb.GenerativeProvider_Anthropic{Anthropic: params},
							})
						})

						t.Run("grpc mixed images", func(t *testing.T) {
							earth, err := planets.GetImageBlob(dataFolderPath, "earth")
							require.NoError(t, err)
							mars, err := planets.GetImageBlob(dataFolderPath, "mars")
							require.NoError(t, err)

							params := params()
							params.Images = &pb.TextArray{Values: []string{earth, mars}}
							params.ImageProperties = &pb.TextArray{Values: []string{"image"}}
							planets.CreatePromptTestWithParamsGRPC(t, class.Class, singlePrompt, groupPrompt, &pb.GenerativeProvider{
								ReturnMetadata: true,
								Kind:           &pb.GenerativeProvider_Anthropic{Anthropic: params},
							})
						})
					})
				}
			})
		}
	}
}
