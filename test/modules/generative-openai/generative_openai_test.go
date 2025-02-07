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

func testGenerativeOpenAI(rest, grpc string) func(t *testing.T) {
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
				name:            "gpt-3.5-turbo",
				generativeModel: "gpt-3.5-turbo",
			},
			{
				name:            "gpt-4",
				generativeModel: "gpt-4",
			},
			{
				name:               "absent module config",
				generativeModel:    "gpt-4",
				absentModuleConfig: true,
			},
			{
				name:            "gpt-4o-mini",
				generativeModel: "gpt-4o-mini",
				withImages:      true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.absentModuleConfig {
					t.Log("skipping adding module config configuration to class")
				} else {
					class.ModuleConfig = map[string]interface{}{
						"generative-openai": map[string]interface{}{
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
							assert.True(t, len(obj.Vectors["description"]) > 0)
						})
					}
				})
				// generative task
				t.Run("create a tweet", func(t *testing.T) {
					planets.CreateTweetTest(t, class.Class)
				})
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "openai:{temperature:0.1}"
					if tt.absentModuleConfig {
						params = fmt.Sprintf("openai:{temperature:0.1 model:\"%s\" baseURL:\"https://api.openai.com\" isAzure:false}", tt.generativeModel)
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
				t.Run("create a tweet using grpc", func(t *testing.T) {
					planets.CreateTweetTestGRPC(t, class.Class)
				})
				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					openaiParams := &pb.GenerativeOpenAI{
						MaxTokens:        grpchelper.ToPtr(int64(90)),
						Model:            tt.generativeModel,
						Temperature:      grpchelper.ToPtr(0.9),
						N:                grpchelper.ToPtr(int64(90)),
						TopP:             grpchelper.ToPtr(0.9),
						FrequencyPenalty: grpchelper.ToPtr(0.9),
						PresencePenalty:  grpchelper.ToPtr(0.9),
					}
					if tt.absentModuleConfig {
						openaiParams.BaseUrl = grpchelper.ToPtr("https://api.openai.com")
					}
					params := &pb.GenerativeProvider_Openai{
						Openai: openaiParams,
					}
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{ReturnMetadata: true, Kind: params})
				})
				if tt.withImages {
					t.Run("image prompt", func(t *testing.T) {
						t.Run("graphql", func(t *testing.T) {
							prompt := "Describe image"
							params := "openai:{images:\"image\"}"
							planets.CreatePromptTestWithParams(t, class.Class, prompt, params)
						})
						t.Run("grpc", func(t *testing.T) {
							prompt := "Give a short answer: What's on the image?"
							openaiParams := &pb.GenerativeOpenAI{
								MaxTokens:   grpchelper.ToPtr(int64(90)),
								Model:       tt.generativeModel,
								Temperature: grpchelper.ToPtr(0.9),
								N:           grpchelper.ToPtr(int64(90)),
								TopP:        grpchelper.ToPtr(0.9),
								Images:      &pb.TextArray{Values: []string{"image"}},
							}
							if tt.absentModuleConfig {
								openaiParams.BaseUrl = grpchelper.ToPtr("https://api.openai.com")
							}
							planets.CreatePromptTestWithParamsGRPC(t, class.Class, prompt, &pb.GenerativeProvider{
								ReturnMetadata: true,
								Kind:           &pb.GenerativeProvider_Openai{Openai: openaiParams},
							})
						})
					})
				}
			})
		}
	}
}
