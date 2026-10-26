//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

const baseURL = "https://api.meta.ai"

func testGenerativeMeta(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		helper.SetupGRPCClient(t, grpc)
		dataFolderPath := "../../../test/helper/sample-schema/planets/data"
		data := planets.Planets
		class := planets.BaseClass("PlanetsGenerativeTest")
		class.VectorConfig = map[string]models.VectorConfig{
			"description": {
				Vectorizer: map[string]any{
					"text2vec-model2vec": map[string]any{
						"properties":         []any{"description"},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
		}
		tests := []struct {
			name               string
			generativeModel    string
			withImages         bool
			absentModuleConfig bool
		}{
			{
				name:            "muse-spark-1.2",
				generativeModel: "muse-spark-1.2",
				withImages:      true,
			},
			{
				name:            "muse-spark-1.1",
				generativeModel: "muse-spark-1.1",
			},
			{
				name:               "absent module config",
				generativeModel:    "muse-spark-1.2",
				absentModuleConfig: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.absentModuleConfig {
					t.Log("skipping adding module config configuration to class")
					class.ModuleConfig = nil
				} else {
					class.ModuleConfig = map[string]any{
						"generative-meta": map[string]any{
							"model": tt.generativeModel,
						},
					}
				}
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
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
				if !tt.absentModuleConfig {
					t.Run("create a tweet", func(t *testing.T) {
						planets.CreateTweetTest(t, class.Class)
					})
				}
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "meta:{temperature:0.1 topP:0.9 reasoningEffort:low}"
					if tt.absentModuleConfig {
						params = fmt.Sprintf("meta:{temperature:0.1 topP:0.9 reasoningEffort:low model:%q baseURL:%q}", tt.generativeModel, baseURL)
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
				if !tt.absentModuleConfig {
					t.Run("create a tweet using grpc", func(t *testing.T) {
						planets.CreateTweetTestGRPC(t, class.Class)
					})
				}

				params := func() *pb.GenerativeMeta {
					params := &pb.GenerativeMeta{
						Model:            grpchelper.ToPtr(tt.generativeModel),
						MaxTokens:        grpchelper.ToPtr(int64(2000)),
						Temperature:      grpchelper.ToPtr(0.9),
						TopP:             grpchelper.ToPtr(0.9),
						FrequencyPenalty: grpchelper.ToPtr(0.1),
						PresencePenalty:  grpchelper.ToPtr(0.1),
						ReasoningEffort:  pb.GenerativeMeta_REASONING_EFFORT_LOW.Enum(),
					}
					if tt.absentModuleConfig {
						params.BaseUrl = grpchelper.ToPtr(baseURL)
					}
					return params
				}
				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{
						ReturnMetadata: true,
						Kind:           &pb.GenerativeProvider_Meta{Meta: params()},
					})
				})

				if tt.withImages {
					t.Run("image prompt", func(t *testing.T) {
						t.Run("graphql", func(t *testing.T) {
							planets.CreatePromptTestWithParams(t, class.Class, "Describe image", "meta:{imageProperties:\"image\"}")
						})

						singlePrompt := "Give a short answer: What's on the image?"
						groupPrompt := "Give a short answer: What are on the following images?"

						t.Run("grpc server stored images", func(t *testing.T) {
							params := params()
							params.ImageProperties = &pb.TextArray{Values: []string{"image"}}
							planets.CreatePromptTestWithParamsGRPC(t, class.Class, singlePrompt, groupPrompt, &pb.GenerativeProvider{
								ReturnMetadata: true,
								Kind:           &pb.GenerativeProvider_Meta{Meta: params},
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
								Kind:           &pb.GenerativeProvider_Meta{Meta: params},
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
								Kind:           &pb.GenerativeProvider_Meta{Meta: params},
							})
						})

						// "name" is a text property, so it contributes no image.
						t.Run("grpc image property that holds no image", func(t *testing.T) {
							params := params()
							params.ImageProperties = &pb.TextArray{Values: []string{"image", "name"}}
							planets.CreatePromptTestWithParamsGRPC(t, class.Class, singlePrompt, groupPrompt, &pb.GenerativeProvider{
								ReturnMetadata: true,
								Kind:           &pb.GenerativeProvider_Meta{Meta: params},
							})
						})
					})
				}
			})
		}
	}
}
