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

const baseURL = "https://inference.do-ai.run"

func testGenerativeDigitalOcean(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		helper.SetupGRPCClient(t, grpc)
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
			absentModuleConfig bool
		}{
			{
				name:            "llama-4-maverick",
				generativeModel: "llama-4-maverick",
			},
			{
				name:            "deepseek-4-flash",
				generativeModel: "deepseek-4-flash",
			},
			{
				name:               "absent module config",
				generativeModel:    "llama-4-maverick",
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
						"generative-digitalocean": map[string]any{
							"model": tt.generativeModel,
						},
					}
				}
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				t.Run("create objects", func(t *testing.T) {
					planets.InsertObjects(t, class.Class)
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
					params := "digitalocean:{temperature:0.1}"
					if tt.absentModuleConfig {
						params = fmt.Sprintf("digitalocean:{temperature:0.1 model:\"%s\" baseURL:\"%s\"}", tt.generativeModel, baseURL)
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
				if !tt.absentModuleConfig {
					t.Run("create a tweet using grpc", func(t *testing.T) {
						planets.CreateTweetTestGRPC(t, class.Class)
					})
				}
				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					params := &pb.GenerativeDigitalOcean{
						Model:            grpchelper.ToPtr(tt.generativeModel),
						MaxTokens:        grpchelper.ToPtr(int64(90)),
						Temperature:      grpchelper.ToPtr(0.9),
						TopP:             grpchelper.ToPtr(0.9),
						FrequencyPenalty: grpchelper.ToPtr(0.1),
						PresencePenalty:  grpchelper.ToPtr(0.1),
					}
					if tt.absentModuleConfig {
						params.BaseUrl = grpchelper.ToPtr(baseURL)
					}
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{
						ReturnMetadata: true,
						Kind:           &pb.GenerativeProvider_Digitalocean{Digitalocean: params},
					})
				})
			})
		}
	}
}
