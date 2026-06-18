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

func testGenerativeDeepseek(rest, grpc, generativeDeepseek string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		helper.SetupGRPCClient(t, grpc)
		// Data
		data := planets.Planets
		// Define class
		class := planets.BaseClass("PlanetsGenerativeTest")
		class.VectorConfig = map[string]models.VectorConfig{
			"description": {
				Vectorizer: map[string]any{
					"text2vec-transformers": map[string]any{
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
			frequencyPenalty   *float64
			presencePenalty    *float64
			absentModuleConfig bool
		}{
			{
				name:             "deepseek-v4-flash",
				generativeModel:  "deepseek-v4-flash",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:               "absent module config",
				generativeModel:    "deepseek-v4-flash",
				absentModuleConfig: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.absentModuleConfig {
					t.Log("skipping adding module config configuration to class")
				} else {
					class.ModuleConfig = map[string]any{
						generativeDeepseek: map[string]any{
							"model": tt.generativeModel,
						},
					}
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
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
				// generative task
				if tt.absentModuleConfig {
					t.Log("skipping create tweet tests with default values as e2e tests rely on specific module config settings")
				} else {
					t.Run("create a tweet", func(t *testing.T) {
						planets.CreateTweetTest(t, class.Class)
					})
					t.Run("create a tweet using grpc", func(t *testing.T) {
						planets.CreateTweetTestGRPC(t, class.Class)
					})
				}
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "deepseek:{temperature:0.5 topP:0.9}"
					if tt.absentModuleConfig {
						params = fmt.Sprintf("deepseek:{temperature:0.5 topP:0.9 model:%q}", tt.generativeModel)
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})

				params := func() *pb.GenerativeDeepseek {
					return &pb.GenerativeDeepseek{
						MaxTokens:        grpchelper.ToPtr(int64(256)),
						Model:            grpchelper.ToPtr(tt.generativeModel),
						Temperature:      grpchelper.ToPtr(0.5),
						TopP:             grpchelper.ToPtr(0.9),
						FrequencyPenalty: tt.frequencyPenalty,
						PresencePenalty:  tt.presencePenalty,
					}
				}

				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{
						ReturnMetadata: true,
						Kind:           &pb.GenerativeProvider_Deepseek{Deepseek: params()},
					})
				})
			})
		}
	}
}
