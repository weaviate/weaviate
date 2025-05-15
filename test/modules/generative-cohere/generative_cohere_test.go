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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"
	"github.com/weaviate/weaviate/test/helper/sample-schema/planets"
)

func testGenerativeCohere(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		helper.SetupGRPCClient(t, grpc)
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
		}{
			{
				name:            "command-r-plus",
				generativeModel: "command-r-plus",
			},
			{
				name:            "command-r",
				generativeModel: "command-r",
			},
			{
				name:               "absent module config",
				generativeModel:    "command-r",
				absentModuleConfig: true,
			},
		}
		for idx, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.absentModuleConfig {
					t.Log("skipping adding module config configuration to class")
				} else {
					class.ModuleConfig = map[string]interface{}{
						"generative-cohere": map[string]interface{}{
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
				t.Run("create a tweet", func(t *testing.T) {
					planets.CreateTweetTest(t, class.Class)
				})
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "cohere:{temperature:0.9 k:400}"
					if tt.absentModuleConfig {
						params = fmt.Sprintf("cohere:{temperature:0.9 k:400 model:\"%s\" baseURL:\"https://api.cohere.ai\"}", tt.generativeModel)
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
				t.Run("create a tweet using grpc", func(t *testing.T) {
					planets.CreateTweetTestGRPC(t, class.Class)
				})
				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					cohere := &pb.GenerativeCohere{
						MaxTokens:        grpchelper.ToPtr(int64(90)),
						Model:            grpchelper.ToPtr(tt.generativeModel),
						Temperature:      grpchelper.ToPtr(0.9),
						K:                grpchelper.ToPtr(int64(90)),
						P:                grpchelper.ToPtr(0.9),
						StopSequences:    &pb.TextArray{Values: []string{"stop"}},
						FrequencyPenalty: grpchelper.ToPtr(0.9),
					}
					if tt.absentModuleConfig {
						cohere.BaseUrl = grpchelper.ToPtr("https://api.cohere.ai")
					}
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{
						ReturnMetadata: true,
						Kind:           &pb.GenerativeProvider_Cohere{Cohere: cohere},
					})
				})
			})
			if idx+1 < len(tests) {
				time.Sleep(60 * time.Second) // sleep to avoid rate limit on cohere api
			}
		}
	}
}
