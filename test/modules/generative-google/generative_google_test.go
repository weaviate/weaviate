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
	"github.com/liutizhong/weaviate/entities/models"
	pb "github.com/liutizhong/weaviate/grpc/generated/protocol/v1"
	"github.com/liutizhong/weaviate/test/helper"
	grpchelper "github.com/liutizhong/weaviate/test/helper/grpc"
	"github.com/liutizhong/weaviate/test/helper/sample-schema/planets"
)

func testGenerativeGoogle(rest, grpc, gcpProject, generativeGoogle string) func(t *testing.T) {
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
					"text2vec-google": map[string]interface{}{
						"properties":         []interface{}{"description"},
						"vectorizeClassName": false,
						"projectId":          gcpProject,
						"modelId":            "textembedding-gecko@001",
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
				name:             "chat-bison",
				generativeModel:  "chat-bison",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:             "chat-bison-32k",
				generativeModel:  "chat-bison-32k",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:             "chat-bison@002",
				generativeModel:  "chat-bison@002",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:             "chat-bison-32k@002",
				generativeModel:  "chat-bison-32k@002",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:            "chat-bison@001",
				generativeModel: "chat-bison@001",
			},
			{
				name:             "gemini-1.5-pro-preview-0514",
				generativeModel:  "gemini-1.5-pro-preview-0514",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:             "gemini-1.5-pro-preview-0409",
				generativeModel:  "gemini-1.5-pro-preview-0409",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:             "gemini-1.5-flash-preview-0514",
				generativeModel:  "gemini-1.5-flash-preview-0514",
				frequencyPenalty: grpchelper.ToPtr(0.5),
				presencePenalty:  grpchelper.ToPtr(0.5),
			},
			{
				name:            "gemini-1.0-pro-002",
				generativeModel: "gemini-1.0-pro-002",
			},
			{
				name:            "gemini-1.0-pro-001",
				generativeModel: "gemini-1.0-pro-001",
			},
			{
				name:            "gemini-1.0-pro",
				generativeModel: "gemini-1.0-pro",
			},
			{
				name:               "absent module config",
				generativeModel:    "gemini-1.0-pro",
				absentModuleConfig: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.absentModuleConfig {
					t.Log("skipping adding module config configuration to class")
				} else {
					class.ModuleConfig = map[string]interface{}{
						generativeGoogle: map[string]interface{}{
							"projectId": gcpProject,
							"modelId":   tt.generativeModel,
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
					for _, company := range data {
						t.Run(company.ID.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, company.ID, "vector")
							require.NoError(t, err)
							require.NotNil(t, obj)
							require.Len(t, obj.Vectors, 1)
							assert.True(t, len(obj.Vectors["description"]) > 0)
						})
					}
				})
				// generative task
				if tt.absentModuleConfig {
					t.Log("skipping create tweet tests with default values as e2e tests rely on specific GCP settings")
				} else {
					t.Run("create a tweet", func(t *testing.T) {
						planets.CreateTweetTest(t, class.Class)
					})
					t.Run("create a tweet using grpc", func(t *testing.T) {
						planets.CreateTweetTestGRPC(t, class.Class)
					})
				}
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "google:{topP:0.1 topK:40}"
					if tt.absentModuleConfig {
						params = fmt.Sprintf("google:{topP:0.1 topK:40 projectId:\"%s\" model:\"%s\"}", gcpProject, tt.generativeModel)
					}
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
				t.Run("create a tweet with params using grpc", func(t *testing.T) {
					google := &pb.GenerativeGoogle{
						MaxTokens:        grpchelper.ToPtr(int64(256)),
						Model:            grpchelper.ToPtr(tt.generativeModel),
						Temperature:      grpchelper.ToPtr(0.5),
						TopK:             grpchelper.ToPtr(int64(40)),
						TopP:             grpchelper.ToPtr(0.1),
						FrequencyPenalty: tt.frequencyPenalty,
						PresencePenalty:  tt.presencePenalty,
					}
					if tt.absentModuleConfig {
						google.ProjectId = &gcpProject
					}
					planets.CreateTweetTestWithParamsGRPC(t, class.Class, &pb.GenerativeProvider{
						ReturnMetadata: true,
						Kind:           &pb.GenerativeProvider_Google{Google: google},
					})
				})
			})
		}
	}
}
