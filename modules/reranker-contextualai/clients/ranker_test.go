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

package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testQuery         = "test query"
	testAPIKey        = "test-key"
	rerankPath        = "/v1/rerank"
	contentTypeHeader = "Content-Type"
	applicationJSON   = "application/json"
	testModel         = "ctxl-rerank-v2-instruct-multilingual"
)

func TestClientRank(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, rerankPath, r.URL.Path)
		assert.Equal(t, applicationJSON, r.Header.Get(contentTypeHeader))
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer")

		var req RankInput
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		assert.Equal(t, testQuery, req.Query)
		assert.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
		assert.Equal(t, testModel, req.Model)

		response := RankResponse{
			Results: []RerankedResult{
				{Index: 0, RelevanceScore: 0.9},
				{Index: 2, RelevanceScore: 0.8},
				{Index: 1, RelevanceScore: 0.7},
			},
		}

		w.Header().Set(contentTypeHeader, applicationJSON)
		json.NewEncoder(w).Encode(response)
	}))
	defer testServer.Close()

	client := &client{
		apiKey:       testAPIKey,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		host:         testServer.URL,
		path:         rerankPath,
		maxDocuments: 1000,
		logger:       logrus.New(),
	}

	fakeConfig := fakeClassConfig{
		classConfig: map[string]interface{}{
			"model": testModel,
		},
	}

	result, err := client.Rank(context.Background(), testQuery, []string{"doc1", "doc2", "doc3"}, fakeConfig)

	require.NoError(t, err)
	assert.Equal(t, testQuery, result.Query)
	assert.Len(t, result.DocumentScores, 3)
	assert.Equal(t, "doc1", result.DocumentScores[0].Document)
	assert.Equal(t, 0.9, result.DocumentScores[0].Score)
}

func TestClientMetaInfo(t *testing.T) {
	client := &client{}

	meta, err := client.MetaInfo()
	require.NoError(t, err)

	assert.Equal(t, "Reranker - Contextual AI", meta["name"])
	assert.Equal(t, "https://docs.contextual.ai/api-reference/rerank/rerank", meta["documentationHref"])
}

func TestRankErrorHandling(t *testing.T) {
	tests := []struct {
		name         string
		statusCode   int
		responseBody map[string]interface{}
		expectedErr  string
	}{
		{
			name:       "error with message field",
			statusCode: 422,
			responseBody: map[string]interface{}{
				"message": "Invalid model specified",
			},
			expectedErr: "Invalid model specified",
		},
		{
			name:       "error with detail field",
			statusCode: 422,
			responseBody: map[string]interface{}{
				"detail": []map[string]interface{}{
					{
						"loc":  []string{"body", "model"},
						"msg":  "Validation failed",
						"type": "value_error",
					},
				},
			},
			expectedErr: "Validation failed",
		},
		{
			name:       "error without specific message",
			statusCode: 500,
			responseBody: map[string]interface{}{
				"error": "Internal server error",
			},
			expectedErr: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(contentTypeHeader, applicationJSON)
				w.WriteHeader(tt.statusCode)
				json.NewEncoder(w).Encode(tt.responseBody)
			}))
			defer testServer.Close()

			client := &client{
				apiKey:       testAPIKey,
				httpClient:   &http.Client{Timeout: 30 * time.Second},
				host:         testServer.URL,
				path:         rerankPath,
				maxDocuments: 1000,
				logger:       logrus.New(),
			}

			fakeConfig := fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": testModel,
				},
			}

			_, err := client.Rank(context.Background(), testQuery, []string{"doc1"}, fakeConfig)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestRankWithDifferentConfigs(t *testing.T) {
	tests := []struct {
		name          string
		config        map[string]interface{}
		expectedModel string
		expectedTopN  *int
		expectedInstr *string
	}{
		{
			name: "with default config",
			config: map[string]interface{}{
				"model": testModel,
			},
			expectedModel: testModel,
		},
		{
			name: "with topN",
			config: map[string]interface{}{
				"model": testModel,
				"topN":  5,
			},
			expectedModel: testModel,
			expectedTopN:  func() *int { v := 5; return &v }(),
		},
		{
			name: "with instruction",
			config: map[string]interface{}{
				"model":       testModel,
				"instruction": "Prioritize recent documents",
			},
			expectedModel: testModel,
			expectedInstr: func() *string { v := "Prioritize recent documents"; return &v }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedRequest RankInput

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				bodyBytes, _ := io.ReadAll(r.Body)
				json.Unmarshal(bodyBytes, &capturedRequest)

				response := RankResponse{
					Results: []RerankedResult{
						{Index: 0, RelevanceScore: 0.9},
					},
				}

				w.Header().Set(contentTypeHeader, applicationJSON)
				json.NewEncoder(w).Encode(response)
			}))
			defer testServer.Close()

			client := &client{
				apiKey:       testAPIKey,
				httpClient:   &http.Client{Timeout: 30 * time.Second},
				host:         testServer.URL,
				path:         rerankPath,
				maxDocuments: 1000,
				logger:       logrus.New(),
			}

			fakeConfig := fakeClassConfig{classConfig: tt.config}
			_, err := client.Rank(context.Background(), testQuery, []string{"doc1"}, fakeConfig)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedModel, capturedRequest.Model)

			if tt.expectedTopN != nil {
				require.NotNil(t, capturedRequest.TopN)
				assert.Equal(t, *tt.expectedTopN, *capturedRequest.TopN)
			}

			if tt.expectedInstr != nil {
				require.NotNil(t, capturedRequest.Instruction)
				assert.Equal(t, *tt.expectedInstr, *capturedRequest.Instruction)
			}
		})
	}
}
