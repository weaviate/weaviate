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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestClient_Rank(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/rerank", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer")

		var req RankInput
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		assert.Equal(t, "test query", req.Query)
		assert.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
		assert.Equal(t, "ctxl-rerank-v2-instruct-multilingual", req.Model)

		response := RankResponse{
			Results: []RerankedResult{
				{Index: 0, RelevanceScore: 0.9},
				{Index: 2, RelevanceScore: 0.8},
				{Index: 1, RelevanceScore: 0.7},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer testServer.Close()

	client := &client{
		apiKey:       "test-key",
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		host:         testServer.URL,
		path:         "/v1/rerank",
		maxDocuments: 1000,
		logger:       logrus.New(),
	}

	mockConfig := &mockClassConfig{
		classConfig: map[string]interface{}{
			"model": "ctxl-rerank-v2-instruct-multilingual",
		},
	}

	result, err := client.Rank(context.Background(), "test query", []string{"doc1", "doc2", "doc3"}, mockConfig)

	require.NoError(t, err)
	assert.Equal(t, "test query", result.Query)
	assert.Len(t, result.DocumentScores, 3)
	assert.Equal(t, "doc1", result.DocumentScores[0].Document)
	assert.Equal(t, 0.9, result.DocumentScores[0].Score)
}

func TestClient_MetaInfo(t *testing.T) {
	client := &client{}

	meta, err := client.MetaInfo()
	require.NoError(t, err)

	assert.Equal(t, "Reranker - Contextual AI", meta["name"])
	assert.Equal(t, "https://contextual.ai/rerank/", meta["documentationHref"])
}

// mockClassConfig implements moduletools.ClassConfig for testing
type mockClassConfig struct {
	classConfig map[string]interface{}
}

func (m *mockClassConfig) Class() map[string]interface{} {
	return m.classConfig
}

func (m *mockClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return m.classConfig
}

func (m *mockClassConfig) Property(propName string) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *mockClassConfig) Tenant() string {
	return ""
}

func (m *mockClassConfig) TargetVector() string {
	return ""
}

func (m *mockClassConfig) Config() *config.Config {
	return nil
}

func (m *mockClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
