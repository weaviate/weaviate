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

package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

func TestRank(t *testing.T) {
	t.Run("sends expected request and maps sorted results back to input order", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/v1/rerank", r.URL.Path)
			assert.Equal(t, "Bearer key", r.Header.Get("Authorization"))

			var req rankRequest
			bodyBytes, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(bodyBytes, &req))
			assert.Equal(t, "bge-reranker-v2-m3", req.Model)
			assert.Equal(t, "Where do I work?", req.Query)
			assert.Equal(t, []string{"I like pizza", "I work at Apple"}, req.Documents)

			// sorted by relevance, as the API does; scores may be negative
			json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{
				{Index: 1, RelevanceScore: 0.92},
				{Index: 0, RelevanceScore: -3.5},
			}})
		}))
		defer server.Close()
		c := New("key", 0, nullLogger())

		cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}
		res, err := c.Rank(context.Background(), "Where do I work?", []string{"I like pizza", "I work at Apple"}, cfg)

		require.NoError(t, err)
		assert.Equal(t, ent.RankResult{
			Query: "Where do I work?",
			DocumentScores: []ent.DocumentScore{
				{Document: "I like pizza", Score: -3.5},
				{Document: "I work at Apple", Score: 0.92},
			},
		}, *res)
	})

	t.Run("uses model from class config", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req rankRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "custom-model", req.Model)
			json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{{Index: 0, RelevanceScore: 1}}})
		}))
		defer server.Close()
		c := New("key", 0, nullLogger())

		cfg := fakeClassConfig{classConfig: map[string]interface{}{"model": "custom-model", "baseURL": server.URL}}
		_, err := c.Rank(context.Background(), "q", []string{"doc"}, cfg)
		require.NoError(t, err)
	})

	t.Run("api key and base url from headers take precedence", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer header-key", r.Header.Get("Authorization"))
			json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{{Index: 0, RelevanceScore: 1}}})
		}))
		defer server.Close()
		c := New("env-key", 0, nullLogger())

		ctx := context.WithValue(context.Background(), "X-Digitalocean-Api-Key", []string{"header-key"})
		ctx = context.WithValue(ctx, "X-Digitalocean-Baseurl", []string{server.URL})
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": "https://unused.example.com"}}
		_, err := c.Rank(ctx, "q", []string{"doc"}, cfg)
		require.NoError(t, err)
	})

	t.Run("missing api key", func(t *testing.T) {
		c := New("", 0, nullLogger())
		cfg := fakeClassConfig{classConfig: map[string]interface{}{}}
		_, err := c.Rank(context.Background(), "q", []string{"doc"}, cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "X-Digitalocean-Api-Key")
	})

	errorTests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{
			name:    "openai style error body",
			status:  http.StatusBadRequest,
			body:    `{"error":{"message":"model not found","type":"invalid_request_error"}}`,
			wantErr: "status: 400 request-id: req-1 error: model not found",
		},
		{
			name:    "digitalocean style error body",
			status:  http.StatusUnauthorized,
			body:    `{"id":"unauthorized","message":"Unable to authenticate you"}`,
			wantErr: "status: 401 request-id: req-1 error: Unable to authenticate you",
		},
		{
			name:    "unparsable error body",
			status:  http.StatusInternalServerError,
			body:    `oops`,
			wantErr: "status: 500 request-id: req-1",
		},
	}
	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("x-request-id", "req-1")
				w.WriteHeader(tt.status)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()
			c := New("key", 0, nullLogger())

			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}
			_, err := c.Rank(context.Background(), "q", []string{"doc"}, cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	invalidResults := []struct {
		name    string
		results []rankResult
		wantErr string
	}{
		{name: "fewer results than documents", results: []rankResult{{Index: 0}}, wantErr: "returned 1 results for 2 documents"},
		{name: "index out of range", results: []rankResult{{Index: 0}, {Index: 2}}, wantErr: "index 2 out of range"},
		{name: "duplicate index", results: []rankResult{{Index: 1}, {Index: 1}}, wantErr: "duplicate index 1"},
	}
	for _, tt := range invalidResults {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(rankResponse{Results: tt.results})
			}))
			defer server.Close()
			c := New("key", 0, nullLogger())

			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}
			_, err := c.Rank(context.Background(), "q", []string{"a", "b"}, cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	t.Run("sends requests in chunks and preserves document order", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req rankRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.LessOrEqual(t, len(req.Documents), 2)
			// score each document by its text so order can be verified
			// regardless of which chunk it arrived in
			results := make([]rankResult, len(req.Documents))
			for i, doc := range req.Documents {
				results[len(req.Documents)-1-i] = rankResult{Index: i, RelevanceScore: float64(len(doc))}
			}
			json.NewEncoder(w).Encode(rankResponse{Results: results})
		}))
		defer server.Close()

		c := New("key", 0, nullLogger())
		c.maxDocuments = 2

		documents := []string{"a", "bb", "ccc", "dddd", "eeeee", "ffffff", "ggggggg"}
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}
		resp, err := c.Rank(context.Background(), "q", documents, cfg)

		require.NoError(t, err)
		require.Len(t, resp.DocumentScores, len(documents))
		for i := range documents {
			assert.Equal(t, documents[i], resp.DocumentScores[i].Document)
			assert.Equal(t, float64(len(documents[i])), resp.DocumentScores[i].Score)
		}
	})
}

type fakeClassConfig struct {
	classConfig map[string]interface{}
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
