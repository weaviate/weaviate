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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/build"
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

func TestRankUserAgent(t *testing.T) {
	tests := []struct {
		name         string
		weaviateUUID string
		expected     string
	}{
		{
			name:         "uuid set",
			weaviateUUID: "test-uuid",
			expected:     fmt.Sprintf("vector-db/weaviate/test-uuid %s", build.Version),
		},
		{
			name:     "uuid not set",
			expected: fmt.Sprintf("vector-db/weaviate/unknown %s", build.Version),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("VECTOR_DB_UUID", tt.weaviateUUID)

			var gotUserAgent string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotUserAgent = r.Header.Get("User-Agent")
				json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{{Index: 0, RelevanceScore: 1}}})
			}))
			defer server.Close()

			c := New("key", 0, nullLogger())
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}
			_, err := c.Rank(context.Background(), "q", []string{"doc"}, cfg)
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tt.expected), gotUserAgent)
		})
	}
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

func TestRankSendsNothingForNoDocuments(t *testing.T) {
	server, requests := countingServer(t, func(w http.ResponseWriter, r *http.Request) {})
	cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}

	res, err := New("key", 0, nullLogger()).Rank(context.Background(), "q", nil, cfg)

	require.NoError(t, err)
	assert.Equal(t, "q", res.Query)
	assert.Empty(t, res.DocumentScores)
	assert.Zero(t, requests.Load())
}

func TestRankRejectsMalformedResponses(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "more results than documents",
			body:    `{"results":[{"index":0,"relevance_score":1},{"index":1,"relevance_score":1},{"index":2,"relevance_score":1}]}`,
			wantErr: "DigitalOcean reranker returned 3 results for 2 documents",
		},
		{
			name:    "no results",
			body:    `{"results":[]}`,
			wantErr: "DigitalOcean reranker returned 0 results for 2 documents",
		},
		{
			name:    "results missing",
			body:    `{}`,
			wantErr: "DigitalOcean reranker returned 0 results for 2 documents",
		},
		{
			name:    "negative index",
			body:    `{"results":[{"index":0,"relevance_score":1},{"index":-1,"relevance_score":1}]}`,
			wantErr: "DigitalOcean reranker result index -1 out of range for 2 documents",
		},
		{
			name:    "body is not JSON",
			body:    `<html>gateway</html>`,
			wantErr: "failed to parse reranker response (status 200)",
		},
		{
			name:    "empty body",
			body:    ``,
			wantErr: "failed to parse reranker response (status 200)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(tt.body))
			})
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}

			res, err := New("key", 0, nullLogger()).Rank(context.Background(), "q", []string{"a", "b"}, cfg)

			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, res)
		})
	}
}

func TestRankChunkBoundaries(t *testing.T) {
	const maxDocuments = 3
	tests := []struct {
		documents    int
		wantRequests int64
	}{
		{documents: 1, wantRequests: 1},
		{documents: maxDocuments - 1, wantRequests: 1},
		{documents: maxDocuments, wantRequests: 1},
		{documents: maxDocuments + 1, wantRequests: 2},
		{documents: 2 * maxDocuments, wantRequests: 2},
		{documents: 2*maxDocuments + 1, wantRequests: 3},
		{documents: 20 * maxDocuments, wantRequests: 20},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d documents", tt.documents), func(t *testing.T) {
			server, requests := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
				var req rankRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Documents) > maxDocuments {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				// reverse order, as a relevance sort may return it; the score
				// is the number in the document
				results := make([]rankResult, len(req.Documents))
				for i, doc := range req.Documents {
					var score float64
					fmt.Sscanf(doc, "doc-%f", &score)
					results[len(req.Documents)-1-i] = rankResult{Index: i, RelevanceScore: score}
				}
				json.NewEncoder(w).Encode(rankResponse{Results: results})
			})
			documents := make([]string, tt.documents)
			for i := range documents {
				documents[i] = fmt.Sprintf("doc-%d", i)
			}
			c := New("key", 0, nullLogger())
			c.maxDocuments = maxDocuments
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}

			res, err := c.Rank(context.Background(), "q", documents, cfg)

			require.NoError(t, err)
			require.Len(t, res.DocumentScores, tt.documents)
			for i := range documents {
				assert.Equal(t, ent.DocumentScore{Document: documents[i], Score: float64(i)}, res.DocumentScores[i])
			}
			assert.Equal(t, tt.wantRequests, requests.Load())
		})
	}
}

func TestRankReturnsNoPartialResultWhenAChunkFails(t *testing.T) {
	tests := []struct {
		name string
		// failing is the document whose chunk the API refuses.
		failing string
	}{
		{name: "first chunk", failing: "a"},
		{name: "middle chunk", failing: "c"},
		{name: "last chunk", failing: "e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
				var req rankRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				results := make([]rankResult, len(req.Documents))
				for i, doc := range req.Documents {
					if doc == tt.failing {
						w.WriteHeader(http.StatusTooManyRequests)
						w.Write([]byte(`{"id":"too_many_requests","message":"slow down"}`))
						return
					}
					results[i] = rankResult{Index: i, RelevanceScore: 1}
				}
				json.NewEncoder(w).Encode(rankResponse{Results: results})
			})
			c := New("key", 0, nullLogger())
			c.maxDocuments = 2
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}

			res, err := c.Rank(context.Background(), "q", []string{"a", "b", "c", "d", "e"}, cfg)

			require.ErrorContains(t, err, "status: 429 error: slow down")
			assert.Nil(t, res)
		})
	}
}

func TestRankStopsOnACancelledContext(t *testing.T) {
	server, requests := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{{Index: 0, RelevanceScore: 1}}})
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}}

	res, err := New("key", 0, nullLogger()).Rank(ctx, "q", []string{"a"}, cfg)

	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, res)
	assert.Zero(t, requests.Load())
}

func TestRankKeepsTheBaseURLPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		fromHeader bool
		wantPath   string
	}{
		{name: "class base URL without a path", path: "", wantPath: "/v1/rerank"},
		{name: "class base URL with a trailing slash", path: "/", wantPath: "/v1/rerank"},
		{name: "class base URL with a gateway path", path: "/gateway", wantPath: "/gateway/v1/rerank"},
		{name: "class base URL with a gateway path and a trailing slash", path: "/gateway/", wantPath: "/gateway/v1/rerank"},
		{name: "header base URL with a gateway path", path: "/gateway", fromHeader: true, wantPath: "/gateway/v1/rerank"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath atomic.Value
			server, _ := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
				gotPath.Store(r.URL.Path)
				json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{{Index: 0, RelevanceScore: 1}}})
			})
			ctx := context.Background()
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL + tt.path}}
			if tt.fromHeader {
				ctx = context.WithValue(ctx, "X-Digitalocean-Api-Key", []string{"request-key"})
				ctx = context.WithValue(ctx, "X-Digitalocean-Baseurl", []string{server.URL + tt.path})
				cfg = fakeClassConfig{classConfig: map[string]interface{}{"baseURL": "https://unused.example.com"}}
			}

			_, err := New("key", 0, nullLogger()).Rank(ctx, "q", []string{"a"}, cfg)

			require.NoError(t, err)
			assert.Equal(t, tt.wantPath, gotPath.Load())
		})
	}
}

// The base URL of a query comes from the header or, without one, from the
// class config, which may predate MODULES_VALIDATE_BASE_URL. Whichever is
// used is validated before the API key and the documents are sent.
func TestRankValidatesTheBaseURLItUses(t *testing.T) {
	tests := []struct {
		name string
		// <fake> stands for the URL of the fake server.
		baseURL  string
		validate bool
		wantErr  string
	}{
		{name: "http", baseURL: "<fake>", validate: true, wantErr: "baseURL must use HTTPS"},
		{name: "http with a path", baseURL: "<fake>/v1", validate: true, wantErr: "baseURL must use HTTPS"},
		{name: "https to a loopback address", baseURL: "https://127.0.0.1:8443", validate: true, wantErr: "baseURL cannot target internal addresses"},
		{name: "https to a private address", baseURL: "https://10.0.0.1", validate: true, wantErr: "baseURL cannot target internal addresses"},
		{name: "https to the metadata address", baseURL: "https://169.254.169.254", validate: true, wantErr: "baseURL cannot target internal addresses"},
		{name: "https to localhost", baseURL: "https://localhost/v1", validate: true, wantErr: "baseURL cannot target internal addresses"},
		{name: "http while validation is off", baseURL: "<fake>"},
	}
	for _, source := range []string{"class config", "header", "header over a valid class config"} {
		for _, tt := range tests {
			t.Run(source+"/"+tt.name, func(t *testing.T) {
				if tt.validate {
					t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
				}
				server, requests := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(rankResponse{Results: []rankResult{{Index: 0, RelevanceScore: 1}}})
				})
				baseURL := strings.ReplaceAll(tt.baseURL, "<fake>", server.URL)
				ctx := context.Background()
				cfg := fakeClassConfig{classConfig: map[string]interface{}{"baseURL": baseURL}}
				switch source {
				case "header":
					ctx = context.WithValue(ctx, "X-Digitalocean-Api-Key", []string{"request-key"})
					ctx = context.WithValue(ctx, "X-Digitalocean-Baseurl", []string{baseURL})
					cfg = fakeClassConfig{classConfig: map[string]interface{}{}}
				case "header over a valid class config":
					ctx = context.WithValue(ctx, "X-Digitalocean-Api-Key", []string{"request-key"})
					ctx = context.WithValue(ctx, "X-Digitalocean-Baseurl", []string{baseURL})
					cfg = fakeClassConfig{classConfig: map[string]interface{}{"baseURL": "https://8.8.8.8"}}
				}

				res, err := New("key", 0, nullLogger()).Rank(ctx, "q", []string{"a"}, cfg)

				if tt.wantErr == "" {
					require.NoError(t, err)
					assert.EqualValues(t, 1, requests.Load())
					return
				}
				require.EqualError(t, err, tt.wantErr)
				assert.Nil(t, res)
				assert.Zero(t, requests.Load())
			})
		}
	}
}

// countingServer starts a fake API and counts the requests it receives.
func countingServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	requests := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return server, requests
}
