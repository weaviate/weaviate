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

package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

type fakeClassConfig struct {
	cfg map[string]any
}

func (f fakeClassConfig) Class() map[string]any { return f.cfg }
func (f fakeClassConfig) ClassByModuleName(string) map[string]any {
	return f.cfg
}
func (f fakeClassConfig) Property(string) map[string]any { return nil }
func (f fakeClassConfig) Tenant() string                 { return "" }
func (f fakeClassConfig) TargetVector() string           { return "" }
func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
func (f fakeClassConfig) Config() *config.Config { return nil }

func TestClient_Vectorize_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/embeddings", r.URL.Path)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))

		var body embeddingsRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "qwen3-embedding-0.6b", body.Model)
		assert.Equal(t, []string{"hello"}, body.Input)
		assert.Equal(t, "float", body.EncodingFormat)

		w.Header().Set("ratelimit-limit", "250")
		w.Header().Set("ratelimit-remaining", "249")
		w.Header().Set("ratelimit-reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"model": "qwen3-embedding-0.6b",
			"data": [
				{"object": "embedding", "index": 0, "embedding": [0.1, 0.2, 0.3]}
			],
			"usage": {"prompt_tokens": 1, "total_tokens": 1}
		}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": server.URL,
	}}

	res, rl, totalTokens, err := c.Vectorize(context.Background(), []string{"hello"}, cfg)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 3, res.Dimensions)
	assert.Equal(t, []float32{0.1, 0.2, 0.3}, res.Vector[0])
	assert.Equal(t, []string{"hello"}, res.Text, "Text must echo the original inputs, not the API's data[].object field")
	assert.Equal(t, 1, totalTokens)

	require.NotNil(t, rl)
	assert.Equal(t, 250, rl.LimitRequests)
	assert.Equal(t, 249, rl.RemainingRequests)
}

func TestClient_Vectorize_ErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-request-id", "req-123")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error": {"message": "model not found", "type": "not_found"}}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "bogus",
		"baseURL": server.URL,
	}}

	_, _, _, err := c.Vectorize(context.Background(), []string{"hello"}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status: 400")
	assert.Contains(t, err.Error(), "req-123")
	assert.Contains(t, err.Error(), "model not found")
}

func TestClient_Vectorize_MissingApiKey(t *testing.T) {
	c := New("", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": "https://example.com",
	}}
	_, _, _, err := c.Vectorize(context.Background(), []string{"hello"}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no api key found")
}

func TestClient_Vectorize_OutOfOrderResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"data": [
				{"object": "embedding", "index": 1, "embedding": [0.2]},
				{"object": "embedding", "index": 0, "embedding": [0.1]}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": server.URL,
	}}

	res, _, _, err := c.Vectorize(context.Background(), []string{"first", "second"}, cfg)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []string{"first", "second"}, res.Text)
	assert.Equal(t, []float32{0.1}, res.Vector[0], "embeddings must be reordered by index to match the inputs")
	assert.Equal(t, []float32{0.2}, res.Vector[1])
}

func TestClient_Vectorize_IndexOutOfRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"data": [
				{"object": "embedding", "index": 5, "embedding": [0.1]}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": server.URL,
	}}

	_, _, _, err := c.Vectorize(context.Background(), []string{"only"}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestClient_Vectorize_PartialResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"data": [
				{"object": "embedding", "index": 0, "embedding": [0.1]}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": server.URL,
	}}

	_, _, _, err := c.Vectorize(context.Background(), []string{"first", "second"}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing embedding for input index 1")
}

func TestClient_Vectorize_DuplicateIndex(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"data": [
				{"object": "embedding", "index": 0, "embedding": [0.1]},
				{"object": "embedding", "index": 0, "embedding": [0.2]}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": server.URL,
	}}

	_, _, _, err := c.Vectorize(context.Background(), []string{"first", "second"}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index 0")
}

func TestClient_Vectorize_EmptyEmbedding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"data": [
				{"object": "embedding", "index": 0, "embedding": []}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": server.URL,
	}}

	_, _, _, err := c.Vectorize(context.Background(), []string{"only"}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty embedding")
}

func TestClient_Vectorize_HeaderBaseURLOverride(t *testing.T) {
	var hit bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.5]}]}`)
	}))
	t.Cleanup(server.Close)

	c := New("test-key", 5*time.Second, logrus.New())
	cfg := fakeClassConfig{cfg: map[string]any{
		"model":   "qwen3-embedding-0.6b",
		"baseURL": "https://should-be-overridden.invalid",
	}}

	ctx := context.WithValue(context.Background(), "X-Digitalocean-Baseurl", []string{server.URL})
	_, _, _, err := c.Vectorize(ctx, []string{"hi"}, cfg)
	require.NoError(t, err)
	assert.True(t, hit, "request should have hit the override server")
}
