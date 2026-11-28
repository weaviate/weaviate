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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchVectorizer(t *testing.T) {
	t.Run("when vectorizing batch of texts", func(t *testing.T) {
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			assert.Equal(t, "/vectors", r.URL.Path)
			assert.Equal(t, http.MethodPost, r.Method)

			var req struct {
				Texts  []string `json:"texts"`
				Config struct {
					PoolingStrategy string `json:"pooling_strategy"`
					TaskType        string `json:"task_type"`
				} `json:"config"`
			}
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			assert.Len(t, req.Texts, 3)
			assert.Equal(t, "passage", req.Config.TaskType)

			vectors := make([][]float32, len(req.Texts))
			for i := range vectors {
				vectors[i] = []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3}
			}

			json.NewEncoder(w).Encode(map[string]any{
				"vectors": vectors,
				"dims":    3,
			})
		}))
		defer server.Close()

		client := NewBatchVectorizer(server.URL, server.URL, 30*time.Second, nullLogger())

		result, _, _, err := client.Vectorize(context.Background(),
			[]string{"hello", "world", "test"}, nil)

		require.NoError(t, err)
		assert.Len(t, result.Vector, 3)
		assert.Equal(t, 1, requestCount, "should make exactly 1 HTTP request for batch")
	})

	t.Run("when vectorizing queries", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Texts  []string `json:"texts"`
				Config struct {
					TaskType string `json:"task_type"`
				} `json:"config"`
			}
			json.NewDecoder(r.Body).Decode(&req)

			assert.Equal(t, "query", req.Config.TaskType)

			vectors := make([][]float32, len(req.Texts))
			for i := range vectors {
				vectors[i] = []float32{0.1, 0.2, 0.3}
			}

			json.NewEncoder(w).Encode(map[string]any{
				"vectors": vectors,
				"dims":    3,
			})
		}))
		defer server.Close()

		client := NewBatchVectorizer(server.URL, server.URL, 30*time.Second, nullLogger())

		result, err := client.VectorizeQuery(context.Background(),
			[]string{"search query"}, nil)

		require.NoError(t, err)
		assert.Len(t, result.Vector, 1)
	})

	t.Run("when input is empty", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{
				"vectors": [][]float32{},
				"dims":    3,
			})
		}))
		defer server.Close()

		client := NewBatchVectorizer(server.URL, server.URL, 30*time.Second, nullLogger())

		result, _, _, err := client.Vectorize(context.Background(), []string{}, nil)

		require.NoError(t, err)
		assert.Empty(t, result.Vector)
	})

	t.Run("when server returns error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{
				"error": "model inference failed",
			})
		}))
		defer server.Close()

		client := NewBatchVectorizer(server.URL, server.URL, 30*time.Second, nullLogger())

		_, _, _, err := client.Vectorize(context.Background(),
			[]string{"test"}, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})

	t.Run("when context is cancelled", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			json.NewEncoder(w).Encode(map[string]any{
				"vectors": [][]float32{{0.1, 0.2}},
				"dims":    2,
			})
		}))
		defer server.Close()

		client := NewBatchVectorizer(server.URL, server.URL, 30*time.Second, nullLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, _, _, err := client.Vectorize(ctx, []string{"test"}, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context")
	})

	t.Run("when vector count mismatch", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Return fewer vectors than requested
			json.NewEncoder(w).Encode(map[string]any{
				"vectors": [][]float32{{0.1, 0.2}},
				"dims":    2,
			})
		}))
		defer server.Close()

		client := NewBatchVectorizer(server.URL, server.URL, 30*time.Second, nullLogger())

		_, _, _, err := client.Vectorize(context.Background(),
			[]string{"text1", "text2", "text3"}, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected 3 vectors")
	})
}
