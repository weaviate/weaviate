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

package weaviateembed

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New[[]float32](0, 0, 0)
		expected := &embeddingsResponse[[]float32]{
			Embeddings: [][]float32{{0.1, 0.2, 0.3}},
			Metadata: metadata{
				Model:                 "model",
				TimeTakenInference:    0.1,
				NumEmbeddingsInferred: 1,
				Usage: &modulecomponents.Usage{
					CompletionTokens: 0,
					PromptTokens:     0,
					TotalTokens:      1,
				},
			},
		}
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})
		res, err := c.Vectorize(ctx, []string{"input"}, false, nil, "model", server.URL)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New[[]float32](0, 0, 0)

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		ctx = context.WithValue(ctx, "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"input"}, false, nil, "model", server.URL)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := New[[]float32](0, 0, 0)

		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})
		_, err := c.Vectorize(ctx, []string{"input"}, false, nil, "model", server.URL)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Weaviate embed API error: 500 ")
	})

	t.Run("when Authorization header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New[[]float32](0, 0, 0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{""})

		_, err := c.Vectorize(ctx, []string{"input"}, false, nil, "model", server.URL)

		require.NotNil(t, err)
		assert.Equal(t, "authentication token: no authentication token found in request header: Authorization", err.Error())
	})

	t.Run("get embed URL", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New[[]float32](0, 0, 0)

		url, _ := c.getWeaviateEmbedURL(context.Background(), "")
		assert.Equal(t, "https://api.embedding.weaviate.io/v1/embeddings/embed", url)

		url, _ = c.getWeaviateEmbedURL(context.Background(), "http://override-url.com")
		assert.Equal(t, "http://override-url.com/v1/embeddings/embed", url)
	})

	t.Run("pass rate limit headers requests", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New[[]float32](0, 0, 0)

		ctxWithValue := context.WithValue(context.Background(),
			"X-Weaviate-Ratelimit-RequestPM-Embedding", []string{"50"})

		var cfg moduletools.ClassConfig
		rl := c.GetVectorizerRateLimit(ctxWithValue, cfg)
		assert.Equal(t, 50, rl.LimitRequests)
		assert.Equal(t, 50, rl.RemainingRequests)
	})

	t.Run("when X-Weaviate-Cluster-URL header is missing", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New[[]float32](0, 0, 0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})

		_, err := c.Vectorize(ctx, []string{"input"}, false, nil, "model", server.URL)

		require.NotNil(t, err)
		assert.Equal(t, "cluster URL: no cluster URL found in request header: X-Weaviate-Cluster-Url", err.Error())
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		embeddingError := map[string]interface{}{
			"message": f.serverError.Error(),
			"type":    "invalid_request_error",
		}
		embeddingResponse := map[string]interface{}{
			"message": embeddingError["message"],
		}
		outBytes, err := json.Marshal(embeddingResponse)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	embeddingResponse := map[string]interface{}{
		"embeddings": [][]float32{{0.1, 0.2, 0.3}},
		"metadata": metadata{
			Model:                 "model",
			TimeTakenInference:    0.1,
			NumEmbeddingsInferred: 1,
			Usage: &modulecomponents.Usage{
				CompletionTokens: 0,
				PromptTokens:     0,
				TotalTokens:      1,
			},
		},
	}
	outBytes, err := json.Marshal(embeddingResponse)
	require.Nil(f.t, err)

	w.Write(outBytes)
}
