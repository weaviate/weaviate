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

package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		ctxWithClusterURL := context.WithValue(context.Background(), "X-Weaviate-Cluster-Url", []string{server.URL})
		res, _, _, err := c.Vectorize(ctxWithClusterURL, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		ctxWithClusterURL := context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})
		defer cancel()

		_, _, _, err := c.Vectorize(ctxWithClusterURL, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}
		ctxWithClusterURL := context.WithValue(context.Background(), "X-Weaviate-Cluster-Url", []string{server.URL})
		_, _, _, err := c.Vectorize(ctxWithClusterURL, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Weaviate embed API error: 500 ")
	})

	t.Run("when Weaviate API key is passed using Authorization header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(), "Authorization", []string{"some-key"})
		ctxWithBothValues := context.WithValue(ctxWithValue, "X-Weaviate-Cluster-Url", []string{server.URL})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, _, err := c.Vectorize(ctxWithBothValues, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when Weaviate API key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		ctxWithClusterURL := context.WithValue(ctx, "X-Weaviate-Cluster-URL", []string{server.URL})
		defer cancel()

		_, _, _, err := c.Vectorize(ctxWithClusterURL, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.Equal(t, "authentication token: neither authentication token found in request header: Authorization "+
			"nor api key in environment variable under WEAVIATE_APIKEY", err.Error())
	})

	t.Run("when X-Weaviate-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"Authorization", []string{""})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{}})

		require.NotNil(t, err)
		assert.Equal(t, "authentication token: neither authentication token found in request header: Authorization "+
			"nor api key in environment variable under WEAVIATE_APIKEY", err.Error())
	})

	t.Run("when X-Weaviate-Baseurl header is passed", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}

		baseURL := "http://default-url.com"
		ctxWithValue := context.WithValue(context.Background(),
			"X-Weaviate-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL := c.getWeaviateEmbedURL(ctxWithValue, baseURL)
		assert.Equal(t, "http://base-url-passed-in-header.com/v1/embeddings/embed", buildURL)

		buildURL = c.getWeaviateEmbedURL(context.TODO(), baseURL)
		assert.Equal(t, "http://default-url.com/v1/embeddings/embed", buildURL)
	})

	t.Run("pass rate limit headers requests", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Weaviate-Ratelimit-RequestPM-Embedding", []string{"50"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{classConfig: map[string]interface{}{}})
		assert.Equal(t, 50, rl.LimitRequests)
		assert.Equal(t, 50, rl.RemainingRequests)
	})

	t.Run("when X-Weaviate-Cluster-URL header is missing", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}

		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.Equal(t, "cluster URL: no cluster URL found in request header: X-Weaviate-Cluster-Url", err.Error())
	})

	t.Run("TestVectorizeRequestBodyWithCustomDimensions", func(t *testing.T) {
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &weaviateEmbedUrlBuilder{
				origin:   "http://example.com",
				pathMask: "/v1/embeddings/embed",
			},
			logger: nullLogger(),
		}

		dims := int64(256)
		cfg := &fakeClassConfig{
			classConfig: map[string]interface{}{
				"dimensions": dims,
			},
		}

		config := c.getVectorizationConfig(cfg)
		reqBody := c.getEmbeddingsRequest([]string{"test text"}, false, config.Dimensions)

		require.NotNil(t, reqBody.Dimensions)
		require.Equal(t, int64(256), *reqBody.Dimensions)
		require.Equal(t, []string{"test text"}, reqBody.Texts)
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

	textInput := b["texts"].([]interface{})
	assert.Greater(f.t, len(textInput), 0)

	embeddingResponse := map[string]interface{}{
		"embeddings": [][]float32{{0.1, 0.2, 0.3}},
	}
	outBytes, err := json.Marshal(embeddingResponse)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
