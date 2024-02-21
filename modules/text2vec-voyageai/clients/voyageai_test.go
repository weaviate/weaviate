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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/text2vec-voyageai/ent"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			Text:       []string{"This is my text"},
			Vectors:    [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{
				Model: "voyage-2",
				BaseURL: "https://api.voyageai.com",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, ent.VectorizationConfig{
			Model: "voyage-2",
			BaseURL: "https://api.voyageai.com",
		})

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
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{
				Model: "voyage-2",
				BaseURL: "https://api.voyageai.com",
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "connection to VoyageAI failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when VoyageAI key is passed using VoyageAIre-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-VoyageAI-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Text:       []string{"This is my text"},
			Vectors:    [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			ent.VectorizationConfig{
				Model: "voyage-2",
				BaseURL: "https://api.voyageai.com",
			})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when VoyageAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, ent.VectorizationConfig{
			Model: "voyage-2",
			BaseURL: "https://api.voyageai.com",
		})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "VoyageAI API Key: no api key found "+
			"neither in request header: X-VoyageAI-Api-Key "+
			"nor in environment variable under VOYAGE_API_KEY")
	})

	t.Run("when X-VoyageAI-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-VoyageAI-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			ent.VectorizationConfig{
				Model: "voyage-2",
				BaseURL: "https://api.voyageai.com",
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "VoyageAI API Key: no api key found "+
			"neither in request header: X-VoyageAI-Api-Key "+
			"nor in environment variable under VOYAGE_API_KEY")
	})

	t.Run("when X-VoyageAI-BaseURL header is passed", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/v1/embeddings",
			},
			logger: nullLogger(),
		}

		baseURL := "http://default-url.com"
		ctxWithValue := context.WithValue(context.Background(),
			"X-VoyageAI-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL := c.getVoyageAIUrl(ctxWithValue, baseURL)
		assert.Equal(t, "http://base-url-passed-in-header.com/v1/embeddings", buildURL)

		buildURL = c.getVoyageAIUrl(context.TODO(), baseURL)
		assert.Equal(t, "http://default-url.com/v1/embeddings", buildURL)
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
