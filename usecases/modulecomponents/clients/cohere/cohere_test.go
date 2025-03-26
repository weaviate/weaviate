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

package cohere

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
		c := &Client{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		settings := Settings{
			Model:   "large",
			BaseURL: server.URL,
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"}, settings)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}
		settings := Settings{
			Model:   "large",
			BaseURL: server.URL,
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, settings)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := &Client{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}
		settings := Settings{
			Model:   "large",
			BaseURL: server.URL,
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"}, settings)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "connection to Cohere failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when Cohere key is passed using X-Cohere-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		settings := Settings{
			Model:   "large",
			BaseURL: server.URL,
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, settings)

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when Cohere key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}
		settings := Settings{
			Model:   "large",
			BaseURL: server.URL,
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, settings)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Cohere API Key: no api key found "+
			"neither in request header: X-Cohere-Api-Key "+
			"nor in environment variable under COHERE_APIKEY")
	})

	t.Run("when X-Cohere-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}
		settings := Settings{
			Model:   "large",
			BaseURL: server.URL,
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, settings)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Cohere API Key: no api key found "+
			"neither in request header: X-Cohere-Api-Key "+
			"nor in environment variable under COHERE_APIKEY")
	})

	t.Run("when X-Cohere-BaseURL header is passed", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}

		baseURL := "http://default-url.com"
		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL := c.getCohereUrl(ctxWithValue, baseURL)
		assert.Equal(t, "http://base-url-passed-in-header.com/v2/embed", buildURL)

		buildURL = c.getCohereUrl(context.TODO(), baseURL)
		assert.Equal(t, "http://default-url.com/v2/embed", buildURL)
	})

	t.Run("pass rate limit headers requests", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &cohereUrlBuilder{
				origin:   server.URL,
				pathMask: "/v2/embed",
			},
			logger: nullLogger(),
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Ratelimit-RequestPM-Embedding", []string{"50"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{classConfig: map[string]interface{}{}})
		assert.Equal(t, 50, rl.LimitRequests)
		assert.Equal(t, 50, rl.RemainingRequests)
	})
}

func TestParse(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		t.Run("text", func(t *testing.T) {
			c := New("apiKey", time.Second, nil)
			request := c.getEmbeddingRequest([]string{"hello", "goodbye"}, Settings{
				Model:     "embed-english-v3.0",
				InputType: SearchDocument,
				Truncate:  "END",
				BaseURL:   "some-base-url",
			})
			check := `{
				"model": "embed-english-v3.0",
				"texts": ["hello", "goodbye"],
				"input_type": "search_document",
				"embedding_types": ["float"],
				"truncate": "END"
			}`
			var checkRequest embeddingsRequest
			err := json.Unmarshal([]byte(check), &checkRequest)
			require.NoError(t, err)
			assert.Equal(t, checkRequest.Model, request.Model)
			assert.Equal(t, checkRequest.Texts, request.Texts)
			assert.Equal(t, checkRequest.Images, request.Images)
			assert.Equal(t, checkRequest.InputType, request.InputType)
			assert.Equal(t, checkRequest.EmbeddingTypes, request.EmbeddingTypes)
			assert.Equal(t, checkRequest.Truncate, request.Truncate)
		})
		t.Run("image", func(t *testing.T) {
			c := New("apiKey", time.Second, nil)
			request := c.getEmbeddingRequest([]string{"data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD//gAfQ29tcHJlc3NlZ"}, Settings{
				Model:     "embed-english-v3.0",
				InputType: Image,
				Truncate:  "END",
				BaseURL:   "some-base-url",
			})
			check := `{
				"model": "embed-english-v3.0",
				"input_type": "image", 
				"embedding_types": ["float"],
				"images": ["data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD//gAfQ29tcHJlc3NlZ"]
			}`
			var checkRequest embeddingsRequest
			err := json.Unmarshal([]byte(check), &checkRequest)
			require.NoError(t, err)
			assert.Equal(t, checkRequest.Model, request.Model)
			assert.Equal(t, checkRequest.Texts, request.Texts)
			assert.Equal(t, checkRequest.Images, request.Images)
			assert.Equal(t, checkRequest.InputType, request.InputType)
			assert.Equal(t, checkRequest.EmbeddingTypes, request.EmbeddingTypes)
			assert.Equal(t, checkRequest.Truncate, request.Truncate)
		})
	})
	t.Run("response", func(t *testing.T) {
		t.Run("text", func(t *testing.T) {
			response := `{
				"id": "da6e531f-54c6-4a73-bf92-f60566d8d753",
				"embeddings": {
					"float": [
						[0.016296387,-0.008354187,-0.04699707],
						[0.1,-0.2,-0.3]
					]
				},
				"texts": [
					"hello",
					"goodbye"
				],
				"meta": {
					"api_version": {
						"version": "2",
						"is_experimental": true
					},
					"billed_units": {
						"input_tokens": 2
					},
					"warnings": [
						"You are using an experimental version, for more information please refer to https://docs.cohere.com/versioning-reference"
					]
				}
			}`
			var resp embeddingsResponse
			err := json.Unmarshal([]byte(response), &resp)
			require.NoError(t, err)
			assert.Equal(t, "da6e531f-54c6-4a73-bf92-f60566d8d753", resp.ID)
			require.Len(t, resp.Embeddings.Float, 2)
			assert.Equal(t, []float32{0.016296387, -0.008354187, -0.04699707}, resp.Embeddings.Float[0])
			assert.Equal(t, []float32{0.1, -0.2, -0.3}, resp.Embeddings.Float[1])
			assert.Equal(t, 2, resp.Meta.BilledUnits.InputTokens)
		})
		t.Run("image", func(t *testing.T) {
			response := `{
				"id": "5807ee2e-0cda-445a-9ec8-864c60a06606",
				"embeddings": {
					"float": [
						[-0.007247925, -0.041229248]
					]
				},
				"texts": [],
				"images": [
					{
						"width": 400,
						"height": 400,
						"format": "jpeg",
						"bit_depth": 24
					}
				],
				"meta": {
					"api_version": {
						"version": "2"
					},
					"billed_units": {
						"images": 1
					}
				}
			}`
			var resp embeddingsResponse
			err := json.Unmarshal([]byte(response), &resp)
			require.NoError(t, err)
			assert.Equal(t, "5807ee2e-0cda-445a-9ec8-864c60a06606", resp.ID)
			require.Len(t, resp.Embeddings.Float, 1)
			assert.Equal(t, []float32{-0.007247925, -0.041229248}, resp.Embeddings.Float[0])
			assert.Equal(t, 1, resp.Meta.BilledUnits.Images)
		})
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
		"embeddings": map[string]interface{}{
			"float": [][]float32{{0.1, 0.2, 0.3}},
		},
	}
	outBytes, err := json.Marshal(embeddingResponse)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
