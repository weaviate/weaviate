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

package voyageai

import (
	"context"
	"encoding/json"
	"fmt"
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
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, Settings{Model: "voyage-2", BaseURL: server.URL})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, Settings{Model: "voyage-2"})

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
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}
		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, Settings{Model: "voyage-2", BaseURL: server.URL})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "connection to VoyageAI failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when VoyageAI key is passed using VoyageAIre-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Voyageai-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, Settings{Model: "voyage-2", BaseURL: server.URL})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when VoyageAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, Settings{Model: "voyage-2"})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "VoyageAI API Key: no api key found "+
			"neither in request header: X-VoyageAI-Api-Key "+
			"nor in environment variable under VOYAGEAI_APIKEY")
	})

	t.Run("when X-VoyageAI-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-VoyageAI-Api-Key", []string{""})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, Settings{Model: "voyage-2"})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "VoyageAI API Key: no api key found "+
			"neither in request header: X-VoyageAI-Api-Key "+
			"nor in environment variable under VOYAGEAI_APIKEY")
	})

	t.Run("when X-VoyageAI-BaseURL header is passed", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &Client{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &fakeVoyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/embeddings",
			},
			logger: nullLogger(),
		}

		baseURL := "http://default-url.com"
		ctxWithValue := context.WithValue(context.Background(),
			"X-Voyageai-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := c.getVoyageAIUrl(ctxWithValue, baseURL, "test-model")
		assert.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com/embeddings", buildURL)

		buildURL, err = c.getVoyageAIUrl(context.TODO(), baseURL, "test-model")
		assert.NoError(t, err)
		assert.Equal(t, "http://default-url.com/embeddings", buildURL)
	})
}

func TestVectorizeMultiModalMalformedResponse(t *testing.T) {
	tests := []struct {
		name    string
		texts   []string
		images  []string
		videos  []string
		body    string
		wantErr string
	}{
		{
			name:    "empty data array",
			texts:   []string{"text"},
			body:    `{"data":[]}`,
			wantErr: "empty embeddings response",
		},
		{
			name:    "more embeddings than inputs",
			texts:   []string{"text"},
			images:  []string{"image"},
			body:    `{"data":[{"index":0,"embedding":[0.1]},{"index":1,"embedding":[0.2]},{"index":2,"embedding":[0.3]}]}`,
			wantErr: "embedding index 2 out of range for 2 inputs",
		},
		{
			name:    "negative index",
			texts:   []string{"text"},
			body:    `{"data":[{"index":-1,"embedding":[0.1]}]}`,
			wantErr: "embedding index -1 out of range for 1 inputs",
		},
		{
			name:    "duplicate index",
			texts:   []string{"first", "second"},
			body:    `{"data":[{"index":0,"embedding":[0.1]},{"index":0,"embedding":[0.2]}]}`,
			wantErr: "duplicate index 0",
		},
		{
			name:    "fewer embeddings than inputs",
			texts:   []string{"text"},
			images:  []string{"image"},
			body:    `{"data":[{"index":0,"embedding":[0.1]}]}`,
			wantErr: "missing embedding for input 1 (got 1 of 2)",
		},
		{
			name:    "missing video embedding",
			texts:   []string{"text"},
			videos:  []string{"video"},
			body:    `{"data":[{"index":0,"embedding":[0.1]}]}`,
			wantErr: "missing embedding for input 1 (got 1 of 2)",
		},
		{
			name:    "empty embedding after a valid one",
			texts:   []string{"text"},
			images:  []string{"image"},
			body:    `{"data":[{"index":0,"embedding":[0.1]},{"index":1,"embedding":[]}]}`,
			wantErr: "empty embedding for index 1",
		},
		{
			name:    "empty embedding for an earlier index",
			texts:   []string{"text"},
			images:  []string{"image"},
			body:    `{"data":[{"index":1,"embedding":[0.1]},{"index":0,"embedding":null}]}`,
			wantErr: "empty embedding for index 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newClientWithBody(t, tt.body)
			res, err := c.VectorizeMultiModal(context.Background(), tt.texts, tt.images, tt.videos,
				Settings{Model: "voyage-multimodal-3"})
			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, res)
		})
	}
}

func TestVectorizeMultiModalResponseOrder(t *testing.T) {
	tests := []struct {
		name   string
		texts  []string
		images []string
		videos []string
		body   string
		want   *modulecomponents.VectorizationCLIPResult[[]float32]
	}{
		{
			name:   "in order",
			texts:  []string{"text"},
			images: []string{"image"},
			videos: []string{"video"},
			body:   `{"data":[{"index":0,"embedding":[0.1]},{"index":1,"embedding":[0.2]},{"index":2,"embedding":[0.3]}]}`,
			want: &modulecomponents.VectorizationCLIPResult[[]float32]{
				TextVectors:  [][]float32{{0.1}},
				ImageVectors: [][]float32{{0.2}},
				VideoVectors: [][]float32{{0.3}},
			},
		},
		{
			name:   "out of order",
			texts:  []string{"text"},
			images: []string{"image"},
			videos: []string{"video"},
			body:   `{"data":[{"index":2,"embedding":[0.3]},{"index":0,"embedding":[0.1]},{"index":1,"embedding":[0.2]}]}`,
			want: &modulecomponents.VectorizationCLIPResult[[]float32]{
				TextVectors:  [][]float32{{0.1}},
				ImageVectors: [][]float32{{0.2}},
				VideoVectors: [][]float32{{0.3}},
			},
		},
		{
			name:   "texts and videos without images, out of order",
			texts:  []string{"first", "second"},
			videos: []string{"video"},
			body:   `{"data":[{"index":2,"embedding":[0.3]},{"index":1,"embedding":[0.2]},{"index":0,"embedding":[0.1]}]}`,
			want: &modulecomponents.VectorizationCLIPResult[[]float32]{
				TextVectors:  [][]float32{{0.1}, {0.2}},
				VideoVectors: [][]float32{{0.3}},
			},
		},
		{
			name:   "images only, out of order",
			images: []string{"first", "second"},
			body:   `{"data":[{"index":1,"embedding":[0.2]},{"index":0,"embedding":[0.1]}]}`,
			want: &modulecomponents.VectorizationCLIPResult[[]float32]{
				ImageVectors: [][]float32{{0.1}, {0.2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, requests := newClientWithBody(t, tt.body)
			res, err := c.VectorizeMultiModal(context.Background(), tt.texts, tt.images, tt.videos,
				Settings{Model: "voyage-multimodal-3"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, res)

			var wantTypes []multimodalType
			for range tt.texts {
				wantTypes = append(wantTypes, text)
			}
			for range tt.images {
				wantTypes = append(wantTypes, imageBase64)
			}
			for range tt.videos {
				wantTypes = append(wantTypes, videoBase64)
			}
			var gotTypes []multimodalType
			for _, input := range (<-requests).Inputs {
				require.Len(t, input.Content, 1)
				gotTypes = append(gotTypes, input.Content[0].MultimodalType)
			}
			assert.Equal(t, wantTypes, gotTypes, "request inputs must be texts, then images, then videos")
		})
	}
}

// newClientWithBody returns a client whose server answers 200 with body, and
// a channel that receives the first request that server decoded.
func newClientWithBody(t *testing.T, body string) (*Client, <-chan embeddingsRequest) {
	t.Helper()
	requests := make(chan embeddingsRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req embeddingsRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		select {
		case requests <- req:
		default:
		}
		w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return New("apiKey", 0, &fakeVoyageaiUrlBuilder{origin: server.URL, pathMask: "/multimodalembeddings"}, nullLogger()), requests
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		resp := embeddingsResponse{
			Detail: "nope, not gonna happen",
		}
		outBytes, err := json.Marshal(resp)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var req embeddingsRequest
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))

	assert.NotNil(f.t, req)
	assert.NotEmpty(f.t, req.Input)

	resp := embeddingsResponse{
		Data: []embeddingsDataResponse{{Embedding: []float32{0.1, 0.2, 0.3}}},
	}
	outBytes, err := json.Marshal(resp)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

type fakeVoyageaiUrlBuilder struct {
	origin   string
	pathMask string
}

func (c *fakeVoyageaiUrlBuilder) URL(baseURL, model string) string {
	if baseURL != "" {
		return fmt.Sprintf("%s%s", baseURL, c.pathMask)
	}
	return fmt.Sprintf("%s%s", c.origin, c.pathMask)
}
