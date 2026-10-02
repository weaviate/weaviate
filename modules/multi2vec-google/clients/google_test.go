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
	"github.com/weaviate/weaviate/modules/multi2vec-google/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
)

func TestVectorizeRejectsForeignEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		config ent.VectorizationConfig
	}{
		{
			name:   "apiEndpoint outside the Google API domain",
			config: ent.VectorizationConfig{ApiEndpoint: "attacker.example.com", Location: "us-central1", ProjectID: "project", Model: "model"},
		},
		{
			name:   "location carrying a host",
			config: ent.VectorizationConfig{ApiEndpoint: "us-central1-aiplatform.googleapis.com", Location: "attacker.example.com/", ProjectID: "project", Model: "model"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &google{
				apiKey:       "apiKey",
				httpClient:   &http.Client{},
				googleApiKey: apikey.NewGoogleApiKey(),
				urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
					t.Fatal("must not build a request URL for a rejected endpoint")
					return ""
				},
				logger: nullLogger(),
			}

			_, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, nil, nil, tt.config)

			require.Error(t, err)
		})
	}
}

func TestClient(t *testing.T) {
	t.Run("when all is fine we vectorize text", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				assert.Equal(t, "location", location)
				assert.Equal(t, "project", projectID)
				assert.Equal(t, "model", model)
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			TextVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, nil, nil,
			ent.VectorizationConfig{
				Location:  "location",
				ProjectID: "project",
				Model:     "model",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when all is fine we vectorize image", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				assert.Equal(t, "location", location)
				assert.Equal(t, "project", projectID)
				assert.Equal(t, "model", model)
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			ImageVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(context.Background(), nil, []string{"base64 encoded image"}, nil, nil,
			ent.VectorizationConfig{
				Location:  "location",
				ProjectID: "project",
				Model:     "model",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, nil, nil, nil, ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := &google{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, nil, nil,
			ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to Google failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when Palm key is passed using X-Palm-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Palm-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			TextVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, nil, nil, nil, ent.VectorizationConfig{})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when Palm key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, nil, nil, nil, ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, "Google API Key: no api key found "+
			"neither in request header: X-Palm-Api-Key or X-Goog-Api-Key or X-Goog-Vertex-Api-Key or X-Goog-Studio-Api-Key "+
			"nor in environment variable under PALM_APIKEY or GOOGLE_APIKEY", err.Error())
	})

	t.Run("when X-Palm-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "",
			googleApiKey: apikey.NewGoogleApiKey(),
			httpClient:   &http.Client{},
			urlBuilderFn: buildURL,
			logger:       nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Palm-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, nil, nil, nil, ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, "Google API Key: no api key found "+
			"neither in request header: X-Palm-Api-Key or X-Goog-Api-Key or X-Goog-Vertex-Api-Key or X-Goog-Studio-Api-Key "+
			"nor in environment variable under PALM_APIKEY or GOOGLE_APIKEY", err.Error())
	})
}

func TestGetApiKeyWithGeminiHeaders(t *testing.T) {
	geminiConfig := ent.VectorizationConfig{
		ApiEndpoint: "generativelanguage.googleapis.com",
		Model:       "gemini-embedding-2",
	}

	tests := []struct {
		name       string
		headerKey  string
		headerVal  string
		envApiKey  string
		wantApiKey string
		wantErr    bool
	}{
		{
			name:       "X-Goog-Studio-Api-Key header",
			headerKey:  "X-Goog-Studio-Api-Key",
			headerVal:  "studio-key-1",
			wantApiKey: "studio-key-1",
		},
		{
			name:       "X-Google-Studio-Api-Key header",
			headerKey:  "X-Google-Studio-Api-Key",
			headerVal:  "studio-key-2",
			wantApiKey: "studio-key-2",
		},
		{
			name:       "X-Goog-Api-Key header falls through for Gemini",
			headerKey:  "X-Goog-Api-Key",
			headerVal:  "goog-key",
			wantApiKey: "goog-key",
		},
		{
			name:       "X-Palm-Api-Key header falls through for Gemini",
			headerKey:  "X-Palm-Api-Key",
			headerVal:  "palm-key",
			wantApiKey: "palm-key",
		},
		{
			name:       "env api key used as fallback",
			envApiKey:  "env-key",
			wantApiKey: "env-key",
		},
		{
			name:    "no key returns error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(&fakeGeminiHandler{t: t})
			defer server.Close()
			c := &google{
				apiKey:       tt.envApiKey,
				httpClient:   &http.Client{},
				googleApiKey: apikey.NewGoogleApiKey(),
				urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
					return server.URL
				},
				logger: nullLogger(),
			}
			ctx := context.Background()
			if tt.headerKey != "" {
				ctx = context.WithValue(ctx, tt.headerKey, []string{tt.headerVal})
			}

			res, err := c.Vectorize(ctx, []string{"hello"}, nil, nil, nil, geminiConfig)

			if tt.wantErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), "no api key found")
			} else {
				require.Nil(t, err)
				assert.NotNil(t, res)
			}
		})
	}
}

func TestGetApiKeyVertexHeadersNotUsedForGemini(t *testing.T) {
	// Vertex-specific headers (X-Goog-Vertex-Api-Key) should NOT work for Gemini endpoint
	server := httptest.NewServer(&fakeGeminiHandler{t: t})
	defer server.Close()
	c := &google{
		apiKey:       "",
		httpClient:   &http.Client{},
		googleApiKey: apikey.NewGoogleApiKey(),
		urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
			return server.URL
		},
		logger: nullLogger(),
	}
	ctx := context.WithValue(context.Background(), "X-Goog-Vertex-Api-Key", []string{"vertex-key"})

	_, err := c.Vectorize(ctx, []string{"hello"}, nil, nil, nil, ent.VectorizationConfig{
		ApiEndpoint: "generativelanguage.googleapis.com",
		Model:       "gemini-embedding-2",
	})

	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "no api key found")
}

func TestGetApiKeyVertexHeaders(t *testing.T) {
	vertexConfig := ent.VectorizationConfig{
		Location:  "us-central1",
		ProjectID: "my-project",
		Model:     "multimodalembedding",
	}

	tests := []struct {
		name       string
		headerKey  string
		headerVal  string
		envApiKey  string
		wantApiKey string
	}{
		{
			name:       "X-Goog-Vertex-Api-Key header",
			headerKey:  "X-Goog-Vertex-Api-Key",
			headerVal:  "vertex-key-1",
			wantApiKey: "vertex-key-1",
		},
		{
			name:       "X-Google-Vertex-Api-Key header",
			headerKey:  "X-Google-Vertex-Api-Key",
			headerVal:  "vertex-key-2",
			wantApiKey: "vertex-key-2",
		},
		{
			name:       "X-Goog-Api-Key header falls through for Vertex",
			headerKey:  "X-Goog-Api-Key",
			headerVal:  "goog-key",
			wantApiKey: "goog-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(&fakeHandler{t: t})
			defer server.Close()
			c := &google{
				apiKey:       tt.envApiKey,
				httpClient:   &http.Client{},
				googleApiKey: apikey.NewGoogleApiKey(),
				urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
					return server.URL
				},
				logger: nullLogger(),
			}
			ctx := context.WithValue(context.Background(), tt.headerKey, []string{tt.headerVal})

			res, err := c.Vectorize(ctx, []string{"hello"}, nil, nil, nil, vertexConfig)

			require.Nil(t, err)
			assert.NotNil(t, res)
		})
	}
}

func TestGeminiStudioHeaderNotUsedForVertex(t *testing.T) {
	// Studio-specific headers should NOT work for Vertex endpoint
	server := httptest.NewServer(&fakeHandler{t: t})
	defer server.Close()
	c := &google{
		apiKey:       "",
		httpClient:   &http.Client{},
		googleApiKey: apikey.NewGoogleApiKey(),
		urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
			return server.URL
		},
		logger: nullLogger(),
	}
	ctx := context.WithValue(context.Background(), "X-Goog-Studio-Api-Key", []string{"studio-key"})

	_, err := c.Vectorize(ctx, []string{"hello"}, nil, nil, nil, ent.VectorizationConfig{
		Location:  "us-central1",
		ProjectID: "my-project",
		Model:     "multimodalembedding",
	})

	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "no api key found")
}

func TestGeminiClient(t *testing.T) {
	t.Run("when all is fine we vectorize audio via Gemini API", func(t *testing.T) {
		server := httptest.NewServer(&fakeGeminiHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			AudioVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(context.Background(), nil, nil, nil, []string{"base64-encoded-audio"},
			ent.VectorizationConfig{
				ApiEndpoint: "generativelanguage.googleapis.com",
				Model:       "gemini-embedding-2",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when all is fine we vectorize text and audio via Gemini API", func(t *testing.T) {
		server := httptest.NewServer(&fakeGeminiHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			TextVectors:  [][]float32{{0.1, 0.2, 0.3}},
			AudioVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(context.Background(), []string{"some text"}, nil, nil, []string{"base64-encoded-audio"},
			ent.VectorizationConfig{
				ApiEndpoint: "generativelanguage.googleapis.com",
				Model:       "gemini-embedding-2",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		embeddingResponse := &embeddingsResponse{
			Error: &googleApiError{
				Code:    http.StatusInternalServerError,
				Status:  "error",
				Message: f.serverError.Error(),
			},
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

	var req embeddingsRequest
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))

	require.NotNil(f.t, req)
	require.Len(f.t, req.Instances, 1)

	textInput := req.Instances[0].Text
	if textInput != nil {
		assert.NotEmpty(f.t, *textInput)
	}
	imageInput := req.Instances[0].Image
	if imageInput != nil {
		assert.NotEmpty(f.t, *imageInput)
	}

	embedding := []float32{0.1, 0.2, 0.3}

	var resp embeddingsResponse

	if textInput != nil {
		resp = embeddingsResponse{
			Predictions: []prediction{{TextEmbedding: embedding}},
		}
	}
	if imageInput != nil {
		resp = embeddingsResponse{
			Predictions: []prediction{{ImageEmbedding: embedding}},
		}
	}

	outBytes, err := json.Marshal(resp)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

type fakeGeminiHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeGeminiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		resp := &batchEmbedResponse{
			Error: &googleApiError{
				Code:    http.StatusInternalServerError,
				Status:  "error",
				Message: f.serverError.Error(),
			},
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

	var req batchEmbedContents
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))

	embedding := []float32{0.1, 0.2, 0.3}
	var embeddings []embedContentEmbedding
	for range req.Requests {
		embeddings = append(embeddings, embedContentEmbedding{Values: embedding})
	}

	resp := batchEmbedResponse{Embeddings: embeddings}
	outBytes, err := json.Marshal(resp)
	require.Nil(f.t, err)
	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestBuildURL(t *testing.T) {
	tests := []struct {
		name        string
		apiEndpoint string
		location    string
		model       string
		want        string
	}{
		{
			name:        "AI Studio",
			apiEndpoint: "generativelanguage.googleapis.com",
			model:       "gemini-embedding-2",
			want:        "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:batchEmbedContents",
		},
		{
			name:        "Vertex regional predict",
			apiEndpoint: "us-central1-aiplatform.googleapis.com",
			location:    "europe-west4",
			model:       "multimodalembedding@001",
			want:        "https://europe-west4-aiplatform.googleapis.com/v1/projects/project/locations/europe-west4/publishers/google/models/multimodalembedding@001:predict",
		},
		{
			name:        "Vertex global predict uses the region-less host",
			apiEndpoint: "us-central1-aiplatform.googleapis.com",
			location:    "global",
			model:       "multimodalembedding@001",
			want:        "https://aiplatform.googleapis.com/v1/projects/project/locations/global/publishers/google/models/multimodalembedding@001:predict",
		},
		{
			name:        "Vertex gemini-embedding-2 with a regional location goes to global embedContent",
			apiEndpoint: "us-central1-aiplatform.googleapis.com",
			location:    "us-central1",
			model:       "gemini-embedding-2",
			want:        "https://aiplatform.googleapis.com/v1/projects/project/locations/global/publishers/google/models/gemini-embedding-2:embedContent",
		},
		{
			name:        "Vertex gemini-embedding-2 with global location",
			apiEndpoint: "us-central1-aiplatform.googleapis.com",
			location:    "global",
			model:       "gemini-embedding-2",
			want:        "https://aiplatform.googleapis.com/v1/projects/project/locations/global/publishers/google/models/gemini-embedding-2:embedContent",
		},
		{
			name:        "Vertex gemini-embedding-2 without location",
			apiEndpoint: "us-central1-aiplatform.googleapis.com",
			model:       "gemini-embedding-2",
			want:        "https://aiplatform.googleapis.com/v1/projects/project/locations/global/publishers/google/models/gemini-embedding-2:embedContent",
		},
		{
			name:        "Vertex gemini-embedding-2-preview",
			apiEndpoint: "us-central1-aiplatform.googleapis.com",
			location:    "us-central1",
			model:       "gemini-embedding-2-preview",
			want:        "https://aiplatform.googleapis.com/v1/projects/project/locations/global/publishers/google/models/gemini-embedding-2-preview:embedContent",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, buildURL(tt.apiEndpoint, tt.location, "project", tt.model))
		})
	}
}

func TestVertexEmbedContentClient(t *testing.T) {
	dimensions := int64(768)
	tests := []struct {
		name       string
		texts      []string
		images     []string
		videos     []string
		audios     []string
		dimensions *int64
		wantParts  []contentPart
		want       *ent.VectorizationResult
	}{
		{
			name:      "text",
			texts:     []string{"text"},
			wantParts: []contentPart{*textPart("text")},
			want:      &ent.VectorizationResult{TextVectors: [][]float32{{1}}},
		},
		{
			name:      "image",
			images:    []string{"img"},
			wantParts: []contentPart{*imagePart("img")},
			want:      &ent.VectorizationResult{ImageVectors: [][]float32{{1}}},
		},
		{
			name:      "video",
			videos:    []string{"vid"},
			wantParts: []contentPart{*videoPart("vid")},
			want:      &ent.VectorizationResult{VideoVectors: [][]float32{{1}}},
		},
		{
			name:      "audio",
			audios:    []string{"aud"},
			wantParts: []contentPart{*audioPart("aud")},
			want:      &ent.VectorizationResult{AudioVectors: [][]float32{{1}}},
		},
		{
			name:       "every modality gets its own request and vector",
			texts:      []string{"text"},
			images:     []string{"img"},
			videos:     []string{"vid"},
			audios:     []string{"aud"},
			dimensions: &dimensions,
			wantParts:  []contentPart{*textPart("text"), *imagePart("img"), *videoPart("vid"), *audioPart("aud")},
			want: &ent.VectorizationResult{
				TextVectors:  [][]float32{{1}},
				ImageVectors: [][]float32{{2}},
				VideoVectors: [][]float32{{3}},
				AudioVectors: [][]float32{{4}},
			},
		},
		{
			name:      "several objects",
			texts:     []string{"a", "b"},
			images:    []string{"img"},
			wantParts: []contentPart{*textPart("a"), *imagePart("img"), *textPart("b")},
			want: &ent.VectorizationResult{
				TextVectors:  [][]float32{{1}, {3}},
				ImageVectors: [][]float32{{2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotRequests []map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				var raw map[string]any
				require.NoError(t, json.Unmarshal(body, &raw))
				gotRequests = append(gotRequests, raw)

				resp := vertexEmbedContentResponse{
					Embedding: &embedContentEmbedding{Values: []float32{float32(len(gotRequests))}},
				}
				require.NoError(t, json.NewEncoder(w).Encode(resp))
			}))
			defer server.Close()

			res, err := newTestClient(server.URL).Vectorize(context.Background(), tt.texts, tt.images, tt.videos, tt.audios,
				ent.VectorizationConfig{ProjectID: "project", Model: "gemini-embedding-2", Dimensions: tt.dimensions})
			require.NoError(t, err)
			assert.Equal(t, tt.want, res)

			require.Len(t, gotRequests, len(tt.wantParts))
			for i, req := range gotRequests {
				var wantPart map[string]any
				partBytes, err := json.Marshal(tt.wantParts[i])
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(partBytes, &wantPart))
				assert.Equal(t, map[string]any{"parts": []any{wantPart}}, req["content"])

				assert.NotContains(t, req, "outputDimensionality", "deprecated top-level field")
				if tt.dimensions == nil {
					assert.NotContains(t, req, "embedContentConfig")
				} else {
					assert.Equal(t, map[string]any{"outputDimensionality": float64(*tt.dimensions)}, req["embedContentConfig"])
				}
			}
		})
	}
}

func TestClientErrorResponses(t *testing.T) {
	models := []struct {
		name        string
		apiEndpoint string
		model       string
	}{
		{name: "Vertex predict", model: "multimodalembedding@001"},
		{name: "Vertex embedContent", model: "gemini-embedding-2"},
		{name: "AI Studio", apiEndpoint: "generativelanguage.googleapis.com", model: "gemini-embedding-2"},
	}
	responses := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{
			name:    "HTML error page",
			status:  http.StatusNotFound,
			body:    "<!DOCTYPE html><html><body>404. That's an error.</body></html>",
			wantErr: "connection to Google failed with status: 404",
		},
		{
			name:    "JSON error",
			status:  http.StatusNotFound,
			body:    `{"error":{"code":404,"message":"Publisher model not found","status":"NOT_FOUND"}}`,
			wantErr: "connection to Google failed with status: 404 error: Publisher model not found",
		},
		{
			name:    "malformed success",
			status:  http.StatusOK,
			body:    "not json",
			wantErr: "failed to parse vectorization response (status 200)",
		},
		{
			name:    "empty success",
			status:  http.StatusOK,
			body:    "{}",
			wantErr: "empty embeddings response",
		},
	}
	for _, m := range models {
		for _, r := range responses {
			t.Run(m.name+"/"+r.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(r.status)
					w.Write([]byte(r.body))
				}))
				defer server.Close()

				_, err := newTestClient(server.URL).Vectorize(context.Background(), []string{"text"}, nil, nil, nil,
					ent.VectorizationConfig{ApiEndpoint: m.apiEndpoint, Location: "us-central1", ProjectID: "project", Model: m.model})
				require.Error(t, err)
				assert.ErrorContains(t, err, r.wantErr)
			})
		}
	}
}

func newTestClient(serverURL string) *google {
	return &google{
		apiKey:       "apiKey",
		httpClient:   &http.Client{},
		googleApiKey: apikey.NewGoogleApiKey(),
		urlBuilderFn: func(apiEndpoint, location, projectID, model string) string {
			return serverURL
		},
		logger: nullLogger(),
	}
}
