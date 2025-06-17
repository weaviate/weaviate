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
	"github.com/weaviate/weaviate/modules/text2vec-google/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
)

func TestBuildURL(t *testing.T) {
	tests := []struct {
		name               string
		useGenerativeAI    bool
		apiEndpoint        string
		projectID          string
		modelID            string
		location           string
		expectedURL        string
	}{
		{
			name:            "Vertex AI with custom location",
			useGenerativeAI: false,
			apiEndpoint:     "europe-west1-aiplatform.googleapis.com",
			projectID:       "my-project",
			modelID:         "textembedding-gecko@001",
			location:        "europe-west1",
			expectedURL:     "https://europe-west1-aiplatform.googleapis.com/v1/projects/my-project/locations/europe-west1/publishers/google/models/textembedding-gecko@001:predict",
		},
		{
			name:            "Vertex AI with us-central1",
			useGenerativeAI: false,
			apiEndpoint:     "us-central1-aiplatform.googleapis.com",
			projectID:       "my-project",
			modelID:         "textembedding-gecko@001",
			location:        "us-central1",
			expectedURL:     "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/publishers/google/models/textembedding-gecko@001:predict",
		},
		{
			name:            "Vertex AI with asia location",
			useGenerativeAI: false,
			apiEndpoint:     "asia-southeast1-aiplatform.googleapis.com",
			projectID:       "my-project",
			modelID:         "textembedding-gecko@001",
			location:        "asia-southeast1",
			expectedURL:     "https://asia-southeast1-aiplatform.googleapis.com/v1/projects/my-project/locations/asia-southeast1/publishers/google/models/textembedding-gecko@001:predict",
		},
		{
			name:            "Generative AI endpoint (location ignored)",
			useGenerativeAI: true,
			apiEndpoint:     "generativelanguage.googleapis.com",
			projectID:       "",
			modelID:         "embedding-001",
			location:        "europe-west1",
			expectedURL:     "https://generativelanguage.googleapis.com/v1/models/embedding-001:batchEmbedContents",
		},
		{
			name:            "Legacy PaLM model (location ignored)",
			useGenerativeAI: true,
			apiEndpoint:     "generativelanguage.googleapis.com",
			projectID:       "",
			modelID:         "embedding-gecko-001",
			location:        "europe-west1",
			expectedURL:     "https://generativelanguage.googleapis.com/v1beta3/models/embedding-gecko-001:batchEmbedText",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := buildURL(tt.useGenerativeAI, tt.apiEndpoint, tt.projectID, tt.modelID, tt.location)
			assert.Equal(t, tt.expectedURL, url)
		})
	}
}

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, location string) string {
				assert.Equal(t, "endpoint", apiEndoint)
				assert.Equal(t, "project", projectID)
				assert.Equal(t, "model", modelID)
				assert.Equal(t, "us-central1", location)
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			Texts:      []string{"This is my text"},
			Vectors:    [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{
				ApiEndpoint: "endpoint",
				ProjectID:   "project",
				Model:       "model",
				Location:    "us-central1",
			}, "")

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when using custom location", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, location string) string {
				assert.Equal(t, "europe-west1-aiplatform.googleapis.com", apiEndoint)
				assert.Equal(t, "my-project", projectID)
				assert.Equal(t, "textembedding-gecko@001", modelID)
				assert.Equal(t, "europe-west1", location)
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			Texts:      []string{"This is my text"},
			Vectors:    [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{
				ApiEndpoint: "europe-west1-aiplatform.googleapis.com",
				ProjectID:   "my-project",
				Model:       "textembedding-gecko@001",
				Location:    "europe-west1",
			}, "")

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
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, location string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, ent.VectorizationConfig{}, "")

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
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, location string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{}, "")

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
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, location string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Palm-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Texts:      []string{"This is my text"},
			Vectors:    [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, ent.VectorizationConfig{}, "")

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
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, location string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, ent.VectorizationConfig{}, "")

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Google API Key: no api key found "+
			"neither in request header: X-Palm-Api-Key or X-Goog-Api-Key or X-Goog-Vertex-Api-Key or X-Goog-Studio-Api-Key "+
			"nor in environment variable under PALM_APIKEY or GOOGLE_APIKEY")
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

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, ent.VectorizationConfig{}, "")

		require.NotNil(t, err)
		assert.Equal(t, "Google API Key: no api key found "+
			"neither in request header: X-Palm-Api-Key or X-Goog-Api-Key or X-Goog-Vertex-Api-Key or X-Goog-Studio-Api-Key "+
			"nor in environment variable under PALM_APIKEY or GOOGLE_APIKEY", err.Error())
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

	textInput := req.Instances[0].Content
	assert.Greater(f.t, len(textInput), 0)

	embeddingResponse := &embeddingsResponse{
		Predictions: []prediction{
			{
				Embeddings: embeddings{
					Values: []float32{0.1, 0.2, 0.3},
				},
			},
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
