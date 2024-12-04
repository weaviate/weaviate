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
	"github.com/weaviate/weaviate/modules/multi2vec-google/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine we vectorize text", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(location, projectID, model string) string {
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
		res, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, nil,
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
			urlBuilderFn: func(location, projectID, model string) string {
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
		res, err := c.Vectorize(context.Background(), nil, []string{"base64 encoded image"}, nil,
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
			urlBuilderFn: func(location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, nil, nil, ent.VectorizationConfig{})

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
			urlBuilderFn: func(location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, nil,
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
			urlBuilderFn: func(location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Palm-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			TextVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, nil, nil, ent.VectorizationConfig{})

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
			urlBuilderFn: func(location, projectID, model string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, nil, nil, ent.VectorizationConfig{})

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

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, nil, nil, ent.VectorizationConfig{})

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

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
