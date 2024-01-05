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
	"github.com/weaviate/weaviate/modules/text2vec-palm/ent"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &palm{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
				assert.Equal(t, "endpoint", apiEndoint)
				assert.Equal(t, "project", projectID)
				assert.Equal(t, "model", modelID)
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
			}, "")

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &palm{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
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
		c := &palm{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{}, "")

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to Google PaLM failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when Palm key is passed using X-Palm-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &palm{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
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
		c := &palm{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilderFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, ent.VectorizationConfig{}, "")

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Palm API Key: no api key found "+
			"neither in request header: X-Palm-Api-Key "+
			"nor in environment variable under PALM_APIKEY")
	})

	t.Run("when X-Palm-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &palm{
			apiKey:       "",
			httpClient:   &http.Client{},
			urlBuilderFn: buildURL,
			logger:       nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Palm-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, ent.VectorizationConfig{}, "")

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Palm API Key: no api key found "+
			"neither in request header: X-Palm-Api-Key "+
			"nor in environment variable under PALM_APIKEY")
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
			Error: &palmApiError{
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
