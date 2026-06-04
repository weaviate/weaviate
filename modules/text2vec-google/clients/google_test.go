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
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
	"golang.org/x/time/rate"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			urlBuilderFn: func(useGenerativeAI bool, apiEndpoint, projectID, modelID string) string {
				assert.Equal(t, "endpoint", apiEndpoint)
				assert.Equal(t, "project", projectID)
				assert.Equal(t, "model", modelID)
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.vectorize(context.Background(), []string{"This is my text"}, retrievalDocument, "",
			settings{
				ApiEndpoint: "endpoint",
				ProjectID:   "project",
				Model:       "model",
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
			urlBuilderFn: func(useGenerativeAI bool, apiEndpoint, projectID, modelID string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.vectorize(ctx, []string{"This is my text"}, retrievalDocument, "", settings{})

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
			urlBuilderFn: func(useGenerativeAI bool, apiEndpoint, projectID, modelID string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		_, err := c.vectorize(context.Background(), []string{"This is my text"}, retrievalDocument, "", settings{})

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
			urlBuilderFn: func(useGenerativeAI bool, apiEndpoint, projectID, modelID string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Palm-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.vectorize(ctxWithValue, []string{"This is my text"}, retrievalDocument, "", settings{})

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
			urlBuilderFn: func(useGenerativeAI bool, apiEndpoint, projectID, modelID string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.vectorize(ctx, []string{"This is my text"}, retrievalDocument, "", settings{})

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

		_, err := c.vectorize(ctxWithValue, []string{"This is my text"}, retrievalDocument, "", settings{})

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

func TestWaitForQuota(t *testing.T) {
	tests := []struct {
		name        string
		limiter     *rate.Limiter
		n           int
		ctxTimeout  time.Duration
		expectError bool
	}{
		{
			name:    "nil limiter is a no-op",
			limiter: nil,
			n:       1000,
		},
		{
			name:    "request fitting in burst",
			limiter: rate.NewLimiter(rate.Limit(1000), 100),
			n:       50,
		},
		{
			name:    "request larger than burst is chunked",
			limiter: rate.NewLimiter(rate.Limit(1000), 10),
			n:       150,
		},
		{
			name:        "context deadline while waiting",
			limiter:     rate.NewLimiter(rate.Limit(1), 1),
			n:           10,
			ctxTimeout:  10 * time.Millisecond,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()
			}
			v := &google{rateLimiter: tt.limiter}
			err := v.waitForQuota(ctx, tt.n)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRateLimiterPacing(t *testing.T) {
	// rpm of 600 = 10 embeddings/second. Sending 3 single-input requests after
	// draining the burst should take at least ~200ms.
	c := New("apiKey", false, 600, time.Minute, logrus.New())
	require.NotNil(t, c.rateLimiter)
	require.NoError(t, c.waitForQuota(context.Background(), c.rateLimiter.Burst())) // drain burst

	start := time.Now()
	for i := 0; i < 3; i++ {
		require.NoError(t, c.waitForQuota(context.Background(), 1))
	}
	assert.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
}
