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
	"github.com/weaviate/weaviate/modules/text2vec-octoai/ent"
)

func TestBuildUrlFn(t *testing.T) {
	t.Run("buildUrlFn returns default OctoAI URL", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Model:   "",
			BaseURL: "https://text.octoai.run",
		}
		url, err := buildUrl(config)
		assert.Nil(t, err)
		assert.Equal(t, "https://text.octoai.run/v1/embeddings", url)
	})

	t.Run("buildUrlFn loads from BaseURL", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Model:   "",
			BaseURL: "https://foobar.some.proxy",
		}
		url, err := buildUrl(config)
		assert.Nil(t, err)
		assert.Equal(t, "https://foobar.some.proxy/v1/embeddings", url)
	})
}

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("apiKey", 0, nullLogger())
		c.buildUrlFn = func(config ent.VectorizationConfig) (string, error) {
			return server.URL, nil
		}

		expected := &modulecomponents.VectorizationResult{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Model": "thenlper/gte-large", "baseURL": server.URL}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKey", 0, nullLogger())
		c.buildUrlFn = func(config ent.VectorizationConfig) (string, error) {
			return server.URL, nil
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{}})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := New("apiKey", 0, nullLogger())
		c.buildUrlFn = func(config ent.VectorizationConfig) (string, error) {
			return server.URL, nil
		}

		_, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: OctoAI API failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when OctoAI key is passed using X-OctoAI-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())
		c.buildUrlFn = func(config ent.VectorizationConfig) (string, error) {
			return server.URL, nil
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-OctoAI-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationResult{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Model": "thenlper/gte-large"}})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when OctoAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())
		c.buildUrlFn = func(config ent.VectorizationConfig) (string, error) {
			return server.URL, nil
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-OctoAI-Api-Key "+
			"nor in environment variable under OCTOAI_APIKEY")
	})

	t.Run("when X-OctoAI-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())
		c.buildUrlFn = func(config ent.VectorizationConfig) (string, error) {
			return server.URL, nil
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-OctoAI-Api-Key", []string{""})

		_, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Model": "thenlper/gte-large"}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-OctoAI-Api-Key "+
			"nor in environment variable under OCTOAI_APIKEY")
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
		embedding := map[string]interface{}{
			"error": embeddingError,
		}
		outBytes, err := json.Marshal(embedding)
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

	textInputArray := b["input"].([]interface{})
	textInput := textInputArray[0].(string)
	assert.Greater(f.t, len(textInput), 0)

	embeddingData := map[string]interface{}{
		"object":    textInput,
		"index":     0,
		"embedding": []float32{0.1, 0.2, 0.3},
	}
	embedding := map[string]interface{}{
		"object": "list",
		"data":   []interface{}{embeddingData},
	}

	outBytes, err := json.Marshal(embedding)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
