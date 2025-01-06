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

package jinaai

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

var defaultSettings = Settings{
	BaseURL: "https://api.jina.ai",
	Model:   "jina-embeddings-v2-base-en",
}

func TestBuildUrlFn(t *testing.T) {
	t.Run("buildUrlFn returns default Jina AI URL", func(t *testing.T) {
		settings := Settings{
			Model:   "",
			BaseURL: "https://api.jina.ai",
		}
		url, err := EmbeddingsBuildUrlFn(settings)
		assert.Nil(t, err)
		assert.Equal(t, "https://api.jina.ai/v1/embeddings", url)
	})

	t.Run("buildUrlFn loads from BaseURL", func(t *testing.T) {
		settings := Settings{
			Model:   "",
			BaseURL: "https://foobar.some.proxy",
		}
		url, err := EmbeddingsBuildUrlFn(settings)
		assert.Nil(t, err)
		assert.Equal(t, "https://foobar.some.proxy/v1/embeddings", url)
	})
}

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[]float32]("apiKey", 0, 0, 0, buildUrlFn, nullLogger())

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		settings := Settings{
			BaseURL: server.URL,
			Model:   "jina-embedding-v2",
		}
		res, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, settings)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when all is fine for ColBERT", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[][]float32]("apiKey", 0, 0, 0, buildUrlFn, nullLogger())

		expected := &modulecomponents.VectorizationResult[[][]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][][]float32{{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}, {0.111, 0.222, 0.333}}},
			Dimensions: 3,
		}
		settings := Settings{
			BaseURL: server.URL,
			Model:   "jina-colbert-v2",
		}
		res, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, settings)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[]float32]("apiKey", 0, 0, 0, buildUrlFn, nullLogger())

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, defaultSettings)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[]float32]("apiKey", 0, 0, 0, buildUrlFn, nullLogger())

		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, defaultSettings)

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: JinaAI API failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when JinaAI key is passed using X-Jinaai-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[]float32]("", 0, 0, 0, buildUrlFn, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Jinaai-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		settings := Settings{
			Model: "jina-embedding-v2",
		}
		res, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, settings)

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when JinaAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[]float32]("", 0, 0, 0, buildUrlFn, nullLogger())

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, Settings{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Jinaai-Api-Key "+
			"nor in environment variable under JINAAI_APIKEY")
	})

	t.Run("when X-Jinaai-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		buildUrlFn := func(settings Settings) (string, error) {
			return server.URL, nil
		}
		c := New[[]float32]("", 0, 0, 0, buildUrlFn, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Jinaai-Api-Key", []string{""})

		settings := Settings{
			Model: "jina-embedding-v2",
		}
		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, settings)

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Jinaai-Api-Key "+
			"nor in environment variable under JINAAI_APIKEY")
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		embedding := map[string]interface{}{
			"detail": f.serverError.Error(),
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

	var vector interface{}
	vector = []float32{0.1, 0.2, 0.3}
	model := b["model"].(string)
	if model == "jina-colbert-v2" {
		vector = [][]float32{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}, {0.111, 0.222, 0.333}}
	}

	embeddingData := map[string]interface{}{
		"object":    textInput,
		"index":     0,
		"embedding": vector,
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
