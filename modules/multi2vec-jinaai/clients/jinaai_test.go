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
)

func TestClient(t *testing.T) {
	defaultSettings := func(url string) fakeClassConfig {
		return fakeClassConfig{classConfig: map[string]interface{}{"Model": "jina-clip-v2", "baseURL": url}}
	}
	t.Run("when all is fine and we send text only", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("apiKey", 0, nullLogger())

		expected := &modulecomponents.VectorizationCLIPResult{
			TextVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, defaultSettings(server.URL))

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when all is fine and we send image only", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("apiKey", 0, nullLogger())

		expected := &modulecomponents.VectorizationCLIPResult{
			ImageVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(context.Background(), nil, []string{"base64"}, defaultSettings(server.URL))

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKey", 0, nullLogger())

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, nil, fakeClassConfig{classConfig: map[string]interface{}{}})

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

		_, err := c.Vectorize(context.Background(), []string{"This is my text"}, nil, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: JinaAI API failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when JinaAI key is passed using X-Jinaai-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Jinaai-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationCLIPResult{
			TextVectors: [][]float32{{0.1, 0.2, 0.3}},
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, nil, defaultSettings(server.URL))

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when JinaAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"This is my text"}, nil, fakeClassConfig{classConfig: map[string]interface{}{}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Jinaai-Api-Key "+
			"nor in environment variable under JINAAI_APIKEY")
	})

	t.Run("when X-Jinaai-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Jinaai-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, nil, fakeClassConfig{classConfig: map[string]interface{}{"Model": "jina-embedding-v2"}})

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
	textInput := textInputArray[0].(map[string]interface{})
	assert.Greater(f.t, len(textInput), 0)
	obj := textInput["text"]
	if textInput["image"] != nil {
		obj = textInput["image"]
	}

	embeddingTextData := map[string]interface{}{
		"object":    obj,
		"index":     0,
		"embedding": []float32{0.1, 0.2, 0.3},
	}

	embedding := map[string]interface{}{
		"object": "list",
		"data":   []interface{}{embeddingTextData},
	}

	outBytes, err := json.Marshal(embedding)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
