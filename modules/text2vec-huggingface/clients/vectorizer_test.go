//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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
	"github.com/semi-technologies/weaviate/modules/text2vec-huggingface/ent"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &huggingFaceUrlBuilder{
				origin:   server.URL,
				pathMask: "/pipeline/feature-extraction/%s",
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{
				Model:        "sentence-transformers/gtr-t5-xxl",
				WaitForModel: false,
				UseGPU:       false,
				UseCache:     true,
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &huggingFaceUrlBuilder{
				origin:   server.URL,
				pathMask: "/pipeline/feature-extraction/%s",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, "This is my text", ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &huggingFaceUrlBuilder{
				origin:   server.URL,
				pathMask: "/pipeline/feature-extraction/%s",
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "failed with status: 500 error: nope, not gonna happen estimated time: 20")
	})

	t.Run("when HuggingFace key is passed using X-Huggingface-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &huggingFaceUrlBuilder{
				origin:   server.URL,
				pathMask: "/pipeline/feature-extraction/%s",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Huggingface-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Model:        "sentence-transformers/gtr-t5-xxl",
				WaitForModel: true,
				UseGPU:       false,
				UseCache:     true,
			})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when HuggingFace key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &huggingFaceUrlBuilder{
				origin:   server.URL,
				pathMask: "/pipeline/feature-extraction/%s",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, "This is my text", ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "HuggingFace API Key: no api key found "+
			"neither in request header: X-HuggingFace-Api-Key "+
			"nor in environment variable under HUGGINGFACE_APIKEY")
	})

	t.Run("when X-Huggingface-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &huggingFaceUrlBuilder{
				origin:   server.URL,
				pathMask: "/pipeline/feature-extraction/%s",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Huggingface-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Model: "sentence-transformers/gtr-t5-xxl",
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "HuggingFace API Key: no api key found "+
			"neither in request header: X-HuggingFace-Api-Key "+
			"nor in environment variable under HUGGINGFACE_APIKEY")
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
			"error":          f.serverError.Error(),
			"estimated_time": 20.0,
		}
		outBytes, err := json.Marshal(embeddingError)
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

	textInputs := b["inputs"].([]interface{})
	assert.Greater(f.t, len(textInputs), 0)
	textInput := textInputs[0].(string)
	assert.Greater(f.t, len(textInput), 0)

	// TODO: fix this
	embedding := [][]float32{{0.1, 0.2, 0.3}}
	outBytes, err := json.Marshal(embedding)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
