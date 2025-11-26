//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	defaultSettings := func(url string) fakeClassConfig {
		return fakeClassConfig{classConfig: map[string]interface{}{"Model": "test-model", "baseURL": url}}
	}

	t.Run("when all is fine and we send image", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New(0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{"cluster-url"})

		expected := &modulecomponents.VectorizationCLIPResult[[][]float32]{
			TextVectors:  [][][]float32{},
			ImageVectors: [][][]float32{{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}}},
		}
		res, err := c.Vectorize(ctx, []string{}, []string{"base64"}, defaultSettings(server.URL))

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when all is fine and we send text query", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New(0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{"cluster-url"})

		expected := &modulecomponents.VectorizationCLIPResult[[][]float32]{
			TextVectors:  [][][]float32{{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}}},
			ImageVectors: [][][]float32{},
		}
		res, err := c.VectorizeQuery(ctx, []string{"text query"}, defaultSettings(server.URL))

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := New(0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{"cluster-url"})

		_, err := c.Vectorize(ctx, []string{}, []string{"base64"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "multi2multivec-weaviate module: Weaviate embed API error: 500 nope, not gonna happen")
	})

	t.Run("fails when vectorizing text data", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New(0)

		_, err := c.Vectorize(context.Background(), []string{"text data"}, []string{"base64"}, defaultSettings(server.URL))

		assert.EqualError(t, err, "vectorizing text data is not supported for multi2multivec-weaviate module")
	})

	t.Run("fails when vectorizing image queries", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New(0)

		_, err := c.VectorizeImages(context.Background(), []string{"base64"}, defaultSettings(server.URL))

		assert.EqualError(t, err, "vectorizing image queries is not supported for multi2multivec-weaviate module")
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

	embeddingResponse := map[string]interface{}{
		"embeddings": [][][]float32{{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}}},
	}
	outBytes, err := json.Marshal(embeddingResponse)
	require.Nil(f.t, err)

	w.Write(outBytes)
}
