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
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New(0)
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})

		res, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New(0)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		ctx = context.WithValue(ctx, "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when vectorizer fails", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := New(0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"baseURL": server.URL}})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "text2vec-weaviate module: Weaviate embed API error: 500 ")
	})

	t.Run("when using custom dimensions", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})

		defer server.Close()
		c := New(0)
		ctx := context.WithValue(context.Background(), "Authorization", []string{"token"})
		ctx = context.WithValue(ctx, "X-Weaviate-Cluster-Url", []string{server.URL})

		dims := int64(16)
		cfg := &fakeClassConfig{
			classConfig: map[string]interface{}{
				"baseURL":    server.URL,
				"dimensions": dims,
			},
		}

		result, _, _, _ := c.Vectorize(ctx, []string{"This is my text"}, cfg)

		require.NotNil(t, result.Dimensions)
		require.Equal(t, int(16), result.Dimensions)
		require.Equal(t, []string{"This is my text"}, result.Text)
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
		embeddingResponse := map[string]interface{}{
			"message": embeddingError["message"],
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

	var b map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	textInput := b["input"].([]interface{})
	assert.Greater(f.t, len(textInput), 0)

	embeddingLen := 3 // default dimension
	if dims, ok := b["dimensions"].(float64); ok {
		embeddingLen = int(dims)
	}

	embeddings := make([]float32, embeddingLen)
	for i := 0; i < embeddingLen; i++ {
		embeddings[i] = float32(0.1 * float32(i+1))
	}

	embeddingResponse := map[string]interface{}{
		"embeddings": [][]float32{embeddings},
	}
	outBytes, err := json.Marshal(embeddingResponse)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
