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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

func TestClient(t *testing.T) {
	t.Run("when embedding text and image", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKey", 0, nullLogger())

		expected := &modulecomponents.VectorizationCLIPResult[[]float32]{
			TextVectors:  [][]float32{{0.1, 0.2, 0.3}},
			ImageVectors: [][]float32{{0.4, 0.5, 0.6}},
		}
		res, err := c.Vectorize(context.Background(),
			[]string{"This is my text"}, []string{base64.StdEncoding.EncodeToString([]byte("fake-image-bytes"))},
			fakeClassConfig{classConfig: map[string]any{"baseURL": server.URL}},
		)

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the request sends correct multipart fields", func(t *testing.T) {
		handler := &fakeHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := New("apiKey", 0, nullLogger())

		_, err := c.VectorizeQuery(context.Background(), []string{"a query"},
			fakeClassConfig{classConfig: map[string]any{"baseURL": server.URL}},
		)

		require.Nil(t, err)
		assert.Equal(t, "marengo3.0", handler.gotModelName)
		assert.Equal(t, "a query", handler.gotText)
		assert.Equal(t, "apiKey", handler.gotAPIKey)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKey", 0, nullLogger())
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"text"}, nil,
			fakeClassConfig{classConfig: map[string]any{"baseURL": server.URL}},
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t, serverError: "api_key_invalid"})
		defer server.Close()
		c := New("apiKey", 0, nullLogger())

		_, err := c.Vectorize(context.Background(), []string{"text"}, nil,
			fakeClassConfig{classConfig: map[string]any{"baseURL": server.URL}},
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "api_key_invalid")
	})

	t.Run("when the API key is passed via header", func(t *testing.T) {
		handler := &fakeHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := New("", 0, nullLogger())
		ctx := context.WithValue(context.Background(),
			"X-Twelvelabs-Api-Key", []string{"header-key"})

		_, err := c.Vectorize(ctx, []string{"text"}, nil,
			fakeClassConfig{classConfig: map[string]any{"baseURL": server.URL}},
		)

		require.Nil(t, err)
		assert.Equal(t, "header-key", handler.gotAPIKey)
	})

	t.Run("when no API key is configured", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		_, err := c.Vectorize(context.Background(), []string{"text"}, nil,
			fakeClassConfig{classConfig: map[string]any{"baseURL": server.URL}},
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "no api key found")
	})
}

// TestClient_Live exercises the real Marengo Embed API and asserts a 512-dim
// text embedding. It is skipped unless TWELVELABS_APIKEY is set.
func TestClient_Live(t *testing.T) {
	apiKey := os.Getenv("TWELVELABS_APIKEY")
	if apiKey == "" {
		t.Skip("set TWELVELABS_APIKEY to run the live TwelveLabs embedding test")
	}

	c := New(apiKey, 30*time.Second, nullLogger())
	res, err := c.VectorizeQuery(context.Background(),
		[]string{"a dog running on the beach"},
		fakeClassConfig{classConfig: map[string]any{}},
	)
	require.Nil(t, err)
	require.Len(t, res.TextVectors, 1)
	assert.Len(t, res.TextVectors[0], 512)
}

type fakeHandler struct {
	t            *testing.T
	serverError  string
	gotModelName string
	gotText      string
	gotImage     bool
	gotAPIKey    string
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)
	f.gotAPIKey = r.Header.Get("x-api-key")

	require.Nil(f.t, r.ParseMultipartForm(1<<20))
	f.gotModelName = r.FormValue("model_name")
	f.gotText = r.FormValue("text")
	if file, _, err := r.FormFile("image_file"); err == nil {
		f.gotImage = true
		file.Close()
	}

	w.Header().Set("Content-Type", "application/json")
	if f.serverError != "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(embedResponse{Code: "error", Message: f.serverError})
		return
	}

	res := embedResponse{ModelName: "marengo3.0"}
	if f.gotImage {
		res.ImageEmbedding = &embedding{Segments: []segment{{Float: []float32{0.4, 0.5, 0.6}}}}
	} else {
		res.TextEmbedding = &embedding{Segments: []segment{{Float: []float32{0.1, 0.2, 0.3}}}}
	}
	json.NewEncoder(w).Encode(res)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

type fakeClassConfig struct {
	classConfig map[string]any
}

func (f fakeClassConfig) Class() map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}
