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
	"github.com/weaviate/weaviate/modules/text2vec-databricks/ent"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("databricksToken", 0, nullLogger())
		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Endpoint", []string{server.URL})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
			Errors:     []error{nil},
		}
		res, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("databricksToken", 0, nullLogger())
		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Endpoint", []string{server.URL})

		ctx, cancel := context.WithDeadline(ctxWithValue, time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := New("databricksToken", 0, nullLogger())
		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Endpoint", []string{server.URL})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			fakeClassConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: Databricks Foundation Model API failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when Databricks token is passed using X-Databricks-Token header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Token", []string{"some-key"})

		ctxWithValue = context.WithValue(ctxWithValue,
			"X-Databricks-Endpoint", []string{server.URL})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
			Errors:     []error{nil},
		}
		res, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when Databricks token is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no Databricks token found "+
			"neither in request header: X-Databricks-Token "+
			"nor in environment variable under DATABRICKS_TOKEN")
	})

	t.Run("when X-Databricks-Token header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Token", []string{""})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no Databricks token found "+
			"neither in request header: X-Databricks-Token "+
			"nor in environment variable under DATABRICKS_TOKEN")
	})

	t.Run("when X-Databricks-Endpoint header is passed", func(t *testing.T) {
		c := New("", 0, nullLogger())

		config := ent.VectorizationConfig{
			Endpoint: "http://serving-url-in-config.com",
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Endpoint", []string{"http://serving-url-passed-in-header.com"})

		endpoint, err := c.buildURL(ctxWithValue, config)
		require.NoError(t, err)
		assert.Equal(t, "http://serving-url-passed-in-header.com", endpoint)

		endpoint, err = c.buildURL(context.TODO(), config)
		require.NoError(t, err)
		assert.Equal(t, "http://serving-url-in-config.com", endpoint)
	})

	t.Run("pass rate limit headers requests", func(t *testing.T) {
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Ratelimit-RequestPM-Embedding", []string{"50"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{})
		assert.Equal(t, 50, rl.LimitRequests)
		assert.Equal(t, 50, rl.RemainingRequests)
	})

	t.Run("pass rate limit headers tokens", func(t *testing.T) {
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(), "X-Databricks-Ratelimit-TokenPM-Embedding", []string{"60"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{})
		assert.Equal(t, 60, rl.LimitTokens)
		assert.Equal(t, 60, rl.RemainingTokens)
	})

	t.Run("when X-Databricks-User-Agent header is passed", func(t *testing.T) {
		userAgent := "weaviate+spark_connector"
		server := httptest.NewServer(&fakeHandler{t: t, userAgent: userAgent})
		defer server.Close()
		c := New("", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Token", []string{"some-key"})

		ctxWithValue = context.WithValue(ctxWithValue,
			"X-Databricks-Endpoint", []string{server.URL})

		ctxWithValue = context.WithValue(ctxWithValue,
			"X-Databricks-User-Agent", []string{userAgent})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		require.NoError(t, err)
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
	userAgent   string
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.userAgent != "" {
		assert.Equal(f.t, f.userAgent, r.UserAgent())
	}
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
