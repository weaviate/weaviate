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
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/cohere"

	"github.com/pkg/errors"
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
			client: cohere.New("apiKey", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, cfg)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			client: cohere.New("apiKey", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, cfg)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		c := &vectorizer{
			client: cohere.New("apiKey", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}
		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, cfg)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "connection to Cohere failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when Cohere key is passed using X-Cohere-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			client: cohere.New("apiKey", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, cfg)

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when Cohere key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			client: cohere.New("", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, cfg)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Cohere API Key: no api key found "+
			"neither in request header: X-Cohere-Api-Key "+
			"nor in environment variable under COHERE_APIKEY")
	})

	t.Run("when X-Cohere-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			client: cohere.New("", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}
		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Api-Key", []string{""})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, cfg)

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "Cohere API Key: no api key found "+
			"neither in request header: X-Cohere-Api-Key "+
			"nor in environment variable under COHERE_APIKEY")
	})

	t.Run("pass rate limit headers requests", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			client: cohere.New("apiKey", 1*time.Minute, nullLogger()),
			logger: nullLogger(),
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Ratelimit-RequestPM-Embedding", []string{"50"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{classConfig: map[string]interface{}{}})
		assert.Equal(t, 50, rl.LimitRequests)
		assert.Equal(t, 50, rl.RemainingRequests)
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

	textInput := b["texts"].([]interface{})
	assert.Greater(f.t, len(textInput), 0)

	embeddingResponse := map[string]interface{}{
		"embeddings": map[string]interface{}{
			"float": [][]float32{{0.1, 0.2, 0.3}},
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

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	if propName == f.skippedProperty {
		return map[string]interface{}{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]interface{}{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]interface{}{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}
