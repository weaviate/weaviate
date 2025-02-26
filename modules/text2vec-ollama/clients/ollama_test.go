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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &ollama{
			httpClient: &http.Client{},
			urlBuilderFn: func(apiEndpoint string) string {
				assert.Equal(t, "endpoint", apiEndpoint)
				return server.URL
			},
			logger: nullLogger(),
		}
		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		cfg := fakeClassConfig{
			classConfig: map[string]interface{}{
				"apiEndpoint": "endpoint",
				"model":       "future-text-embed",
			},
		}
		res, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, cfg)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &ollama{
			httpClient: &http.Client{},
			urlBuilderFn: func(apiEndpoint string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
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
		c := &ollama{
			httpClient: &http.Client{},
			urlBuilderFn: func(apiEndpoint string) string {
				return server.URL
			},
			logger: nullLogger(),
		}
		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to Ollama API failed with error: nope, not gonna happen")
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
			Error: f.serverError.Error(),
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

	embeddingResponse := &embeddingsResponse{
		Embeddings: [][]float32{{0.1, 0.2, 0.3}},
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
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	apiEndpoint           string
	modelID               string
	properties            interface{}
}

func (f fakeClassConfig) Class() map[string]interface{} {
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
	}
	if f.apiEndpoint != "" {
		classSettings["apiEndpoint"] = f.apiEndpoint
	}
	if f.modelID != "" {
		classSettings["modelID"] = f.modelID
	}
	if f.properties != nil {
		classSettings["properties"] = f.properties
	}
	for k, v := range f.classConfig {
		classSettings[k] = v
	}
	return classSettings
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.Class()
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

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
