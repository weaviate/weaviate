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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/voyageai"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{voyageai.New("apiKey", 0, &voyageaiUrlBuilder{origin: server.URL, pathMask: "/multimodalembeddings"}, nullLogger())}
		expected := &modulecomponents.VectorizationCLIPResult[[]float32]{
			TextVectors:  [][]float32{{0.1, 0.2, 0.3}},
			ImageVectors: [][]float32{{0.4, 0.5, 0.6}},
		}
		res, err := c.Vectorize(context.Background(),
			[]string{"This is my text"}, []string{"base64image"},
			fakeClassConfig{classConfig: map[string]interface{}{"Model": "voyage-multimodal-3", "baseURL": server.URL}},
		)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{voyageai.New("apiKey", 0, &voyageaiUrlBuilder{origin: server.URL, pathMask: "/multimodalembeddings"}, nullLogger())}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"text"}, []string{"image"},
			fakeClassConfig{classConfig: map[string]interface{}{"Model": "voyage-multimodal-3"}},
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: "nope, not gonna happen",
		})
		defer server.Close()
		c := &vectorizer{voyageai.New("apiKey", 0, &voyageaiUrlBuilder{origin: server.URL, pathMask: "/multimodalembeddings"}, nullLogger())}
		_, err := c.Vectorize(context.Background(), []string{"text"}, []string{"image"},
			fakeClassConfig{classConfig: map[string]interface{}{"Model": "voyage-multimodal-3", "baseURL": server.URL}},
		)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "nope, not gonna happen")
	})

	t.Run("when VoyageAI key is passed using header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{voyageai.New("apiKey", 0, &voyageaiUrlBuilder{origin: server.URL, pathMask: "/multimodalembeddings"}, nullLogger())}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Voyageai-Api-Key", []string{"some-key"})

		expected := &modulecomponents.VectorizationCLIPResult[[]float32]{
			TextVectors:  [][]float32{{0.1, 0.2, 0.3}},
			ImageVectors: [][]float32{{0.4, 0.5, 0.6}},
		}
		res, err := c.Vectorize(ctxWithValue, []string{"text"}, []string{"image"},
			fakeClassConfig{classConfig: map[string]interface{}{"Model": "voyage-multimodal-3", "baseURL": server.URL}},
		)

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError string
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != "" {
		resp := map[string]interface{}{
			"detail": f.serverError,
		}
		outBytes, err := json.Marshal(resp)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var req map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))
	assert.NotNil(f.t, req)

	resp := map[string]interface{}{
		"data": []map[string]interface{}{
			{"embedding": []float32{0.1, 0.2, 0.3}},
			{"embedding": []float32{0.4, 0.5, 0.6}},
		},
	}

	outBytes, err := json.Marshal(resp)
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

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
