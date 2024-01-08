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
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
)

func TestBuildUrlFn(t *testing.T) {
	t.Run("buildUrlFn returns default OpenAI Client", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Type:         "",
			Model:        "",
			ModelVersion: "",
			ResourceName: "",
			DeploymentID: "",
			BaseURL:      "https://api.openai.com",
			IsAzure:      false,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://api.openai.com/v1/embeddings", url)
	})
	t.Run("buildUrlFn returns Azure Client", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Type:         "",
			Model:        "",
			ModelVersion: "",
			ResourceName: "resourceID",
			DeploymentID: "deploymentID",
			BaseURL:      "",
			IsAzure:      true,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://resourceID.openai.azure.com/openai/deployments/deploymentID/embeddings?api-version=2022-12-01", url)
	})

	t.Run("buildUrlFn returns Azure client with BaseUrl set", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Type:         "",
			Model:        "",
			ModelVersion: "",
			ResourceName: "resourceID",
			DeploymentID: "deploymentID",
			BaseURL:      "https://foobar.some.proxy",
			IsAzure:      true,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://foobar.some.proxy/openai/deployments/deploymentID/embeddings?api-version=2022-12-01", url)
	})

	t.Run("buildUrlFn loads from BaseURL", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Type:         "",
			Model:        "",
			ModelVersion: "",
			ResourceName: "resourceID",
			DeploymentID: "deploymentID",
			BaseURL:      "https://foobar.some.proxy",
			IsAzure:      false,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://foobar.some.proxy/v1/embeddings", url)
	})
}

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		expected := &ent.VectorizationResult{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{
				Type:  "text",
				Model: "ada",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
			return server.URL, nil
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
		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		_, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: OpenAI API failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when OpenAI key is passed using X-Openai-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Type:  "text",
				Model: "ada",
			})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when OpenAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, "This is my text", ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Openai-Api-Key "+
			"nor in environment variable under OPENAI_APIKEY")
	})

	t.Run("when X-Openai-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Type:  "text",
				Model: "ada",
			})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Openai-Api-Key "+
			"nor in environment variable under OPENAI_APIKEY")
	})

	t.Run("when X-OpenAI-BaseURL header is passed", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())

		config := ent.VectorizationConfig{
			Type:    "text",
			Model:   "ada",
			BaseURL: "http://default-url.com",
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := c.buildURL(ctxWithValue, config)
		require.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com/v1/embeddings", buildURL)

		buildURL, err = c.buildURL(context.TODO(), config)
		require.NoError(t, err)
		assert.Equal(t, "http://default-url.com/v1/embeddings", buildURL)
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

func Test_getModelString(t *testing.T) {
	t.Run("getModelStringDocument", func(t *testing.T) {
		type args struct {
			docType string
			model   string
			version string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: document",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "text-search-ada-doc-001",
			},
			{
				name: "Document type: text model: ada-002 vectorizationType: document",
				args: args{
					docType: "text",
					model:   "ada",
					version: "002",
				},
				want: "text-embedding-ada-002",
			},
			{
				name: "Document type: text model: babbage vectorizationType: document",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "text-search-babbage-doc-001",
			},
			{
				name: "Document type: text model: curie vectorizationType: document",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "text-search-curie-doc-001",
			},
			{
				name: "Document type: text model: davinci vectorizationType: document",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "text-search-davinci-doc-001",
			},
			{
				name: "Document type: code model: ada vectorizationType: code",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "code-search-ada-code-001",
			},
			{
				name: "Document type: code model: babbage vectorizationType: code",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "code-search-babbage-code-001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				v := New("apiKey", "", "", 0, nullLogger())
				if got := v.getModelString(tt.args.docType, tt.args.model, "document", tt.args.version); got != tt.want {
					t.Errorf("vectorizer.getModelString() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("getModelStringQuery", func(t *testing.T) {
		type args struct {
			docType string
			model   string
			version string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: query",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "text-search-ada-query-001",
			},
			{
				name: "Document type: text model: babbage vectorizationType: query",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "text-search-babbage-query-001",
			},
			{
				name: "Document type: text model: curie vectorizationType: query",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "text-search-curie-query-001",
			},
			{
				name: "Document type: text model: davinci vectorizationType: query",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "text-search-davinci-query-001",
			},
			{
				name: "Document type: code model: ada vectorizationType: text",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "code-search-ada-text-001",
			},
			{
				name: "Document type: code model: babbage vectorizationType: text",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "code-search-babbage-text-001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				v := New("apiKey", "", "", 0, nullLogger())
				if got := v.getModelString(tt.args.docType, tt.args.model, "query", tt.args.version); got != tt.want {
					t.Errorf("vectorizer.getModelString() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}

func TestOpenAIApiErrorDecode(t *testing.T) {
	t.Run("getModelStringQuery", func(t *testing.T) {
		type args struct {
			response []byte
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Error code: missing property",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg..."}`),
				},
				want: "",
			},
			{
				name: "Error code: as int",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": 500}`),
				},
				want: "500",
			},
			{
				name: "Error code as string",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "500"}`),
				},
				want: "500",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var got *openAIApiError
				err := json.Unmarshal(tt.args.response, &got)
				require.NoError(t, err)

				if got.Code.String() != tt.want {
					t.Errorf("OpenAIerror.code = %v, want %v", got.Code, tt.want)
				}
			})
		}
	})
}
