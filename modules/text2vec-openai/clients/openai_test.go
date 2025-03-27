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
			ApiVersion:   "2022-12-01",
			BaseURL:      "https://api.openai.com",
			IsAzure:      false,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.ApiVersion, config.IsAzure)
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
			ApiVersion:   "2022-12-01",
			BaseURL:      "",
			IsAzure:      true,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.ApiVersion, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://resourceID.openai.azure.com/openai/deployments/deploymentID/embeddings?api-version=2022-12-01", url)
	})

	t.Run("buildUrlFn returns Azure Client with custom API Version", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Type:         "",
			Model:        "",
			ModelVersion: "",
			ResourceName: "resourceID",
			DeploymentID: "deploymentID",
			ApiVersion:   "2024-02-01",
			BaseURL:      "",
			IsAzure:      true,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.ApiVersion, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://resourceID.openai.azure.com/openai/deployments/deploymentID/embeddings?api-version=2024-02-01", url)
	})

	t.Run("buildUrlFn returns Azure client with BaseUrl set", func(t *testing.T) {
		config := ent.VectorizationConfig{
			Type:         "",
			Model:        "",
			ModelVersion: "",
			ResourceName: "resourceID",
			DeploymentID: "deploymentID",
			ApiVersion:   "2022-12-01",
			BaseURL:      "https://foobar.some.proxy",
			IsAzure:      true,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.ApiVersion, config.IsAzure)
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
			ApiVersion:   "2022-12-01",
			BaseURL:      "https://foobar.some.proxy",
			IsAzure:      false,
		}
		url, err := buildUrl(config.BaseURL, config.ResourceName, config.DeploymentID, config.ApiVersion, config.IsAzure)
		assert.Nil(t, err)
		assert.Equal(t, "https://foobar.some.proxy/v1/embeddings", url)
	})
}

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
			Errors:     []error{nil},
		}
		res, rl, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)

		assert.Equal(t, false, rl.UpdateWithMissingValues)
		assert.Equal(t, 100, rl.RemainingTokens)
		assert.Equal(t, 100, rl.RemainingRequests)
		assert.Equal(t, 100, rl.LimitTokens)
		assert.Equal(t, 100, rl.LimitRequests)
	})

	t.Run("when rate limit values are missing", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t, noRlHeader: true})
		defer server.Close()

		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
			Errors:     []error{nil},
		}
		res, rl, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)

		assert.Equal(t, true, rl.UpdateWithMissingValues)
		assert.Equal(t, -1, rl.RemainingTokens)
		assert.Equal(t, -1, rl.RemainingRequests)
		assert.Equal(t, -1, rl.LimitTokens)
		assert.Equal(t, -1, rl.LimitRequests)
	})

	t.Run("when rate limit values are returned but are bad values", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t, noRlHeader: false, RlValues: "0"})
		defer server.Close()

		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		expected := &modulecomponents.VectorizationResult[[]float32]{
			Text:       []string{"This is my text"},
			Vector:     [][]float32{{0.1, 0.2, 0.3}},
			Dimensions: 3,
			Errors:     []error{nil},
		}
		res, rl, _, err := c.Vectorize(context.Background(), []string{"This is my text"}, fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)

		assert.Equal(t, true, rl.UpdateWithMissingValues)
		assert.Equal(t, 0, rl.RemainingTokens)
		assert.Equal(t, 0, rl.RemainingRequests)
		assert.Equal(t, 0, rl.LimitTokens)
		assert.Equal(t, 0, rl.LimitRequests)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
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
		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"},
			fakeClassConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: OpenAI API failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when the server returns an error with request id", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:               t,
			serverError:     errors.Errorf("nope, not gonna happen"),
			headerRequestID: "some-request-id",
		})
		defer server.Close()
		c := New("apiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		_, _, _, err := c.Vectorize(context.Background(), []string{"This is my text"},
			fakeClassConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to: OpenAI API failed with status: 500 request-id: some-request-id error: nope, not gonna happen")
	})

	t.Run("when OpenAI key is passed using X-Openai-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Api-Key", []string{"some-key"})

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

	t.Run("when OpenAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, _, _, err := c.Vectorize(ctx, []string{"This is my text"}, fakeClassConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Openai-Api-Key "+
			"nor in environment variable under OPENAI_APIKEY")
	})

	t.Run("when X-Openai-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
			return server.URL, nil
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Api-Key", []string{""})

		_, _, _, err := c.Vectorize(ctxWithValue, []string{"This is my text"},
			fakeClassConfig{classConfig: map[string]interface{}{"Type": "text", "Model": "ada"}})

		require.NotNil(t, err)
		assert.EqualError(t, err, "API Key: no api key found "+
			"neither in request header: X-Openai-Api-Key "+
			"nor in environment variable under OPENAI_APIKEY")
	})

	t.Run("when X-OpenAI-BaseURL header is passed", func(t *testing.T) {
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

	t.Run("when X-Azure-* headers are passed", func(t *testing.T) {
		c := New("", "", "", 0, nullLogger())

		config := ent.VectorizationConfig{
			IsAzure:    true,
			ApiVersion: "",
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Azure-Deployment-Id", []string{"spoofDeployment"})
		ctxWithValue = context.WithValue(ctxWithValue,
			"X-Azure-Resource-Name", []string{"spoofResource"})

		buildURL, err := c.buildURL(ctxWithValue, config)
		require.NoError(t, err)
		assert.Equal(t, "https://spoofResource.openai.azure.com/openai/deployments/spoofDeployment/embeddings?api-version=", buildURL)
	})

	t.Run("pass rate limit headers requests", func(t *testing.T) {
		c := New("", "", "", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Ratelimit-RequestPM-Embedding", []string{"50"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{})
		assert.Equal(t, 50, rl.LimitRequests)
		assert.Equal(t, 50, rl.RemainingRequests)
	})

	t.Run("pass rate limit headers tokens", func(t *testing.T) {
		c := New("", "", "", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(), "X-Openai-Ratelimit-TokenPM-Embedding", []string{"60"})

		rl := c.GetVectorizerRateLimit(ctxWithValue, fakeClassConfig{})
		assert.Equal(t, 60, rl.LimitTokens)
		assert.Equal(t, 60, rl.RemainingTokens)
	})
}

type fakeHandler struct {
	t               *testing.T
	serverError     error
	headerRequestID string
	noRlHeader      bool
	RlValues        string
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

		if f.headerRequestID != "" {
			w.Header().Add("x-request-id", f.headerRequestID)
		}
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

	if !f.noRlHeader {
		rlValues := f.RlValues
		if f.RlValues == "" {
			rlValues = "100"
		}
		w.Header().Add("x-ratelimit-limit-requests", rlValues)
		w.Header().Add("x-ratelimit-limit-tokens", rlValues)
		w.Header().Add("x-ratelimit-remaining-requests", rlValues)
		w.Header().Add("x-ratelimit-remaining-tokens", rlValues)
	}

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
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
				name: "Error code as string number",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "500"}`),
				},
				want: "500",
			},
			{
				name: "Error code as string text",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "invalid_api_key"}`),
				},
				want: "invalid_api_key",
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
