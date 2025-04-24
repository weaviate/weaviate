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

func TestGetApiKeyFromContext(t *testing.T) {
	t.Run("value from context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"key-from-ctx"})
		c := New("", "", "", 0, nullLogger())
		key, err := c.getApiKey(ctx, false)
		require.NoError(t, err)
		assert.Equal(t, "key-from-ctx", key)
	})

	t.Run("value from env fallback", func(t *testing.T) {
		ctx := context.Background()
		c := New("env-key", "", "", 0, nullLogger())
		key, err := c.getApiKey(ctx, false)
		require.NoError(t, err)
		assert.Equal(t, "env-key", key)
	})

	t.Run("no value at all", func(t *testing.T) {
		ctx := context.Background()
		c := New("", "", "", 0, nullLogger())
		_, err := c.getApiKey(ctx, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no api key found")
	})
}

func TestGetOpenAIOrganization(t *testing.T) {
	t.Run("from context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "X-Openai-Organization", []string{"from-context"})
		c := New("", "default-org", "", 0, nullLogger())
		assert.Equal(t, "from-context", c.getOpenAIOrganization(ctx))
	})

	t.Run("from default", func(t *testing.T) {
		ctx := context.Background()
		c := New("", "default-org", "", 0, nullLogger())
		assert.Equal(t, "default-org", c.getOpenAIOrganization(ctx))
	})
}

func TestGetEmbeddingsRequest(t *testing.T) {
	t.Run("Azure true omits model", func(t *testing.T) {
		c := New("", "", "", 0, nullLogger())
		req := c.getEmbeddingsRequest([]string{"foo"}, "model", true, nil)
		assert.Equal(t, []string{"foo"}, req.Input)
		assert.Equal(t, (*int64)(nil), req.Dimensions)
		assert.Empty(t, req.Model)
	})
	t.Run("Non-Azure includes model", func(t *testing.T) {
		c := New("", "", "", 0, nullLogger())
		dim := int64(42)
		req := c.getEmbeddingsRequest([]string{"foo"}, "model", false, &dim)
		assert.Equal(t, []string{"foo"}, req.Input)
		assert.Equal(t, "model", req.Model)
		assert.Equal(t, &dim, req.Dimensions)
	})
}

func TestGetApiKeyHeaderAndValue(t *testing.T) {
	c := New("", "", "", 0, nullLogger())
	h, v := c.getApiKeyHeaderAndValue("some-key", true)
	assert.Equal(t, "api-key", h)
	assert.Equal(t, "some-key", v)

	h, v = c.getApiKeyHeaderAndValue("other-key", false)
	assert.Equal(t, "Authorization", h)
	assert.Equal(t, "Bearer other-key", v)
}

func TestGetApiKeyHash(t *testing.T) {
	c := New("super-secret", "", "", 0, nullLogger())
	hash := c.GetApiKeyHash(context.Background(), fakeClassConfig{})
	assert.NotEqual(t, [32]byte{}, hash)
	assert.Equal(t, hash, c.GetApiKeyHash(context.Background(), fakeClassConfig{}))
}

func TestGetErrorFormat(t *testing.T) {
	c := New("", "", "", 0, nullLogger())
	err := c.getError(403, "abc-123", &openAIApiError{Message: "denied"}, false)
	assert.Contains(t, err.Error(), "403")
	assert.Contains(t, err.Error(), "abc-123")
	assert.Contains(t, err.Error(), "denied")
}

func TestOpenAIApiErrorDecode(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		expected string
	}{
		{"missing code", `{"message": "fail", "type": "err", "param": "x"}`, ""},
		{"numeric code", `{"message": "fail", "type": "err", "param": "x", "code": 500}`, "500"},
		{"string number", `{"message": "fail", "type": "err", "param": "x", "code": "500"}`, "500"},
		{"string literal", `{"message": "fail", "type": "err", "param": "x", "code": "invalid_key"}`, "invalid_key"},
		{"empty string", `{"message": "fail", "type": "err", "param": "x", "code": ""}`, ""},
		{"null code", `{"message": "fail", "type": "err", "param": "x", "code": null}`, ""},
		{"code as boolean (invalid)", `{"message": "fail", "type": "err", "param": "x", "code": true}`, ""},
		{"code as array (invalid)", `{"message": "fail", "type": "err", "param": "x", "code": ["bad"]}`, ""},
		{"code as object (invalid)", `{"message": "fail", "type": "err", "param": "x", "code": {"key": "val"}}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *openAIApiError
			err := json.Unmarshal([]byte(tt.payload), &got)
			if err != nil && tt.expected != "" {
				t.Errorf("unexpected unmarshal error: %v", err)
				return
			}
			if got != nil && got.Code.String() != tt.expected {
				t.Errorf("got code %q, expected %q", got.Code.String(), tt.expected)
			}
		})
	}
}
