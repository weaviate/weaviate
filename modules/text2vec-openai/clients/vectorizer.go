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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
)

type embeddingsRequest struct {
	Input []string `json:"input"`
	Model string   `json:"model,omitempty"`
}

type embedding struct {
	Object string          `json:"object"`
	Data   []embeddingData `json:"data,omitempty"`
	Error  *openAIApiError `json:"error,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

type openAIApiError struct {
	Message string      `json:"message"`
	Type    string      `json:"type"`
	Param   string      `json:"param"`
	Code    json.Number `json:"code"`
}

func buildUrl(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
	if isAzure {
		host := baseURL
		if host == "" || host == "https://api.openai.com" {
			// Fall back to old assumption
			host = "https://" + resourceName + ".openai.azure.com"
		}

		path := "openai/deployments/" + deploymentID + "/embeddings"
		queryParam := "api-version=2022-12-01"
		return fmt.Sprintf("%s/%s?%s", host, path, queryParam), nil
	}

	host := baseURL
	path := "/v1/embeddings"
	return url.JoinPath(host, path)
}

type vectorizer struct {
	openAIApiKey       string
	openAIOrganization string
	azureApiKey        string
	httpClient         *http.Client
	buildUrlFn         func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error)
	logger             logrus.FieldLogger
}

func New(openAIApiKey, openAIOrganization, azureApiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		openAIApiKey:       openAIApiKey,
		openAIOrganization: openAIOrganization,
		azureApiKey:        azureApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn: buildUrl,
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, []string{input}, v.getModelString(config.Type, config.Model, "document", config.ModelVersion), config)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.getModelString(config.Type, config.Model, "query", config.ModelVersion), config)
}

func (v *vectorizer) vectorize(ctx context.Context, input []string, model string, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(v.getEmbeddingsRequest(input, model, config.IsAzure))
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	endpoint, err := v.buildURL(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "join OpenAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return nil, errors.Wrap(err, "API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey, config.IsAzure))
	if openAIOrganization := v.getOpenAIOrganization(ctx); openAIOrganization != "" {
		req.Header.Add("OpenAI-Organization", openAIOrganization)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody embedding
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, resBody.Error, config.IsAzure)
	}

	texts := make([]string, len(resBody.Data))
	embeddings := make([][]float32, len(resBody.Data))
	for i := range resBody.Data {
		texts[i] = resBody.Data[i].Object
		embeddings[i] = resBody.Data[i].Embedding
	}

	return &ent.VectorizationResult{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
	}, nil
}

func (v *vectorizer) buildURL(ctx context.Context, config ent.VectorizationConfig) (string, error) {
	baseURL, resourceName, deploymentID, isAzure := config.BaseURL, config.ResourceName, config.DeploymentID, config.IsAzure
	if headerBaseURL := v.getValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}
	return v.buildUrlFn(baseURL, resourceName, deploymentID, isAzure)
}

func (v *vectorizer) getError(statusCode int, resBodyError *openAIApiError, isAzure bool) error {
	endpoint := "OpenAI API"
	if isAzure {
		endpoint = "Azure OpenAI API"
	}
	if resBodyError != nil {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBodyError.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *vectorizer) getEmbeddingsRequest(input []string, model string, isAzure bool) embeddingsRequest {
	if isAzure {
		return embeddingsRequest{Input: input}
	}
	return embeddingsRequest{Input: input, Model: model}
}

func (v *vectorizer) getApiKeyHeaderAndValue(apiKey string, isAzure bool) (string, string) {
	if isAzure {
		return "api-key", apiKey
	}
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *vectorizer) getOpenAIOrganization(ctx context.Context) string {
	if value := v.getValueFromContext(ctx, "X-Openai-Organization"); value != "" {
		return value
	}
	return v.openAIOrganization
}

func (v *vectorizer) getApiKey(ctx context.Context, isAzure bool) (string, error) {
	var apiKey, envVar string

	if isAzure {
		apiKey = "X-Azure-Api-Key"
		envVar = "AZURE_APIKEY"
		if len(v.azureApiKey) > 0 {
			return v.azureApiKey, nil
		}
	} else {
		apiKey = "X-Openai-Api-Key"
		envVar = "OPENAI_APIKEY"
		if len(v.openAIApiKey) > 0 {
			return v.openAIApiKey, nil
		}
	}

	return v.getApiKeyFromContext(ctx, apiKey, envVar)
}

func (v *vectorizer) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *vectorizer) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if apiKey := modulecomponents.GetValueFromGRPC(ctx, key); len(apiKey) > 0 && len(apiKey[0]) > 0 {
		return apiKey[0]
	}

	return ""
}

func (v *vectorizer) getModelString(docType, model, action, version string) string {
	if version == "002" {
		return v.getModel002String(model)
	}

	return v.getModel001String(docType, model, action)
}

func (v *vectorizer) getModel001String(docType, model, action string) string {
	modelBaseString := "%s-search-%s-%s-001"
	if action == "document" {
		if docType == "code" {
			return fmt.Sprintf(modelBaseString, docType, model, "code")
		}
		return fmt.Sprintf(modelBaseString, docType, model, "doc")

	} else {
		if docType == "code" {
			return fmt.Sprintf(modelBaseString, docType, model, "text")
		}
		return fmt.Sprintf(modelBaseString, docType, model, "query")
	}
}

func (v *vectorizer) getModel002String(model string) string {
	modelBaseString := "text-embedding-%s-002"
	return fmt.Sprintf(modelBaseString, model)
}
