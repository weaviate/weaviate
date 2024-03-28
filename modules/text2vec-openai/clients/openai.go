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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
)

type embeddingsRequest struct {
	Input      []string `json:"input"`
	Model      string   `json:"model,omitempty"`
	Dimensions *int64   `json:"dimensions,omitempty"`
}

type embedding struct {
	Object string          `json:"object"`
	Data   []embeddingData `json:"data,omitempty"`
	Error  *openAIApiError `json:"error,omitempty"`
}

type embeddingData struct {
	Object    string          `json:"object"`
	Index     int             `json:"index"`
	Embedding []float32       `json:"embedding"`
	Error     *openAIApiError `json:"error,omitempty"`
}

type openAIApiError struct {
	Message string     `json:"message"`
	Type    string     `json:"type"`
	Param   string     `json:"param"`
	Code    openAICode `json:"code"`
}

type openAICode string

func (c *openAICode) String() string {
	if c == nil {
		return ""
	}
	return string(*c)
}

func (c *openAICode) UnmarshalJSON(data []byte) (err error) {
	if number, err := strconv.Atoi(string(data)); err == nil {
		str := strconv.Itoa(number)
		*c = openAICode(str)
		return nil
	}
	var str string
	err = json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*c = openAICode(str)
	return nil
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

type client struct {
	openAIApiKey       string
	openAIOrganization string
	azureApiKey        string
	httpClient         *http.Client
	buildUrlFn         func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error)
	logger             logrus.FieldLogger
}

func New(openAIApiKey, openAIOrganization, azureApiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
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

func (v *client) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	config := v.getVectorizationConfig(cfg)
	return v.vectorize(ctx, input, v.getModelString(config.Type, config.Model, "document", config.ModelVersion), config)
}

func (v *client) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	config := v.getVectorizationConfig(cfg)
	res, _, err := v.vectorize(ctx, input, v.getModelString(config.Type, config.Model, "query", config.ModelVersion), config)
	return res, err
}

func (v *client) vectorize(ctx context.Context, input []string, model string, config ent.VectorizationConfig) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	body, err := json.Marshal(v.getEmbeddingsRequest(input, model, config.IsAzure, config.Dimensions))
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshal body")
	}

	endpoint, err := v.buildURL(ctx, config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "join OpenAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return nil, nil, errors.Wrap(err, "API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey, config.IsAzure))
	if openAIOrganization := v.getOpenAIOrganization(ctx); openAIOrganization != "" {
		req.Header.Add("OpenAI-Organization", openAIOrganization)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, errors.Wrap(err, "read response body")
	}

	var resBody embedding
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, nil, v.getError(res.StatusCode, resBody.Error, config.IsAzure)
	}
	rateLimit := ent.GetRateLimitsFromHeader(res.Header)

	texts := make([]string, len(resBody.Data))
	embeddings := make([][]float32, len(resBody.Data))
	openAIerror := make([]error, len(resBody.Data))
	for i := range resBody.Data {
		texts[i] = resBody.Data[i].Object
		embeddings[i] = resBody.Data[i].Embedding
		if resBody.Data[i].Error != nil {
			openAIerror[i] = v.getError(res.StatusCode, resBody.Data[i].Error, config.IsAzure)
		}
	}

	return &modulecomponents.VectorizationResult{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
		Errors:     openAIerror,
	}, rateLimit, nil
}

func (v *client) buildURL(ctx context.Context, config ent.VectorizationConfig) (string, error) {
	baseURL, resourceName, deploymentID, isAzure := config.BaseURL, config.ResourceName, config.DeploymentID, config.IsAzure
	if headerBaseURL := v.getValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}
	return v.buildUrlFn(baseURL, resourceName, deploymentID, isAzure)
}

func (v *client) getError(statusCode int, resBodyError *openAIApiError, isAzure bool) error {
	endpoint := "OpenAI API"
	if isAzure {
		endpoint = "Azure OpenAI API"
	}
	if resBodyError != nil {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBodyError.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *client) getEmbeddingsRequest(input []string, model string, isAzure bool, dimensions *int64) embeddingsRequest {
	if isAzure {
		return embeddingsRequest{Input: input}
	}
	return embeddingsRequest{Input: input, Model: model, Dimensions: dimensions}
}

func (v *client) getApiKeyHeaderAndValue(apiKey string, isAzure bool) (string, string) {
	if isAzure {
		return "api-key", apiKey
	}
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *client) getOpenAIOrganization(ctx context.Context) string {
	if value := v.getValueFromContext(ctx, "X-Openai-Organization"); value != "" {
		return value
	}
	return v.openAIOrganization
}

func (v *client) getRateLimit(ctx context.Context) (int, int) {
	returnRPM := 0
	returnTPM := 0
	if rpmS := v.getValueFromContext(ctx, "X-Openai-Ratelimit-RequestPM-Embedding"); rpmS != "" {
		s, err := strconv.Atoi(rpmS)
		if err == nil {
			returnRPM = s
		}
	}
	if tpmS := v.getValueFromContext(ctx, "X-Openai-Ratelimit-TokenPM-Embedding"); tpmS != "" {
		s, err := strconv.Atoi(tpmS)
		if err == nil {
			returnTPM = s
		}
	}

	return returnRPM, returnTPM
}

func (v *client) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	config := v.getVectorizationConfig(cfg)

	key, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *client) GetVectorizerRateLimit(ctx context.Context) *modulecomponents.RateLimits {
	rpm, tpm := v.getRateLimit(ctx)
	return &modulecomponents.RateLimits{
		RemainingTokens:   tpm,
		LimitTokens:       tpm,
		ResetTokens:       time.Now().Add(61 * time.Second),
		RemainingRequests: rpm,
		LimitRequests:     rpm,
		ResetRequests:     time.Now().Add(61 * time.Second),
	}
}

func (v *client) getApiKey(ctx context.Context, isAzure bool) (string, error) {
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

func (v *client) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *client) getValueFromContext(ctx context.Context, key string) string {
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

func (v *client) getModelString(docType, model, action, version string) string {
	if strings.HasPrefix(model, "text-embedding-3") {
		// indicates that we handle v3 models
		return model
	}
	if version == "002" {
		return v.getModel002String(model)
	}
	return v.getModel001String(docType, model, action)
}

func (v *client) getModel001String(docType, model, action string) string {
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

func (v *client) getModel002String(model string) string {
	modelBaseString := "text-embedding-%s-002"
	return fmt.Sprintf(modelBaseString, model)
}

func (v *client) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Type:         settings.Type(),
		Model:        settings.Model(),
		ModelVersion: settings.ModelVersion(),
		ResourceName: settings.ResourceName(),
		DeploymentID: settings.DeploymentID(),
		BaseURL:      settings.BaseURL(),
		IsAzure:      settings.IsAzure(),
		Dimensions:   settings.Dimensions(),
	}
}
