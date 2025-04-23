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
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/monitoring"

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
	Object string                  `json:"object"`
	Data   []embeddingData         `json:"data,omitempty"`
	Error  *openAIApiError         `json:"error,omitempty"`
	Usage  *modulecomponents.Usage `json:"usage,omitempty"`
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

func buildUrl(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error) {
	if isAzure {
		host := baseURL
		if host == "" || host == "https://api.openai.com" {
			// Fall back to old assumption
			host = "https://" + resourceName + ".openai.azure.com"
		}

		path := "openai/deployments/" + deploymentID + "/embeddings"
		queryParam := fmt.Sprintf("api-version=%s", apiVersion)
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
	buildUrlFn         func(baseURL, resourceName, deploymentID, apiVersion string, isAzure bool) (string, error)
	logger             logrus.FieldLogger
	sampledLogger      *logrusext.Sampler
}

func New(openAIApiKey, openAIOrganization, azureApiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		openAIApiKey:       openAIApiKey,
		openAIOrganization: openAIOrganization,
		azureApiKey:        azureApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn:    buildUrl,
		logger:        logger,
		sampledLogger: logrusext.NewSampler(logger, 5, time.Minute),
	}
}

func (v *client) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg, "document")
	res, rateLimits, totalTokens, err := v.vectorize(ctx, input, config.ModelString, config)
	if err != nil {
		monitoring.GetMetrics().ModuleCallError.WithLabelValues("openai", "-", "-").Inc()
	}
	return res, rateLimits, totalTokens, err
}

func (v *client) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getVectorizationConfig(cfg, "query")
	res, _, _, err := v.vectorize(ctx, input, config.ModelString, config)
	if err != nil {
		monitoring.GetMetrics().ModuleExternalError.WithLabelValues("text2vec", "openai", "-", "-").Inc()
	}
	return res, err
}

func (v *client) vectorize(ctx context.Context, input []string, model string, config ent.VectorizationConfig) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	metrics := monitoring.GetMetrics()
	startTime := time.Now()
	metrics.ModuleExternalRequests.WithLabelValues("text2vec", "openai").Inc()

	body, err := json.Marshal(v.getEmbeddingsRequest(input, model, config.IsAzure, config.Dimensions))
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "marshal body")
	}

	endpoint, err := v.buildURL(ctx, config)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "join OpenAI API host and path")
	}

	defer func() {
		monitoring.GetMetrics().ModuleExternalRequestDuration.WithLabelValues("openai", endpoint).Observe(time.Since(startTime).Seconds())
	}()

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey, config.IsAzure))
	if openAIOrganization := v.getOpenAIOrganization(ctx); openAIOrganization != "" {
		req.Header.Add("OpenAI-Organization", openAIOrganization)
	}
	req.Header.Add("Content-Type", "application/json")

	metrics.ModuleExternalRequestSingleCount.WithLabelValues("text2vec", endpoint).Inc()

	metrics.ModuleExternalRequestSize.WithLabelValues("text2vec", endpoint).Observe(float64(len(body)))

	res, err := v.httpClient.Do(req)
	if res != nil {
		vrst := monitoring.GetMetrics().ModuleExternalResponseStatus
		vrst.WithLabelValues("text2vec", endpoint, fmt.Sprintf("%v", res.StatusCode)).Inc()
	}
	if err != nil {
		metrics.ModuleCallError.WithLabelValues("openai", endpoint, fmt.Sprintf("%v", err)).Inc()
		return nil, nil, 0, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	requestID := res.Header.Get("x-request-id")
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "read response body")
	}

	vrs := metrics.ModuleExternalResponseSize
	vrs.WithLabelValues("text2vec", endpoint).Observe(float64(len(bodyBytes)))

	var resBody embedding
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, nil, 0, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, nil, 0, v.getError(res.StatusCode, requestID, resBody.Error, config.IsAzure)
	}
	rateLimit := ent.GetRateLimitsFromHeader(v.sampledLogger, res.Header, config.IsAzure)

	texts := make([]string, len(resBody.Data))
	embeddings := make([][]float32, len(resBody.Data))
	openAIerror := make([]error, len(resBody.Data))
	for i := range resBody.Data {
		texts[i] = resBody.Data[i].Object
		embeddings[i] = resBody.Data[i].Embedding
		if resBody.Data[i].Error != nil {
			openAIerror[i] = v.getError(res.StatusCode, requestID, resBody.Data[i].Error, config.IsAzure)
		}
		if resBody.Usage != nil {
			vrt := metrics.VectorizerRequestTokens
			vrt.WithLabelValues("input", endpoint).Observe(float64(resBody.Usage.PromptTokens))
			vrt.WithLabelValues("output", endpoint).Observe(float64(resBody.Usage.CompletionTokens))
		}
	}

	if len(resBody.Data) == 0 {
		return nil, nil, 0, errors.New("no data returned from OpenAI API")
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
		Errors:     openAIerror,
	}, rateLimit, modulecomponents.GetTotalTokens(resBody.Usage), nil
}

func (v *client) buildURL(ctx context.Context, config ent.VectorizationConfig) (string, error) {
	baseURL, resourceName, deploymentID, apiVersion, isAzure := config.BaseURL, config.ResourceName, config.DeploymentID, config.ApiVersion, config.IsAzure

	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}

	if headerDeploymentID := modulecomponents.GetValueFromContext(ctx, "X-Azure-Deployment-Id"); headerDeploymentID != "" {
		deploymentID = headerDeploymentID
	}

	if headerResourceName := modulecomponents.GetValueFromContext(ctx, "X-Azure-Resource-Name"); headerResourceName != "" {
		resourceName = headerResourceName
	}

	return v.buildUrlFn(baseURL, resourceName, deploymentID, apiVersion, isAzure)
}

func (v *client) getError(statusCode int, requestID string, resBodyError *openAIApiError, isAzure bool) error {
	endpoint := "OpenAI API"
	if isAzure {
		endpoint = "Azure OpenAI API"
	}
	errorMsg := fmt.Sprintf("connection to: %s failed with status: %d", endpoint, statusCode)
	if requestID != "" {
		errorMsg = fmt.Sprintf("%s request-id: %s", errorMsg, requestID)
	}
	if resBodyError != nil {
		errorMsg = fmt.Sprintf("%s error: %v", errorMsg, resBodyError.Message)
	}
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("text2vec", endpoint, errorMsg, fmt.Sprintf("%v", statusCode)).Inc()
	return errors.New(errorMsg)
}

func (v *client) getEmbeddingsRequest(input []string, model string, isAzure bool, dimensions *int64) embeddingsRequest {
	if isAzure {
		return embeddingsRequest{Input: input, Dimensions: dimensions}
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
	if value := modulecomponents.GetValueFromContext(ctx, "X-Openai-Organization"); value != "" {
		return value
	}
	return v.openAIOrganization
}

func (v *client) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	config := v.getVectorizationConfig(cfg, "document")

	key, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	config := v.getVectorizationConfig(cfg, "document")
	name := "Openai"
	if config.IsAzure {
		name = "Azure"
	}
	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, name, 0, 0)
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
	var apiKey, envVarValue, envVar string

	if isAzure {
		apiKey = "X-Azure-Api-Key"
		envVar = "AZURE_APIKEY"
		envVarValue = v.azureApiKey
	} else {
		apiKey = "X-Openai-Api-Key"
		envVar = "OPENAI_APIKEY"
		envVarValue = v.openAIApiKey
	}

	return v.getApiKeyFromContext(ctx, apiKey, envVarValue, envVar)
}

func (v *client) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *client) getVectorizationConfig(cfg moduletools.ClassConfig, action string) ent.VectorizationConfig {
	settings := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Type:                 settings.Type(),
		Model:                settings.Model(),
		ModelVersion:         settings.ModelVersion(),
		ResourceName:         settings.ResourceName(),
		DeploymentID:         settings.DeploymentID(),
		BaseURL:              settings.BaseURL(),
		IsAzure:              settings.IsAzure(),
		IsThirdPartyProvider: settings.IsThirdPartyProvider(),
		ApiVersion:           settings.ApiVersion(),
		Dimensions:           settings.Dimensions(),
		ModelString:          settings.ModelStringForAction(action),
	}
}
