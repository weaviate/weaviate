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
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-databricks/ent"
)

type embeddingsRequest struct {
	Input       []string `json:"input"`
	Instruction string   `json:"instruction,omitempty"`
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

type client struct {
	databricksToken string
	httpClient      *http.Client
	logger          logrus.FieldLogger
}

func New(databricksToken string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		databricksToken: databricksToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *client) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	config := v.getVectorizationConfig(cfg)
	return v.vectorize(ctx, input, config)
}

func (v *client) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	config := v.getVectorizationConfig(cfg)
	res, _, err := v.vectorize(ctx, input, config)
	return res, err
}

func (v *client) vectorize(ctx context.Context, input []string, config ent.VectorizationConfig) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, error) {
	body, err := json.Marshal(v.getEmbeddingsRequest(input, config.Instruction))
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshal body")
	}

	endpoint, _ := v.buildURL(ctx, config)

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
	servingUrl := config.ServingURL
	if headerServingUrl := modulecomponents.GetValueFromContext(ctx, "X-Databricks-Servingurl"); headerServingUrl != "" {
		servingUrl = headerServingUrl
	}
	return servingUrl, nil
}

func (v *client) getError(statusCode int, resBodyError *openAIApiError, isAzure bool) error {
	endpoint := "Databricks Foundation Model API"
	if isAzure {
		endpoint = "Azure OpenAI API"
	}
	if resBodyError != nil {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBodyError.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *client) getEmbeddingsRequest(input []string, instruction string) embeddingsRequest {

	return embeddingsRequest{Input: input, Instruction: instruction}
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
	return ""
}

func (v *client) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	config := v.getVectorizationConfig(cfg)

	key, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	config := v.getVectorizationConfig(cfg)
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

	apiKey = "X-Databricks-Token"
	envVar = "DATABRICKS_TOKEN"
	envVarValue = v.databricksToken

	return v.getApiKeyFromContext(ctx, apiKey, envVarValue, envVar)
}

func (v *client) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no Databricks token found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *client) getModelString(config ent.VectorizationConfig, action string) string {
	if strings.HasPrefix(config.Model, "text-embedding-3") || config.IsThirdPartyProvider {
		// indicates that we handle v3 models
		return config.Model
	}
	if config.ModelVersion == "002" {
		return v.getModel002String(config.Model)
	}
	return v.getModel001String(config.Type, config.Model, action)
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
		ServingURL:           settings.ServingURL(),
		Instruction:          settings.Instruction(),
	}
}
