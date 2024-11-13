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
	Object    string          `json:"object"`
	Data      []embeddingData `json:"data,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
	Message   string          `json:"message,omitempty"`
	Error     struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
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
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg)
	res, limits, err := v.vectorize(ctx, input, config)
	return res, limits, 0, err
}

func (v *client) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getVectorizationConfig(cfg)
	res, _, err := v.vectorize(ctx, input, config)
	return res, err
}

func (v *client) vectorize(ctx context.Context, input []string, config ent.VectorizationConfig) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, error) {
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
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey))
	req.Header.Add("Content-Type", "application/json")
	if userAgent := modulecomponents.GetValueFromContext(ctx, "X-Databricks-User-Agent"); userAgent != "" {
		req.Header.Add("User-Agent", userAgent)
	}

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
		return nil, nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 || resBody.ErrorCode != "" {
		return nil, nil, v.getError(res.StatusCode, resBody)
	}
	rateLimit := ent.GetRateLimitsFromHeader(res.Header)

	texts := make([]string, len(resBody.Data))
	embeddings := make([][]float32, len(resBody.Data))
	databrickserror := make([]error, len(resBody.Data))
	for i := range resBody.Data {
		texts[i] = resBody.Data[i].Object
		embeddings[i] = resBody.Data[i].Embedding

	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
		Errors:     databrickserror,
	}, rateLimit, nil
}

func (v *client) buildURL(ctx context.Context, config ent.VectorizationConfig) (string, error) {
	endpoint := config.Endpoint
	if headerEndpoint := modulecomponents.GetValueFromContext(ctx, "X-Databricks-Endpoint"); headerEndpoint != "" {
		endpoint = headerEndpoint
	}
	return endpoint, nil
}

func (v *client) getError(statusCode int, resBody embedding) error {
	endpoint := "Databricks Foundation Model API"

	if resBody.ErrorCode != "" {
		return fmt.Errorf("connection to: %s failed with error code: %s message: %v", endpoint, resBody.ErrorCode, resBody.Message)
	}

	if resBody.Error.Message != "" {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBody.Error.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *client) getEmbeddingsRequest(input []string, instruction string) embeddingsRequest {
	return embeddingsRequest{Input: input, Instruction: instruction}
}

func (v *client) getApiKeyHeaderAndValue(apiKey string) (string, string) {
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *client) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	key, err := v.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	name := "Databricks"

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

func (v *client) getApiKey(ctx context.Context) (string, error) {
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

func (v *client) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Endpoint:    settings.Endpoint(),
		Instruction: settings.Instruction(),
	}
}
