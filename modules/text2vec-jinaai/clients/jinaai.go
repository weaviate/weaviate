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
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-jinaai/ent"
)

const (
	DefaultRPM = 500 // from https://jina.ai/embeddings/
	DefaultTPM = 1_000_000
)

type (
	task          string
	embeddingType string
)

const (
	// taskType
	retrievalQuery   task = "retrieval.query"
	retrievalPassage task = "retrieval.passage"
	// embeddingType
	embeddingTypeFloat   embeddingType = "float"
	embeddingTypeBase64  embeddingType = "base64"
	embeddingTypeBinary  embeddingType = "binary"
	embeddingTypeUbinary embeddingType = "ubinary"
)

type embeddingsRequest struct {
	Input         []string      `json:"input"`
	Model         string        `json:"model,omitempty"`
	EmbeddingType embeddingType `json:"embedding_type,omitempty"`
	Normalized    bool          `json:"normalized,omitempty"`
	Task          *task         `json:"task,omitempty"`
	Dimensions    *int64        `json:"dimensions,omitempty"`
}

type jinaErrorDetail struct {
	Detail string `json:"detail,omitempty"` // in case of error detail holds the error message
}

type embedding struct {
	jinaErrorDetail
	Object string                  `json:"object"`
	Data   []embeddingData         `json:"data,omitempty"`
	Usage  *modulecomponents.Usage `json:"usage,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

func buildUrl(config ent.VectorizationConfig) (string, error) {
	host := config.BaseURL
	path := "/v1/embeddings"
	return url.JoinPath(host, path)
}

type vectorizer struct {
	jinaAIApiKey string
	httpClient   *http.Client
	buildUrlFn   func(config ent.VectorizationConfig) (string, error)
	logger       logrus.FieldLogger
}

func New(jinaAIApiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		jinaAIApiKey: jinaAIApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn: buildUrl,
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, int, error) {
	config := v.getVectorizationConfig(cfg)
	res, usage, err := v.vectorize(ctx, input, config.Model, retrievalPassage, config)
	return res, nil, usage, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	config := v.getVectorizationConfig(cfg)
	res, _, err := v.vectorize(ctx, input, config.Model, retrievalQuery, config)
	return res, err
}

func (v *vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	icheck := ent.NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:      icheck.Model(),
		BaseURL:    icheck.BaseURL(),
		Dimensions: icheck.Dimensions(),
	}
}

func (v *vectorizer) vectorize(ctx context.Context,
	input []string, model string, task task, config ent.VectorizationConfig,
) (*modulecomponents.VectorizationResult, int, error) {
	body, err := json.Marshal(v.getEmbeddingsRequest(input, model, task, config.Dimensions))
	if err != nil {
		return nil, 0, errors.Wrap(err, "marshal body")
	}

	endpoint, err := v.buildUrlFn(config)
	if err != nil {
		return nil, 0, errors.Wrap(err, "join jinaAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, 0, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, 0, errors.Wrap(err, "API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, 0, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 0, errors.Wrap(err, "read response body")
	}

	var resBody embedding
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, 0, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 {
		return nil, 0, v.getError(res.StatusCode, resBody.Detail)
	}

	texts := make([]string, len(resBody.Data))
	embeddings := make([][]float32, len(resBody.Data))
	for i := range resBody.Data {
		index := resBody.Data[i].Index
		texts[index] = resBody.Data[i].Object
		embeddings[index] = resBody.Data[i].Embedding
	}

	return &modulecomponents.VectorizationResult{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
	}, modulecomponents.GetTotalTokens(resBody.Usage), nil
}

func (v *vectorizer) getError(statusCode int, errorMessage string) error {
	endpoint := "JinaAI API"
	if errorMessage != "" {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, errorMessage)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *vectorizer) getEmbeddingsRequest(input []string, model string, task task, dimensions *int64) embeddingsRequest {
	req := embeddingsRequest{Input: input, Model: model, EmbeddingType: embeddingTypeFloat, Normalized: false}
	if strings.Contains(model, "v3") {
		// v3 models require taskType and dimensions params
		req.Task = &task
		req.Dimensions = dimensions
	}
	return req
}

func (v *vectorizer) getApiKeyHeaderAndValue(apiKey string) (string, string) {
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	return v.getApiKeyFromContext(ctx, "X-Jinaai-Api-Key", "JINAAI_APIKEY")
}

func (v *vectorizer) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if v.jinaAIApiKey != "" {
		return v.jinaAIApiKey, nil
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

func (v *vectorizer) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := v.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, _ := modulecomponents.GetRateLimitFromContext(ctx, "Jinaai", DefaultRPM, 0)

	execAfterRequestFunction := func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			if deductRequest {
				limits.RemainingRequests -= 1
			}
			return
		}

		limits.RemainingRequests = rpm
		limits.ResetRequests = time.Now().Add(time.Duration(61) * time.Second)
		limits.LimitRequests = rpm
		limits.LastOverwrite = time.Now()

		limits.RemainingTokens = DefaultTPM
		limits.LimitTokens = DefaultTPM
		limits.ResetTokens = time.Now().Add(time.Duration(61) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}

func (v *vectorizer) HasTokenLimit() bool { return true }

func (v *vectorizer) ReturnsRateLimit() bool { return false }
