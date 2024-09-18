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
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-jinaai/ent"
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
	Object string          `json:"object"`
	Data   []embeddingData `json:"data,omitempty"`
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

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, []string{input}, config.Model, retrievalPassage, config)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config.Model, retrievalQuery, config)
}

func (v *vectorizer) vectorize(ctx context.Context,
	input []string, model string, task task, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(v.getEmbeddingsRequest(input, model, task, config.Dimensions))
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	endpoint, err := v.buildUrlFn(config)
	if err != nil {
		return nil, errors.Wrap(err, "join jinaAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey))
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

	if res.StatusCode != 200 {
		return nil, v.getError(res.StatusCode, resBody.Detail)
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
