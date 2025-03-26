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

package jinaai

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

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type (
	Task          string
	embeddingType string
)

const (
	// taskType
	RetrievalQuery   Task = "retrieval.query"
	RetrievalPassage Task = "retrieval.passage"
	// embeddingType
	embeddingTypeFloat   embeddingType = "float"
	embeddingTypeBase64  embeddingType = "base64"
	embeddingTypeBinary  embeddingType = "binary"
	embeddingTypeUbinary embeddingType = "ubinary"
)

type MultiModalInput struct {
	Text  string `json:"text,omitempty"`
	Image string `json:"image,omitempty"`
}

type embeddingsRequest[T []string | []MultiModalInput] struct {
	Input         T             `json:"input"`
	Model         string        `json:"model,omitempty"`
	EmbeddingType embeddingType `json:"embedding_type,omitempty"`
	Normalized    bool          `json:"normalized,omitempty"`
	Task          *Task         `json:"task,omitempty"`
	Dimensions    *int64        `json:"dimensions,omitempty"`
}

type jinaErrorDetail struct {
	Detail string `json:"detail,omitempty"` // in case of error detail holds the error message
}

type embedding[T dto.Embedding] struct {
	jinaErrorDetail
	Object string                  `json:"object"`
	Data   []embeddingData[T]      `json:"data,omitempty"`
	Usage  *modulecomponents.Usage `json:"usage,omitempty"`
}

type embeddingData[T dto.Embedding] struct {
	Object     string `json:"object"`
	Index      int    `json:"index"`
	Embedding  T      `json:"embedding"`
	Embeddings T      `json:"embeddings"`
}

type Settings struct {
	Task       Task
	Model      string
	BaseURL    string
	Dimensions *int64
	Normalized bool
}

type Client[T dto.Embedding] struct {
	jinaAIApiKey string
	httpClient   *http.Client
	buildUrlFn   func(settings Settings) (string, error)
	defaultRPM   int
	defaultTPM   int
	logger       logrus.FieldLogger
}

func EmbeddingsBuildUrlFn(settings Settings) (string, error) {
	host := settings.BaseURL
	path := "/v1/embeddings"
	return url.JoinPath(host, path)
}

func MultiVectorBuildUrlFn(settings Settings) (string, error) {
	host := settings.BaseURL
	path := "/v1/multi-vector"
	return url.JoinPath(host, path)
}

func New[T dto.Embedding](jinaAIApiKey string,
	timeout time.Duration,
	defaultRPM, defaultTPM int,
	buildUrlFn func(settings Settings) (string, error),
	logger logrus.FieldLogger,
) *Client[T] {
	return &Client[T]{
		jinaAIApiKey: jinaAIApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn: buildUrlFn,
		defaultRPM: defaultRPM,
		defaultTPM: defaultTPM,
		logger:     logger,
	}
}

func (c *Client[T]) Vectorize(ctx context.Context, input []string,
	settings Settings,
) (*modulecomponents.VectorizationResult[T], *modulecomponents.RateLimits, int, error) {
	embeddingRequest := c.getTextEmbeddingsRequest(input, settings)
	resBody, err := c.vectorize(ctx, embeddingRequest, settings)
	if err != nil {
		return nil, nil, 0, err
	}

	texts := make([]string, len(resBody.Data))
	embeddings := make([]T, len(resBody.Data))
	for i := range resBody.Data {
		index := resBody.Data[i].Index
		texts[index] = resBody.Data[i].Object
		if resBody.Data[i].Embedding != nil {
			embeddings[index] = resBody.Data[i].Embedding
		} else if resBody.Data[i].Embeddings != nil {
			embeddings[index] = resBody.Data[i].Embeddings
		}
	}

	res := &modulecomponents.VectorizationResult[T]{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
	}

	return res, nil, modulecomponents.GetTotalTokens(resBody.Usage), nil
}

func (c *Client[T]) VectorizeMultiModal(ctx context.Context, texts, images []string,
	settings Settings,
) (*modulecomponents.VectorizationCLIPResult[T], error) {
	embeddingRequest := c.getMultiModalEmbeddingsRequest(texts, images, settings)
	resBody, err := c.vectorize(ctx, embeddingRequest, settings)
	if err != nil {
		return nil, err
	}

	var textVectors, imageVectors []T
	for i := range resBody.Data {
		if i < len(texts) {
			textVectors = append(textVectors, resBody.Data[i].Embedding)
		} else {
			imageVectors = append(imageVectors, resBody.Data[i].Embedding)
		}
	}

	res := &modulecomponents.VectorizationCLIPResult[T]{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}
	return res, nil
}

func (c *Client[T]) vectorize(ctx context.Context,
	embeddingRequest interface{}, settings Settings,
) (*embedding[T], error) {
	body, err := json.Marshal(embeddingRequest)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	endpoint, err := c.buildUrlFn(settings)
	if err != nil {
		return nil, errors.Wrap(err, "join jinaAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "API Key")
	}
	req.Header.Add(c.getApiKeyHeaderAndValue(apiKey))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-header-vectordb-source", "Weaviate")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody embedding[T]
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 {
		return nil, c.getError(res.StatusCode, resBody.Detail)
	}

	return &resBody, nil
}

func (c *Client[T]) getError(statusCode int, errorMessage string) error {
	endpoint := "JinaAI API"
	if errorMessage != "" {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, errorMessage)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (c *Client[T]) getTextEmbeddingsRequest(input []string, settings Settings) embeddingsRequest[[]string] {
	req := embeddingsRequest[[]string]{
		Input:         input,
		Model:         settings.Model,
		EmbeddingType: embeddingTypeFloat,
		Normalized:    settings.Normalized, Dimensions: settings.Dimensions,
	}
	if strings.Contains(settings.Model, "v3") || strings.Contains(settings.Model, "clip") {
		// v3 models require taskType and dimensions params
		req.Task = &settings.Task
	}
	return req
}

func (c *Client[T]) getMultiModalEmbeddingsRequest(texts, images []string, settings Settings) embeddingsRequest[[]MultiModalInput] {
	input := make([]MultiModalInput, len(texts)+len(images))
	for i := range texts {
		input[i] = MultiModalInput{Text: texts[i]}
	}
	offset := len(texts)
	for i := range images {
		input[offset+i] = MultiModalInput{Image: images[i]}
	}
	return embeddingsRequest[[]MultiModalInput]{
		Input:         input,
		Model:         settings.Model,
		EmbeddingType: embeddingTypeFloat,
		Normalized:    settings.Normalized,
		Dimensions:    settings.Dimensions,
	}
}

func (c *Client[T]) getApiKeyHeaderAndValue(apiKey string) (string, string) {
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (c *Client[T]) getApiKey(ctx context.Context) (string, error) {
	return c.getApiKeyFromContext(ctx, "X-Jinaai-Api-Key", "JINAAI_APIKEY")
}

func (c *Client[T]) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if c.jinaAIApiKey != "" {
		return c.jinaAIApiKey, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (c *Client[T]) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := c.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client[T]) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, _ := modulecomponents.GetRateLimitFromContext(ctx, "Jinaai", c.defaultRPM, 0)

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

		limits.RemainingTokens = c.defaultTPM
		limits.LimitTokens = c.defaultTPM
		limits.ResetTokens = time.Now().Add(time.Duration(61) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}
