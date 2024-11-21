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

type Settings struct {
	Task       Task
	Model      string
	BaseURL    string
	Dimensions *int64
	Normalized bool
}

func buildUrl(settings Settings) (string, error) {
	host := settings.BaseURL
	path := "/v1/embeddings"
	return url.JoinPath(host, path)
}

type Client struct {
	jinaAIApiKey string
	httpClient   *http.Client
	buildUrlFn   func(settings Settings) (string, error)
	defaultRPM   int
	defaultTPM   int
	logger       logrus.FieldLogger
}

func New(jinaAIApiKey string, timeout time.Duration, defaultRPM, defaultTPM int, logger logrus.FieldLogger) *Client {
	return &Client{
		jinaAIApiKey: jinaAIApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrlFn: buildUrl,
		defaultRPM: defaultRPM,
		defaultTPM: defaultTPM,
		logger:     logger,
	}
}

func (c *Client) Vectorize(ctx context.Context, input []string,
	settings Settings,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, int, error) {
	embeddingRequest := c.getTextEmbeddingsRequest(input, settings)
	resBody, err := c.vectorize(ctx, embeddingRequest, settings)
	if err != nil {
		return nil, nil, 0, err
	}

	texts := make([]string, len(resBody.Data))
	embeddings := make([][]float32, len(resBody.Data))
	for i := range resBody.Data {
		index := resBody.Data[i].Index
		texts[index] = resBody.Data[i].Object
		embeddings[index] = resBody.Data[i].Embedding
	}

	res := &modulecomponents.VectorizationResult{
		Text:       texts,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     embeddings,
	}

	return res, nil, modulecomponents.GetTotalTokens(resBody.Usage), nil
}

func (c *Client) VectorizeMultiModal(ctx context.Context, texts, images []string,
	settings Settings,
) (*modulecomponents.VectorizationCLIPResult, error) {
	embeddingRequest := c.getMultiModalEmbeddingsRequest(texts, images, settings)
	resBody, err := c.vectorize(ctx, embeddingRequest, settings)
	if err != nil {
		return nil, err
	}

	var textVectors, imageVectors [][]float32
	for i := range resBody.Data {
		if i < len(texts) {
			textVectors = append(textVectors, resBody.Data[i].Embedding)
		} else {
			imageVectors = append(imageVectors, resBody.Data[i].Embedding)
		}
	}

	res := &modulecomponents.VectorizationCLIPResult{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}
	return res, nil
}

func (c *Client) vectorize(ctx context.Context,
	embeddingRequest interface{}, settings Settings,
) (*embedding, error) {
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

	res, err := c.httpClient.Do(req)
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
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 {
		return nil, c.getError(res.StatusCode, resBody.Detail)
	}

	return &resBody, nil
}

func (c *Client) getError(statusCode int, errorMessage string) error {
	endpoint := "JinaAI API"
	if errorMessage != "" {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, errorMessage)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (c *Client) getTextEmbeddingsRequest(input []string, settings Settings) embeddingsRequest[[]string] {
	req := embeddingsRequest[[]string]{Input: input, Model: settings.Model, EmbeddingType: embeddingTypeFloat, Normalized: settings.Normalized}
	if strings.Contains(settings.Model, "v3") || strings.Contains(settings.Model, "clip") {
		// v3 models require taskType and dimensions params
		req.Task = &settings.Task
		req.Dimensions = settings.Dimensions
	}
	return req
}

func (c *Client) getMultiModalEmbeddingsRequest(texts, images []string, settings Settings) embeddingsRequest[[]MultiModalInput] {
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

func (c *Client) getApiKeyHeaderAndValue(apiKey string) (string, string) {
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (c *Client) getApiKey(ctx context.Context) (string, error) {
	return c.getApiKeyFromContext(ctx, "X-Jinaai-Api-Key", "JINAAI_APIKEY")
}

func (c *Client) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if c.jinaAIApiKey != "" {
		return c.jinaAIApiKey, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (c *Client) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := c.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
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
