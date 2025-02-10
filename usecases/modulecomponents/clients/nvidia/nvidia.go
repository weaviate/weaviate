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

package nvidia

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
)

type (
	InputType      string
	encodingFormat string
)

const (
	// inputType
	encodingFormatFloat encodingFormat = "float"
)

var (
	// inputType
	Query   InputType = "query"
	Passage InputType = "passage"
)

type embeddingsRequest struct {
	Input          []string       `json:"input,omitempty"`
	Model          string         `json:"model,omitempty"`
	EncodingFormat encodingFormat `json:"encoding_format,omitempty"`
	InputType      *InputType     `json:"input_type,omitempty"`
	Truncate       *string        `json:"truncate,omitempty"`
}

type embeddingsResponse struct {
	ID    string           `json:"id"`
	Model string           `json:"model,omitempty"`
	Data  []embeddingsData `json:"data,omitempty"`
	Usage *usage           `json:"usage,omitempty"`
}

type embeddingsData struct {
	Object    string    `json:"object,omitempty"`
	Index     int64     `json:"index,omitempty"`
	Embedding []float32 `json:"embedding,omitempty"`
}

type usage struct {
	PromptTokens int64 `json:"prompt_tokens,omitempty"`
	TotalTokens  int64 `json:"total_tokens,omitempty"`
}

type embeddingsErrorResponse struct {
	Object  string `json:"object,omitempty"`
	Message string `json:"message,omitempty"`
	Title   string `json:"title,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

type Client struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

type Settings struct {
	BaseURL   string
	Model     string
	InputType *InputType
	Truncate  *string
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *Client) Vectorize(ctx context.Context,
	inputs []string, settings Settings,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	body, err := json.Marshal(c.getEmbeddingRequest(inputs, settings))
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	url := c.getNvidiaUrl(ctx, settings.BaseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Nvidia API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Accept", "application/json")
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

	if res.StatusCode != 200 {
		return nil, c.getResponseError(res.StatusCode, bodyBytes)
	}

	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if len(resBody.Data) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	vectors := make([][]float32, len(resBody.Data))
	for i := range resBody.Data {
		vectors[i] = resBody.Data[i].Embedding
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       inputs,
		Dimensions: len(vectors[0]),
		Vector:     vectors,
	}, nil
}

func (c *Client) getEmbeddingRequest(inputs []string, settings Settings) embeddingsRequest {
	return embeddingsRequest{
		Input:          inputs,
		Model:          settings.Model,
		InputType:      settings.InputType,
		Truncate:       settings.Truncate,
		EncodingFormat: encodingFormatFloat,
	}
}

func (c *Client) getNvidiaUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Nvidia-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/v1/embeddings", passedBaseURL)
}

func (c *Client) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := c.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	return &modulecomponents.RateLimits{
		LimitRequests:        100,
		LimitTokens:          1000000,
		RemainingRequests:    100,
		RemainingTokens:      1000000,
		ResetRequests:        time.Now(),
		ResetTokens:          time.Now(),
		AfterRequestFunction: func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {},
	}
}

func (c *Client) getResponseError(statusCode int, bodyBytes []byte) error {
	switch statusCode {
	case 400, 402, 422, 500:
		var resBody embeddingsErrorResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return fmt.Errorf("connection to NVIDIA API failed with status: %d: unmarshal response body: %w: got: %v",
				statusCode, err, string(bodyBytes))
		}
		switch statusCode {
		case 400:
			return fmt.Errorf("connection to NVIDIA API failed with status: %d error: %s", statusCode, resBody.Message)
		case 402:
			return fmt.Errorf("connection to NVIDIA API failed with status: %d error: %s", statusCode, resBody.Detail)
		default:
			return fmt.Errorf("connection to NVIDIA API failed with status: %d error: %s", statusCode, resBody.Title)
		}
	default:
		return fmt.Errorf("connection to NVIDIA API failed with status: %d", statusCode)
	}
}

func (c *Client) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Nvidia-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Nvidia-Api-Key " +
		"nor in environment variable under NVIDIA_APIKEY")
}

func (c *Client) HasTokenLimit() bool { return false }

func (c *Client) ReturnsRateLimit() bool { return false }
