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

package cohere

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	DefaultRPM = 10000    // from https://docs.cohere.com/docs/going-live#production-key-specifications default for production keys
	DefaultTPM = 10000000 // no token limit used by cohere
)

type embeddingsRequest struct {
	Texts          []string        `json:"texts,omitempty"`
	Images         []string        `json:"images,omitempty"`
	Model          string          `json:"model,omitempty"`
	InputType      InputType       `json:"input_type,omitempty"`
	EmbeddingTypes []embeddingType `json:"embedding_types,omitempty"`
	Truncate       string          `json:"truncate,omitempty"`
}

type billedUnits struct {
	InputTokens    int `json:"input_tokens,omitempty"`
	OutputTokens   int `json:"output_tokens,omitempty"`
	SearchUnits    int `json:"search_units,omitempty"`
	Classificatons int `json:"classifications,omitempty"`
	Images         int `json:"images,omitempty"`
}

type meta struct {
	BilledUnits billedUnits `json:"billed_units,omitempty"`
	Warnings    []string    `json:"warnings,omitempty"`
}

type embeddingsResponse struct {
	ID         string     `json:"id"`
	Embeddings embeddings `json:"embeddings,omitempty"`
	Message    string     `json:"message,omitempty"`
	Meta       meta       `json:"meta,omitempty"`
}

type embeddings struct {
	Float [][]float32 `json:"float,omitempty"`
}

type Client struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *cohereUrlBuilder
	logger     logrus.FieldLogger
}

type InputType string

const (
	SearchDocument InputType = "search_document"
	SearchQuery    InputType = "search_query"
	Image          InputType = "image"
)

type embeddingType string

const float embeddingType = "float"

type Settings struct {
	Model     string
	Truncate  string
	BaseURL   string
	InputType InputType
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: newCohereUrlBuilder(),
		logger:     logger,
	}
}

func (c *Client) Vectorize(ctx context.Context,
	inputs []string, settings Settings,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	body, err := json.Marshal(c.getEmbeddingRequest(inputs, settings))
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	url := c.getCohereUrl(ctx, settings.BaseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Cohere API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Request-Source", "unspecified:weaviate")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 {
		errorMessage := c.getErrorMessage(res.StatusCode, resBody.Message)
		return nil, errors.New(errorMessage)
	}

	if len(resBody.Embeddings.Float) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       inputs,
		Dimensions: len(resBody.Embeddings.Float[0]),
		Vector:     resBody.Embeddings.Float,
	}, nil
}

func (c *Client) getEmbeddingRequest(inputs []string, settings Settings) embeddingsRequest {
	switch settings.InputType {
	case Image:
		images := make([]string, len(inputs))
		for i := range inputs {
			if !strings.HasPrefix(inputs[i], "data:") {
				images[i] = fmt.Sprintf("data:image/png;base64,%s", inputs[i])
			} else {
				images[i] = inputs[i]
			}
		}
		return embeddingsRequest{
			Images:         images,
			Model:          settings.Model,
			InputType:      settings.InputType,
			EmbeddingTypes: []embeddingType{float},
		}
	default:
		return embeddingsRequest{
			Texts:          inputs,
			Model:          settings.Model,
			Truncate:       settings.Truncate,
			InputType:      settings.InputType,
			EmbeddingTypes: []embeddingType{float},
		}
	}
}

func (c *Client) getCohereUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Cohere-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return c.urlBuilder.url(passedBaseURL)
}

func (c *Client) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	key, err := c.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (c *Client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, _ := modulecomponents.GetRateLimitFromContext(ctx, "Cohere", DefaultRPM, 0)

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

		// high dummy values
		limits.RemainingTokens = DefaultTPM
		limits.LimitTokens = DefaultTPM
		limits.ResetTokens = time.Now().Add(time.Duration(1) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}

func (c *Client) getErrorMessage(statusCode int, resBodyError string) string {
	if resBodyError != "" {
		return fmt.Sprintf("connection to Cohere failed with status: %d error: %v", statusCode, resBodyError)
	}
	return fmt.Sprintf("connection to Cohere failed with status: %d", statusCode)
}

func (c *Client) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Cohere-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Cohere-Api-Key " +
		"nor in environment variable under COHERE_APIKEY")
}

func (c *Client) HasTokenLimit() bool { return false }

func (c *Client) ReturnsRateLimit() bool { return false }
