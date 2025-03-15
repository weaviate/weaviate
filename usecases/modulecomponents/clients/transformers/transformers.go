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

package transformers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type taskType string

const (
	query   taskType = "query"
	passage taskType = "passage"
)

type VectorizationConfig struct {
	PoolingStrategy                        string
	InferenceURL                           string
	PassageInferenceURL, QueryInferenceURL string
}

type VectorizationResult struct {
	Text       string
	Dimensions int
	Vector     []float32
}

type vecRequest struct {
	Text   string           `json:"text"`
	Dims   int              `json:"dims"`
	Vector []float32        `json:"vector"`
	Error  string           `json:"error"`
	Config vecRequestConfig `json:"config"`
}

type vecRequestConfig struct {
	PoolingStrategy string   `json:"pooling_strategy,omitempty"`
	TaskType        taskType `json:"task_type,omitempty"`
}

type URLBuilder struct {
	originPassage, originQuery string
}

func NewURLBuilder(originPassage, originQuery string) *URLBuilder {
	return &URLBuilder{originPassage: originPassage, originQuery: originQuery}
}

func (b *URLBuilder) GetPassageURL(path string, config VectorizationConfig) string {
	baseURL := b.originPassage
	if config.PassageInferenceURL != "" {
		baseURL = config.PassageInferenceURL
	}
	if config.InferenceURL != "" {
		baseURL = config.InferenceURL
	}
	return fmt.Sprintf("%s%s", baseURL, path)
}

func (b *URLBuilder) GetQueryURL(path string, config VectorizationConfig) string {
	baseURL := b.originQuery
	if config.QueryInferenceURL != "" {
		baseURL = config.QueryInferenceURL
	}
	if config.InferenceURL != "" {
		baseURL = config.InferenceURL
	}
	return fmt.Sprintf("%s%s", baseURL, path)
}

type Client struct {
	httpClient *http.Client
	urlBuilder *URLBuilder
	logger     logrus.FieldLogger
}

func New(urlBuilder *URLBuilder, timeout time.Duration, logger logrus.FieldLogger) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilder: urlBuilder,
		logger:     logger,
	}
}

func (c *Client) VectorizeObject(ctx context.Context, input string,
	config VectorizationConfig,
) (*VectorizationResult, error) {
	return c.vectorize(ctx, input, config, c.urlBuilder.GetPassageURL, passage)
}

func (c *Client) VectorizeQuery(ctx context.Context, input string,
	config VectorizationConfig,
) (*VectorizationResult, error) {
	return c.vectorize(ctx, input, config, c.urlBuilder.GetQueryURL, query)
}

func (c *Client) vectorize(ctx context.Context, input string,
	config VectorizationConfig, url func(string, VectorizationConfig) string,
	task taskType,
) (*VectorizationResult, error) {
	body, err := json.Marshal(vecRequest{
		Text: input,
		Config: vecRequestConfig{
			PoolingStrategy: config.PoolingStrategy,
			TaskType:        task,
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url("/vectors", config),
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody vecRequest
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 {
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
			resBody.Error)
	}

	return &VectorizationResult{
		Text:       resBody.Text,
		Dimensions: resBody.Dims,
		Vector:     resBody.Vector,
	}, nil
}

func (c *Client) CheckReady(ctx context.Context, endpoint string) error {
	// spawn a new context (derived on the overall context) which is used to
	// consider an individual request timed out
	// due to parent timeout being superior over request's one, request can be cancelled by parent timeout
	// resulting in "send check ready request" even if service is responding with non 2xx http code
	requestCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.Wrap(err, "create check ready request")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "send check ready request")
	}

	defer res.Body.Close()
	if res.StatusCode > 299 {
		return errors.Errorf("not ready: status %d", res.StatusCode)
	}

	return nil
}

func (c *Client) MetaInfo(endpoint string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(context.Background(), "GET", endpoint, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create GET meta request")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send GET meta request")
	}
	defer res.Body.Close()
	if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
		return nil, errors.Errorf("unexpected status code '%d' of meta request", res.StatusCode)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read meta response body")
	}

	var resBody map[string]any
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal meta response body")
	}
	return resBody, nil
}
