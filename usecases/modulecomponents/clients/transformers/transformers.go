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
	query    taskType = "query"
	document taskType = "document"
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

type Client struct {
	originPassage string
	originQuery   string
	httpClient    *http.Client
	logger        logrus.FieldLogger
}

func New(originPassage, originQuery string, timeout time.Duration, logger logrus.FieldLogger) *Client {
	return &Client{
		originPassage: originPassage,
		originQuery:   originQuery,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *Client) VectorizeObject(ctx context.Context, input string,
	config VectorizationConfig,
) (*VectorizationResult, error) {
	return c.vectorize(ctx, input, config, c.urlPassage, document)
}

func (c *Client) VectorizeQuery(ctx context.Context, input string,
	config VectorizationConfig,
) (*VectorizationResult, error) {
	return c.vectorize(ctx, input, config, c.urlQuery, query)
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

func (c *Client) urlPassage(path string, config VectorizationConfig) string {
	baseURL := c.originPassage
	if config.PassageInferenceURL != "" {
		baseURL = config.PassageInferenceURL
	}
	if config.InferenceURL != "" {
		baseURL = config.InferenceURL
	}
	return fmt.Sprintf("%s%s", baseURL, path)
}

func (c *Client) urlQuery(path string, config VectorizationConfig) string {
	baseURL := c.originQuery
	if config.QueryInferenceURL != "" {
		baseURL = config.QueryInferenceURL
	}
	if config.InferenceURL != "" {
		baseURL = config.InferenceURL
	}
	return fmt.Sprintf("%s%s", baseURL, path)
}
