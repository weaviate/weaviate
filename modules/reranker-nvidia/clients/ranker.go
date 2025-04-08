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

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-nvidia/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

var _NUMCPU = runtime.NumCPU()

type client struct {
	lock         sync.RWMutex
	apiKey       string
	httpClient   *http.Client
	maxDocuments int
	logger       logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:       apiKey,
		httpClient:   &http.Client{Timeout: timeout},
		maxDocuments: 512,
		logger:       logger,
	}
}

func (c *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	eg := enterrors.NewErrorGroupWrapper(c.logger)
	eg.SetLimit(_NUMCPU)

	chunkedDocuments := c.chunkDocuments(documents, c.maxDocuments)
	documentScoreResponses := make([][]ent.DocumentScore, len(chunkedDocuments))
	for i := range chunkedDocuments {
		i := i // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			documentScoreResponse, err := c.performRank(ctx, query, chunkedDocuments[i], cfg)
			if err != nil {
				return err
			}
			c.lockGuard(func() {
				documentScoreResponses[i] = documentScoreResponse
			})
			return nil
		}, chunkedDocuments[i])
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return c.toRankResult(query, documentScoreResponses), nil
}

func (c *client) lockGuard(mutate func()) {
	c.lock.Lock()
	defer c.lock.Unlock()
	mutate()
}

func (c *client) toRankResult(query string, results [][]ent.DocumentScore) *ent.RankResult {
	documentScores := []ent.DocumentScore{}
	for i := range results {
		documentScores = append(documentScores, results[i]...)
	}
	return &ent.RankResult{
		Query:          query,
		DocumentScores: documentScores,
	}
}

func (c *client) performRank(ctx context.Context,
	query string, documents []string, cfg moduletools.ClassConfig,
) ([]ent.DocumentScore, error) {
	settings := config.NewClassSettings(cfg)
	body, err := json.Marshal(c.getRankRequest(settings.Model(), query, documents))
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}
	url := c.getNvidiaUrl(ctx, settings.BaseURL())
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

	var resBody rankResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	return c.toDocumentScores(documents, resBody.Rankings), nil
}

func (c *client) getRankRequest(model, query string, documents []string) rankRequest {
	passages := make([]text, len(documents))
	for i := range documents {
		passages[i] = text{Text: documents[i]}
	}
	return rankRequest{
		Model:    model,
		Query:    text{Text: query},
		Passages: passages,
	}
}

func (c *client) toDocumentScores(documents []string, results []ranking) []ent.DocumentScore {
	documentScores := make([]ent.DocumentScore, len(results))
	for _, result := range results {
		documentScores[result.Index] = ent.DocumentScore{
			Document: documents[result.Index],
			Score:    result.Logit,
		}
	}
	return documentScores
}

func (c *client) chunkDocuments(documents []string, chunkSize int) [][]string {
	var requests [][]string
	for i := 0; i < len(documents); i += chunkSize {
		end := i + chunkSize

		if end > len(documents) {
			end = len(documents)
		}

		requests = append(requests, documents[i:end])
	}

	return requests
}

func (c *client) getNvidiaUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Nvidia-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/v1/retrieval/nvidia/reranking", passedBaseURL)
}

func (c *client) getApiKey(ctx context.Context) (string, error) {
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

func (c *client) getResponseError(statusCode int, bodyBytes []byte) error {
	switch statusCode {
	case 402, 403:
		var resBody responseError402
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return fmt.Errorf("connection to NVIDIA API failed with status: %d: unmarshal response body: %w: got: %v",
				statusCode, err, string(bodyBytes))
		}
		return fmt.Errorf("connection to NVIDIA API failed with status: %d error: %s", statusCode, resBody.Detail)
	case 422:
		var resBody responseError422
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return fmt.Errorf("connection to NVIDIA API failed with status: %d: unmarshal response body: %w: got: %v",
				statusCode, err, string(bodyBytes))
		}
		details := make([]string, len(resBody.Detail))
		for i := range resBody.Detail {
			details[i] = resBody.Detail[0].Message
		}
		return fmt.Errorf("connection to NVIDIA API failed with status: %d error: %s", statusCode, strings.Join(details, " "))
	default:
		return fmt.Errorf("connection to NVIDIA API failed with status: %d", statusCode)
	}
}

type rankRequest struct {
	Model    string  `json:"model"`
	Query    text    `json:"query"`
	Passages []text  `json:"passages"`
	Truncate *string `json:"truncate,omitempty"`
}

type text struct {
	Text string `json:"text"`
}

type rankResponse struct {
	Rankings []ranking `json:"rankings"`
}

type ranking struct {
	Index int     `json:"index"`
	Logit float64 `json:"logit"`
}

type responseError402 struct {
	Detail string `json:"detail"`
}

type responseError422 struct {
	Detail []errorDetail `json:"detail"`
}

type errorDetail struct {
	Message string `json:"msg"`
}
