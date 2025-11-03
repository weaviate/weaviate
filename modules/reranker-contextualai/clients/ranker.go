//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	"runtime"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-contextualai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

var _NUMCPU = runtime.NumCPU()

type client struct {
	lock         sync.RWMutex
	apiKey       string
	host         string
	path         string
	httpClient   *http.Client
	maxDocuments int
	logger       logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:       apiKey,
		httpClient:   &http.Client{Timeout: timeout},
		host:         "https://api.contextual.ai",
		path:         "/v1/rerank",
		maxDocuments: 1000,
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

func (c *client) performRank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) ([]ent.DocumentScore, error) {
	input, err := c.buildRankInput(query, documents, cfg)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	res, err := c.makeRankRequest(ctx, body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	if res.StatusCode != 200 {
		return nil, c.handleRankError(res.StatusCode, bodyBytes)
	}

	var rankResponse RankResponse
	if err := json.Unmarshal(bodyBytes, &rankResponse); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}
	return c.toDocumentScores(documents, rankResponse.Results), nil
}

func (c *client) buildRankInput(query string, documents []string, cfg moduletools.ClassConfig) (RankInput, error) {
	settings := config.NewClassSettings(cfg)

	input := RankInput{
		Query:     query,
		Documents: documents,
		Model:     settings.Model(),
	}

	if instruction := settings.Instruction(); instruction != "" {
		input.Instruction = &instruction
	}

	if topN := settings.TopN(); topN > 0 {
		input.TopN = &topN
	}

	metadata := make([]string, len(documents))
	for i := range metadata {
		metadata[i] = ""
	}
	input.Metadata = metadata

	return input, nil
}

func (c *client) makeRankRequest(ctx context.Context, body []byte) (*http.Response, error) {
	contextualUrl, err := url.JoinPath(c.host, c.path)
	if err != nil {
		return nil, errors.Wrap(err, "join Contextual AI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", contextualUrl, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Contextual AI API Key")
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}

	return res, nil
}

func (c *client) handleRankError(statusCode int, bodyBytes []byte) error {
	var apiError contextualApiError
	if err := json.Unmarshal(bodyBytes, &apiError); err != nil {
		return errors.Wrap(err, "unmarshal error from response body")
	}

	if apiError.Message != "" {
		return errors.Errorf("connection to Contextual AI API failed with status %d: %s", statusCode, apiError.Message)
	}

	if len(apiError.Detail) > 0 && apiError.Detail[0].Msg != "" {
		return errors.Errorf("connection to Contextual AI API failed with status %d: %s", statusCode, apiError.Detail[0].Msg)
	}

	return errors.Errorf("connection to Contextual AI API failed with status %d", statusCode)
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

func (c *client) toDocumentScores(documents []string, results []RerankedResult) []ent.DocumentScore {
	documentScores := make([]ent.DocumentScore, len(results))
	for _, result := range results {
		documentScores[result.Index] = ent.DocumentScore{
			Document: documents[result.Index],
			Score:    result.RelevanceScore,
		}
	}
	return documentScores
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

func (c *client) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-ContextualAI-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found for Contextual AI " +
		"neither in request header: X-ContextualAI-Api-Key " +
		"nor in the environment variable under CONTEXTUALAI_APIKEY")
}

type RankInput struct {
	Query       string   `json:"query"`
	Documents   []string `json:"documents"`
	Model       string   `json:"model"`
	TopN        *int     `json:"top_n,omitempty"`
	Instruction *string  `json:"instruction,omitempty"`
	Metadata    []string `json:"metadata,omitempty"`
}

type RerankedResult struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
}

type RankResponse struct {
	Results []RerankedResult `json:"results"`
}

type contextualApiError struct {
	Message string        `json:"message"`
	Detail  []errorDetail `json:"detail"`
}

type errorDetail struct {
	Loc  []string `json:"loc"`
	Msg  string   `json:"msg"`
	Type string   `json:"type"`
}
