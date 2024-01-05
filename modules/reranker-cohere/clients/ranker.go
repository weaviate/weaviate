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
	"runtime"
	"sync"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-cohere/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
	"golang.org/x/sync/errgroup"
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
		host:         "https://api.cohere.ai",
		path:         "/v1/rerank",
		maxDocuments: 1000,
		logger:       logger,
	}
}

func (c *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	eg := &errgroup.Group{}
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
		})
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
	settings := config.NewClassSettings(cfg)
	cohereUrl, err := url.JoinPath(c.host, c.path)
	if err != nil {
		return nil, errors.Wrap(err, "join Cohere API host and path")
	}

	input := RankInput{
		Documents:       documents,
		Query:           query,
		Model:           settings.Model(),
		ReturnDocuments: false,
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cohereUrl, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Cohere API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
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
		var apiError cohereApiError
		err = json.Unmarshal(bodyBytes, &apiError)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal error from response body")
		}
		if apiError.Message != "" {
			return nil, errors.Errorf("connection to Cohere API failed with status %d: %s", res.StatusCode, apiError.Message)
		}
		return nil, errors.Errorf("connection to Cohere API failed with status %d", res.StatusCode)
	}

	var rankResponse RankResponse
	if err := json.Unmarshal(bodyBytes, &rankResponse); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}
	return c.toDocumentScores(documents, rankResponse.Results), nil
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

func (c *client) toDocumentScores(documents []string, results []Result) []ent.DocumentScore {
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
	if len(c.apiKey) > 0 {
		return c.apiKey, nil
	}
	key := "X-Cohere-Api-Key"

	apiKey := ctx.Value(key)
	// try getting header from GRPC if not successful
	if apiKey == nil {
		apiKey = modulecomponents.GetValueFromGRPC(ctx, key)
	}
	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0], nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Cohere-Api-Key " +
		"nor in environment variable under COHERE_APIKEY")
}

type RankInput struct {
	Documents       []string `json:"documents"`
	Query           string   `json:"query"`
	Model           string   `json:"model"`
	ReturnDocuments bool     `json:"return_documents"`
}

type Document struct {
	Text string `json:"text"`
}

type Result struct {
	Index          int      `json:"index"`
	RelevanceScore float64  `json:"relevance_score"`
	Document       Document `json:"document"`
}

type APIVersion struct {
	Version string `json:"version"`
}

type Meta struct {
	APIVersion APIVersion `json:"api_version"`
}

type RankResponse struct {
	ID      string   `json:"id"`
	Results []Result `json:"results"`
	Meta    Meta     `json:"meta"`
}

type cohereApiError struct {
	Message string `json:"message"`
}
