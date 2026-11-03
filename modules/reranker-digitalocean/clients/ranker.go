//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"net/url"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-digitalocean/config"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

var _NUMCPU = runtime.NumCPU()

type client struct {
	apiKey       string
	httpClient   *http.Client
	maxDocuments int
	logger       logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:       apiKey,
		httpClient:   modulecomponents.NewBaseHttpClient(timeout),
		maxDocuments: 100,
		logger:       logger,
	}
}

// Rank scores documents against query. Documents are split into chunks of at
// most maxDocuments and sent concurrently; the returned scores keep the order
// of the input documents.
func (c *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	eg := enterrors.NewErrorGroupWrapper(c.logger)
	eg.SetLimit(_NUMCPU)

	chunkedDocuments := c.chunkDocuments(documents, c.maxDocuments)
	// each goroutine writes only its own slot, so no locking is needed
	documentScoreResponses := make([][]ent.DocumentScore, len(chunkedDocuments))
	for i := range chunkedDocuments {
		eg.Go(func() error {
			documentScoreResponse, err := c.performRank(ctx, query, chunkedDocuments[i], cfg)
			if err != nil {
				return err
			}
			documentScoreResponses[i] = documentScoreResponse
			return nil
		}, chunkedDocuments[i])
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return c.toRankResult(query, documentScoreResponses), nil
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
	body, err := json.Marshal(rankRequest{
		Model:     settings.Model(),
		Query:     query,
		Documents: documents,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}
	endpoint, err := c.getRerankURL(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "DigitalOcean API Key")
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if weaviateUUID := settings.WeaviateUUID(); weaviateUUID != "" {
		req.Header.Set("User-Agent", fmt.Sprintf("vector-db/weaviate/%s %s", weaviateUUID, build.Version))
	} else {
		req.Header.Set("User-Agent", fmt.Sprintf("vector-db/weaviate/unknown %s", build.Version))
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

	if res.StatusCode != http.StatusOK {
		return nil, c.getResponseError(res.StatusCode, res.Header.Get("x-request-id"), bodyBytes)
	}

	var resBody rankResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, fmt.Errorf("failed to parse reranker response (status %d): %w", res.StatusCode, err)
	}

	return c.toDocumentScores(documents, resBody.Results)
}

// toDocumentScores maps results, which the API returns sorted by relevance,
// back to the order of the input documents using each result's index. The
// request asks for every document to be scored, so a missing, duplicate or
// out-of-range index means a malformed response.
func (c *client) toDocumentScores(documents []string, results []rankResult) ([]ent.DocumentScore, error) {
	if len(results) != len(documents) {
		return nil, fmt.Errorf("DigitalOcean reranker returned %d results for %d documents", len(results), len(documents))
	}
	documentScores := make([]ent.DocumentScore, len(documents))
	seen := make([]bool, len(documents))
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(documents) {
			return nil, fmt.Errorf("DigitalOcean reranker result index %d out of range for %d documents", result.Index, len(documents))
		}
		if seen[result.Index] {
			return nil, fmt.Errorf("DigitalOcean reranker returned duplicate index %d", result.Index)
		}
		seen[result.Index] = true
		documentScores[result.Index] = ent.DocumentScore{
			Document: documents[result.Index],
			Score:    result.RelevanceScore,
		}
	}
	return documentScores, nil
}

func (c *client) chunkDocuments(documents []string, chunkSize int) [][]string {
	var requests [][]string
	for i := 0; i < len(documents); i += chunkSize {
		end := min(i+chunkSize, len(documents))
		requests = append(requests, documents[i:end])
	}
	return requests
}

func (c *client) getRerankURL(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL, err := modulecomponents.ValidatedBaseURLFromHeader(ctx, "X-Digitalocean-Baseurl", baseURL)
	if err != nil {
		return "", err
	}
	return url.JoinPath(passedBaseURL, "/v1/rerank")
}

func (c *client) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Digitalocean-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Digitalocean-Api-Key " +
		"nor in environment variable under DIGITALOCEAN_APIKEY")
}

// getResponseError builds an error from a non-200 response. DigitalOcean
// returns either an OpenAI-style {"error":{"message":...}} body or the
// platform-wide {"id":...,"message":...} body, so both are checked.
func (c *client) getResponseError(statusCode int, requestID string, bodyBytes []byte) error {
	msg := fmt.Sprintf("connection to DigitalOcean Serverless Inference API failed with status: %d", statusCode)
	if requestID != "" {
		msg = fmt.Sprintf("%s request-id: %s", msg, requestID)
	}
	var resBody responseError
	if err := json.Unmarshal(bodyBytes, &resBody); err == nil {
		if resBody.Error != nil && resBody.Error.Message != "" {
			return fmt.Errorf("%s error: %s", msg, resBody.Error.Message)
		}
		if resBody.Message != "" {
			return fmt.Errorf("%s error: %s", msg, resBody.Message)
		}
	}
	return errors.New(msg)
}

type rankRequest struct {
	Model     string   `json:"model"`
	Query     string   `json:"query"`
	Documents []string `json:"documents"`
}

type rankResponse struct {
	Results []rankResult `json:"results"`
}

type rankResult struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
}

type responseError struct {
	Error   *errorDetail `json:"error,omitempty"`
	Message string       `json:"message,omitempty"`
}

type errorDetail struct {
	Message string `json:"message"`
}
