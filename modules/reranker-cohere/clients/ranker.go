//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-cohere/config"
	"github.com/weaviate/weaviate/modules/reranker-cohere/ent"
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

func New(apiKey string, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:       apiKey,
		httpClient:   &http.Client{},
		host:         "https://api.cohere.ai",
		path:         "/v1/rerank",
		maxDocuments: 1000,
		logger:       logger,
	}
}

func (v *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	var batchRankResponses []*batchRankResponse
	eg := &errgroup.Group{}
	eg.SetLimit(_NUMCPU)

	batchRankRequests := v.chunkDocuments(documents, v.maxDocuments)
	for i := range batchRankRequests {
		request := batchRankRequests[i]
		eg.Go(func() error {
			batchRankResponse, err := v.performRank(ctx, query, request, cfg)
			if err != nil {
				return err
			}
			v.lockGuard(func() {
				batchRankResponses = append(batchRankResponses, batchRankResponse)
			})
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return v.toRankResult(query, batchRankResponses), nil
}

func (v *client) lockGuard(mutate func()) {
	v.lock.Lock()
	defer v.lock.Unlock()
	mutate()
}

func (v *client) performRank(ctx context.Context, query string, request batchRankRequest,
	cfg moduletools.ClassConfig,
) (*batchRankResponse, error) {
	settings := config.NewClassSettings(cfg)
	cohereUrl, err := url.JoinPath(v.host, v.path)
	if err != nil {
		return nil, errors.Wrap(err, "join Cohere API host and path")
	}

	input := RankInput{
		Documents:       request.documents,
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

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Cohere API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
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
	return &batchRankResponse{
		index:          request.index,
		documentScores: v.toDocumentScores(request.documents, rankResponse.Results),
	}, nil
}

func (v *client) chunkDocuments(documents []string, chunkSize int) []batchRankRequest {
	var requests []batchRankRequest
	index := 0
	for i := 0; i < len(documents); i += chunkSize {
		end := i + chunkSize

		if end > len(documents) {
			end = len(documents)
		}

		requests = append(requests, batchRankRequest{
			index:     index,
			documents: documents[i:end],
		})
		index++
	}

	return requests
}

func (v *client) toDocumentScores(documents []string, results []Result) []ent.DocumentScore {
	documentScores := make([]ent.DocumentScore, len(results))
	for _, result := range results {
		documentScores[result.Index] = ent.DocumentScore{
			Document: documents[result.Index],
			Score:    result.RelevanceScore,
		}
	}
	return documentScores
}

func (v *client) toRankResult(query string, results []*batchRankResponse) *ent.RankResult {
	if len(results) > 1 {
		// we need to reconstruct the order of the document chunks
		// that's why all of the batch rank requests have index field
		// so that we could then reconstruct the order of the documents
		sort.Slice(results, func(i, j int) bool {
			return results[i].index < results[j].index
		})
	}
	documentScores := []ent.DocumentScore{}
	for i := range results {
		documentScores = append(documentScores, results[i].documentScores...)
	}
	return &ent.RankResult{
		Query:          query,
		DocumentScores: documentScores,
	}
}

func (v *client) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	apiKey := ctx.Value("X-Cohere-Api-Key")
	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0], nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Cohere-Api-Key " +
		"nor in environment variable under COHERE_APIKEY")
}

type batchRankRequest struct {
	index     int
	documents []string
}

type batchRankResponse struct {
	index          int
	documentScores []ent.DocumentScore
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
