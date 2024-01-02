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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
	"golang.org/x/sync/errgroup"
)

var _NUMCPU = runtime.NumCPU()

type client struct {
	lock         sync.RWMutex
	origin       string
	httpClient   *http.Client
	maxDocuments int
	logger       logrus.FieldLogger
}

func New(origin string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		origin:       origin,
		httpClient:   &http.Client{Timeout: timeout},
		maxDocuments: 32,
		logger:       logger,
	}
}

func (c *client) Rank(ctx context.Context,
	query string, documents []string, cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	eg := &errgroup.Group{}
	eg.SetLimit(_NUMCPU)

	chunkedDocuments := c.chunkDocuments(documents, c.maxDocuments)
	documentScoreResponses := make([][]DocumentScore, len(chunkedDocuments))
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

func (c *client) toRankResult(query string, scores [][]DocumentScore) *ent.RankResult {
	documentScores := []ent.DocumentScore{}
	for _, docScores := range scores {
		for i := range docScores {
			documentScores = append(documentScores, ent.DocumentScore{
				Document: docScores[i].Document,
				Score:    docScores[i].Score,
			})
		}
	}
	return &ent.RankResult{
		Query:          query,
		DocumentScores: documentScores,
	}
}

func (c *client) performRank(ctx context.Context,
	query string, documents []string, cfg moduletools.ClassConfig,
) ([]DocumentScore, error) {
	body, err := json.Marshal(RankInput{
		Query:     query,
		Documents: documents,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url("/rerank"),
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

	var resBody RankResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 {
		if resBody.Error != "" {
			return nil, errors.Errorf("fail with status %d: %s", res.StatusCode,
				resBody.Error)
		}
		return nil, errors.Errorf("fail with status %d", res.StatusCode)
	}

	return resBody.Scores, nil
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

func (c *client) url(path string) string {
	return fmt.Sprintf("%s%s", c.origin, path)
}

type RankInput struct {
	Query             string   `json:"query"`
	Documents         []string `json:"documents"`
	RankPropertyValue string   `json:"property"`
}

type DocumentScore struct {
	Document string  `json:"document"`
	Score    float64 `json:"score"`
}

type RankResponse struct {
	Query             string          `json:"query"`
	Scores            []DocumentScore `json:"scores"`
	RankPropertyValue string          `json:"property"`
	Score             float64         `json:"score"`
	Error             string          `json:"error"`
}
