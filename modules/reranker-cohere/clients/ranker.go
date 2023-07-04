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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-cohere/config"
	"github.com/weaviate/weaviate/modules/reranker-cohere/ent"
)

type client struct {
	apiKey     string
	host       string
	path       string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *client {
	return &client{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		host:       "https://api.cohere.ai",
		path:       "/v1/rerank",
		logger:     logger,
	}
}

func (v *client) Rank(ctx context.Context, cfg moduletools.ClassConfig,
	rankpropertyValue string, query string,
) (*ent.RankResult, error) {
	settings := config.NewClassSettings(cfg)
	cohereUrl, err := url.JoinPath(v.host, v.path)
	if err != nil {
		return nil, errors.Wrap(err, "join Cohere API host and path")
	}

	cohereDocumentsInput := []string{rankpropertyValue}

	input := RankInput{
		RankPropertyValue: cohereDocumentsInput,
		Query:             query,
		Model:             settings.Model(),
		ReturnDocuments:   settings.ReturnDocuments(),
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
	req.Header.Add("Authorization", fmt.Sprintf("BEARER %s", apiKey))
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

	var resBody RankResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		var apiError cohereApiError
		err = json.Unmarshal(bodyBytes, &apiError)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal error from response body")
		}
		return nil, errors.Errorf("fail with status %d: %s", res.StatusCode, apiError.Message)
	}
	return &ent.RankResult{
		RankPropertyValue: rankpropertyValue,
		Query:             query,
		Score:             resBody.Results[0].RelevanceScore,
	}, nil
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

type RankInput struct {
	RankPropertyValue []string `json:"documents"`
	Query             string   `json:"query"`
	Model             string   `json:"model"`
	ReturnDocuments   bool     `json:"return_documents"`
}

type Result struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
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
	Message string `json:"error"`
}
