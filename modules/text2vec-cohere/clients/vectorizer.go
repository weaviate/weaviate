//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/modules/text2vec-cohere/ent"
	"github.com/sirupsen/logrus"
)

type embeddingsRequest struct {
	Input    []string `json:"texts"`
	Model    string   `json:"model,omitempty"`
	Truncate string   `json:"truncate,omitempty"`
}

type embedding struct {
	Object string          `json:"object"`
	Data   []embeddingData `json:"data,omitempty"`
	Error  *cohereApiError `json:"error,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

type cohereApiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param"`
	Code    string `json:"code"`
}

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *cohereUrlBuilder
	logger     logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		urlBuilder: newCohereUrlBuilder(),
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.url(), v.getModel(config), v.getTruncate(config))
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.url(), v.getModel(config), v.getTruncate(config))
}

func (v *vectorizer) vectorize(ctx context.Context, input []string,
	url string, model string, truncate string,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(embeddingsRequest{
		Input:    input,
		Model:    model,
		Truncate: truncate,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
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

	var resBody embedding
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode > 399 {
		if resBody.Error != nil {
			return nil, errors.Errorf("failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("failed with status: %d", res.StatusCode)
	}

	if len(resBody.Data) != 1 {
		return nil, errors.Errorf("wrong number of embeddings: %v", len(resBody.Data))
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: len(resBody.Data[0].Embedding),
		Vector:     resBody.Data[0].Embedding,
	}, nil
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
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

func (v *vectorizer) url() string {
	return v.urlBuilder.url()
}

func (v *vectorizer) getModel(config ent.VectorizationConfig) string {
	return config.Model
}

func (v *vectorizer) getTruncate(config ent.VectorizationConfig) string {
	return config.Truncate
}
