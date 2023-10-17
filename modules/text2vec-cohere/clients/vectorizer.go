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
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-cohere/ent"
)

type embeddingsRequest struct {
	Input    []string `json:"texts"`
	Model    string   `json:"model,omitempty"`
	Truncate string   `json:"truncate,omitempty"`
}

type embeddingsResponse struct {
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Message    string      `json:"message,omitempty"`
}

type vectorizer struct {
	apiKey     string
	httpClient *http.Client
	urlBuilder *cohereUrlBuilder
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
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
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode >= 500 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Message, "connection to Cohere failed with status: %d error: %v")
		return nil, errors.Errorf(errorMessage)
	} else if res.StatusCode > 200 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Message, "failed with status: %d error: %v")
		return nil, errors.Errorf(errorMessage)
	}

	if len(resBody.Embeddings) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: len(resBody.Embeddings[0]),
		Vectors:    resBody.Embeddings,
	}, nil
}

func getErrorMessage(statusCode int, resBodyError string, errorTemplate string) string {
	if resBodyError != "" {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError)
	}
	return fmt.Sprintf(errorTemplate, statusCode)
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	key := "X-Cohere-Api-Key"

	apiKey := ctx.Value(key)
	// try getting header from GRPC if not successful
	if apiKey == nil {
		apiKey = modulecomponents.GetApiKeyFromGRPC(ctx, key)
	}
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
