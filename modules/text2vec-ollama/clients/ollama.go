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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/modules/text2vec-ollama/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

func buildURL(apiEndoint string) string {
	return fmt.Sprintf("%s/api/embed", apiEndoint)
}

type ollama struct {
	httpClient   *http.Client
	urlBuilderFn func(apiEndoint string) string
	logger       logrus.FieldLogger
}

func New(timeout time.Duration, logger logrus.FieldLogger) *ollama {
	return &ollama{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilderFn: buildURL,
		logger:       logger,
	}
}

func (v *ollama) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, *modulecomponents.RateLimits, int, error) {
	res, err := v.vectorize(ctx, input, cfg)
	return res, nil, 0, err
}

func (v *ollama) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	return v.vectorize(ctx, input, cfg)
}

func (v *ollama) GetApiKeyHash(ctx context.Context, config moduletools.ClassConfig) [32]byte {
	return sha256.Sum256([]byte("ollama"))
}

func (v *ollama) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	// TODO: What to put here as rate limits?
	return &modulecomponents.RateLimits{}
}

func (v *ollama) vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult, error) {
	settings := ent.NewClassSettings(cfg)
	body, err := json.Marshal(embeddingsRequest{
		Model: settings.Model(),
		Input: input,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	endpointURL := v.urlBuilderFn(settings.ApiEndpoint())

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	return v.parseEmbeddingsResponse(res.StatusCode, bodyBytes, input)
}

func (v *ollama) parseEmbeddingsResponse(statusCode int,
	bodyBytes []byte, input []string,
) (*modulecomponents.VectorizationResult, error) {
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, fmt.Errorf("unmarshal response body. Got: %v: %w", string(bodyBytes), err)
	}

	if resBody.Error != "" {
		return nil, errors.Errorf("connection to Ollama API failed with error: %s", resBody.Error)
	}

	if statusCode != 200 {
		return nil, fmt.Errorf("connection to Ollama API failed with status: %d", statusCode)
	}

	if len(resBody.Embeddings) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &modulecomponents.VectorizationResult{
		Text:       input,
		Vector:     resBody.Embeddings,
		Dimensions: len(resBody.Embeddings[0]),
	}, nil
}

type embeddingsRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingsResponse struct {
	Model      string      `json:"model"`
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Error      string      `json:"error,omitempty"`
}
