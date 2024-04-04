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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-ollama/ent"
)

func buildURL(apiEndoint string) string {
	return fmt.Sprintf("%s/api/embeddings", apiEndoint)
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

func (v *ollama) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *ollama) vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(embeddingsRequest{
		Model:  v.getModel(config),
		Prompt: input,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	endpointURL := v.urlBuilderFn(v.getApiEndpoint(config))

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
	bodyBytes []byte, input string,
) (*ent.VectorizationResult, error) {
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if resBody.Error != "" {
		return nil, errors.Errorf("connection to Ollama API failed with error: %s", resBody.Error)
	}

	if statusCode != 200 {
		return nil, fmt.Errorf("connection to Ollama API failed with status: %d", statusCode)
	}

	if len(resBody.Embedding) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: len(resBody.Embedding),
		Vector:     resBody.Embedding,
	}, nil
}

func (v *ollama) getApiEndpoint(config ent.VectorizationConfig) string {
	return config.ApiEndpoint
}

func (v *ollama) getModel(config ent.VectorizationConfig) string {
	return config.Model
}

type embeddingsRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type embeddingsResponse struct {
	Embedding []float32 `json:"embedding,omitempty"`
	Error     string    `json:"error,omitempty"`
}
