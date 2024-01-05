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

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
)

const (
	DefaultOrigin = "https://api-inference.huggingface.co"
	DefaultPath   = "pipeline/feature-extraction"
)

type embeddingsRequest struct {
	Inputs  []string `json:"inputs"`
	Options *options `json:"options,omitempty"`
}

type options struct {
	WaitForModel bool `json:"wait_for_model,omitempty"`
	UseGPU       bool `json:"use_gpu,omitempty"`
	UseCache     bool `json:"use_cache,omitempty"`
}

type embedding [][]float32

type embeddingBert [][][][]float32

type embeddingObject struct {
	Embeddings embedding `json:"embeddings"`
}

type huggingFaceApiError struct {
	Error         string   `json:"error"`
	EstimatedTime *float32 `json:"estimated_time,omitempty"`
	Warnings      []string `json:"warnings"`
}

type vectorizer struct {
	apiKey                string
	httpClient            *http.Client
	bertEmbeddingsDecoder *bertEmbeddingsDecoder
	logger                logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		bertEmbeddingsDecoder: newBertEmbeddingsDecoder(),
		logger:                logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, v.getURL(config), input, v.getOptions(config))
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, v.getURL(config), input, v.getOptions(config))
}

func (v *vectorizer) vectorize(ctx context.Context, url string,
	input string, options options,
) (*ent.VectorizationResult, error) {
	body, err := json.Marshal(embeddingsRequest{
		Inputs:  []string{input},
		Options: &options,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	if apiKey := v.getApiKey(ctx); apiKey != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	}
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

	if err := checkResponse(res, bodyBytes); err != nil {
		return nil, err
	}

	vector, err := v.decodeVector(bodyBytes)
	if err != nil {
		return nil, errors.Wrap(err, "cannot decode vector")
	}

	return &ent.VectorizationResult{
		Text:       input,
		Dimensions: len(vector),
		Vector:     vector,
	}, nil
}

func checkResponse(res *http.Response, bodyBytes []byte) error {
	if res.StatusCode < 400 {
		return nil
	}

	var resBody huggingFaceApiError
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return fmt.Errorf("unmarshal error response body: %v", string(bodyBytes))
	}

	message := fmt.Sprintf("failed with status: %d", res.StatusCode)
	if resBody.Error != "" {
		message = fmt.Sprintf("%s error: %v", message, resBody.Error)
		if resBody.EstimatedTime != nil {
			message = fmt.Sprintf("%s estimated time: %v", message, *resBody.EstimatedTime)
		}
		if len(resBody.Warnings) > 0 {
			message = fmt.Sprintf("%s warnings: %v", message, resBody.Warnings)
		}
	}

	if res.StatusCode == http.StatusInternalServerError {
		message = fmt.Sprintf("connection to HuggingFace %v", message)
	}

	return errors.New(message)
}

func (v *vectorizer) decodeVector(bodyBytes []byte) ([]float32, error) {
	var emb embedding
	if err := json.Unmarshal(bodyBytes, &emb); err != nil {
		var embObject embeddingObject
		if err := json.Unmarshal(bodyBytes, &embObject); err != nil {
			var embBert embeddingBert
			if err := json.Unmarshal(bodyBytes, &embBert); err != nil {
				return nil, errors.Wrap(err, "unmarshal response body")
			}

			if len(embBert) == 1 && len(embBert[0]) == 1 {
				return v.bertEmbeddingsDecoder.calculateVector(embBert[0][0])
			}

			return nil, errors.New("unprocessable response body")
		}
		if len(embObject.Embeddings) == 1 {
			return embObject.Embeddings[0], nil
		}

		return nil, errors.New("unprocessable response body")
	}

	if len(emb) == 1 {
		return emb[0], nil
	}

	return nil, errors.New("unprocessable response body")
}

func (v *vectorizer) getApiKey(ctx context.Context) string {
	if len(v.apiKey) > 0 {
		return v.apiKey
	}
	key := "X-Huggingface-Api-Key"
	apiKey := ctx.Value(key)
	// try getting header from GRPC if not successful
	if apiKey == nil {
		apiKey = modulecomponents.GetValueFromGRPC(ctx, key)
	}

	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0]
	}
	return ""
}

func (v *vectorizer) getOptions(config ent.VectorizationConfig) options {
	return options{
		WaitForModel: config.WaitForModel,
		UseGPU:       config.UseGPU,
		UseCache:     config.UseCache,
	}
}

func (v *vectorizer) getURL(config ent.VectorizationConfig) string {
	if config.EndpointURL != "" {
		return config.EndpointURL
	}

	return fmt.Sprintf("%s/%s/%s", DefaultOrigin, DefaultPath, config.Model)
}
