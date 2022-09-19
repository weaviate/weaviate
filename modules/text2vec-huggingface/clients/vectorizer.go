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
	"github.com/semi-technologies/weaviate/modules/text2vec-huggingface/ent"
	"github.com/sirupsen/logrus"
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

type huggingFaceApiError struct {
	Error         string   `json:"error"`
	EstimatedTime *float32 `json:"estimated_time,omitempty"`
	Warnings      []string `json:"warnings"`
}

type vectorizer struct {
	apiKey                string
	httpClient            *http.Client
	urlBuilder            *huggingFaceUrlBuilder
	bertEmbeddingsDecoder *bertEmbeddingsDecoder
	logger                logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		apiKey:                apiKey,
		httpClient:            &http.Client{},
		urlBuilder:            newHuggingFaceUrlBuilder(),
		bertEmbeddingsDecoder: newBertEmbeddingsDecoder(),
		logger:                logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, v.url(config.Model), input, v.getOptions(config))
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, v.url(config.Model), input, v.getOptions(config))
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
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "HuggingFace API Key")
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

	if res.StatusCode > 399 {
		var resBody huggingFaceApiError
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return nil, errors.Wrapf(err, "unmarshal error response body: %v", string(bodyBytes))
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
		return nil, errors.New(message)
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

func (v *vectorizer) decodeVector(bodyBytes []byte) ([]float32, error) {
	var emb embedding
	if err := json.Unmarshal(bodyBytes, &emb); err != nil {
		var embBert embeddingBert
		if err := json.Unmarshal(bodyBytes, &embBert); err != nil {
			return nil, errors.Wrap(err, "unmarshal response body")
		}

		if len(embBert) == 1 && len(embBert[0]) == 1 {
			return v.bertEmbeddingsDecoder.calculateVector(embBert[0][0])
		}

		return nil, errors.New("unprocessable response body")
	}

	if len(emb) == 1 {
		return emb[0], nil
	}

	return nil, errors.New("unprocessable response body")
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	apiKey := ctx.Value("X-Huggingface-Api-Key")
	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0], nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-HuggingFace-Api-Key " +
		"nor in environment variable under HUGGINGFACE_APIKEY")
}

func (v *vectorizer) url(model string) string {
	return v.urlBuilder.url(model)
}

func (v *vectorizer) getOptions(config ent.VectorizationConfig) options {
	return options{
		WaitForModel: config.WaitForModel,
		UseGPU:       config.UseGPU,
		UseCache:     config.UseCache,
	}
}
