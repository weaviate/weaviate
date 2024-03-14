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
	"github.com/weaviate/weaviate/modules/multi2vec-palm/ent"
)

func buildURL(location, projectID, model string) string {
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		location, projectID, location, model)
}

type palm struct {
	apiKey       string
	httpClient   *http.Client
	urlBuilderFn func(location, projectID, model string) string
	logger       logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *palm {
	return &palm{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilderFn: buildURL,
		logger:       logger,
	}
}

func (v *palm) Vectorize(ctx context.Context,
	texts, images []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, texts, images, config)
}

func (v *palm) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, nil, config)
}

func (v *palm) vectorize(ctx context.Context,
	texts, images []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	endpointURL := v.getURL(config)
	payload := v.getPayload(texts, images, config.Dimensions)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Palm API Key")
	}
	req.Header.Add("Content-Type", "application/json; charset=utf-8")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	return v.parseEmbeddingsResponse(res.StatusCode, bodyBytes)
}

func (v *palm) getURL(config ent.VectorizationConfig) string {
	return v.urlBuilderFn(config.Location, config.ProjectID, config.Model)
}

func (v *palm) getPayload(texts, images []string, dimensions int64) embeddingsRequest {
	instances := make([]instance, 0)
	for _, text := range texts {
		instances = append(instances, instance{Text: &text})
	}
	for _, img := range images {
		instances = append(instances, instance{Image: &image{BytesBase64Encoded: img}})
	}
	return embeddingsRequest{
		Instances:  instances,
		Parameters: parameters{Dimension: dimensions},
	}
}

func (v *palm) checkResponse(statusCode int, palmApiError *palmApiError) error {
	if statusCode != 200 || palmApiError != nil {
		if palmApiError != nil {
			return fmt.Errorf("connection to Google PaLM failed with status: %v error: %v",
				statusCode, palmApiError.Message)
		}
		return fmt.Errorf("connection to Google PaLM failed with status: %d", statusCode)
	}
	return nil
}

func (v *palm) getApiKey(ctx context.Context) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, "X-Palm-Api-Key"); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Palm-Api-Key " +
		"nor in environment variable under PALM_APIKEY")
}

func (v *palm) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if apiKey := modulecomponents.GetValueFromGRPC(ctx, key); len(apiKey) > 0 && len(apiKey[0]) > 0 {
		return apiKey[0]
	}
	return ""
}

func (v *palm) parseEmbeddingsResponse(statusCode int,
	bodyBytes []byte,
) (*ent.VectorizationResult, error) {
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Predictions) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}
	var textEmbeddings [][]float32
	var imageEmbeddings [][]float32

	for _, p := range resBody.Predictions {
		if len(p.TextEmbedding) > 0 {
			textEmbeddings = append(textEmbeddings, p.TextEmbedding)
		}
		if len(p.ImageEmbedding) > 0 {
			imageEmbeddings = append(imageEmbeddings, p.ImageEmbedding)
		}
	}
	return v.getResponse(textEmbeddings, imageEmbeddings)
}

func (v *palm) getResponse(textVectors, imageVectors [][]float32) (*ent.VectorizationResult, error) {
	return &ent.VectorizationResult{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}, nil
}

type embeddingsRequest struct {
	Instances  []instance `json:"instances,omitempty"`
	Parameters parameters `json:"parameters,omitempty"`
}

type parameters struct {
	Dimension int64 `json:"dimension,omitempty"`
}

type instance struct {
	Text  *string `json:"text,omitempty"`
	Image *image  `json:"image,omitempty"`
}

type image struct {
	BytesBase64Encoded string `json:"bytesBase64Encoded"`
}

type embeddingsResponse struct {
	Predictions     []prediction  `json:"predictions,omitempty"`
	Error           *palmApiError `json:"error,omitempty"`
	DeployedModelId string        `json:"deployedModelId,omitempty"`
}

type prediction struct {
	TextEmbedding  []float32 `json:"textEmbedding,omitempty"`
	ImageEmbedding []float32 `json:"imageEmbedding,omitempty"`
}

type palmApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}
