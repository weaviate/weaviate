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
	"github.com/weaviate/weaviate/modules/text2vec-palm/ent"
)

func buildURL(apiEndoint, projectID, modelID string) string {
	urlTemplate := "https://%s/v1/projects/%s/locations/us-central1/publishers/google/models/%s:predict"
	return fmt.Sprintf(urlTemplate, apiEndoint, projectID, modelID)
}

type palm struct {
	apiKey       string
	httpClient   *http.Client
	urlBuilderFn func(apiEndoint, projectID, modelID string) string
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

func (v *palm) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *palm) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, config)
}

func (v *palm) vectorize(ctx context.Context, input []string, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	endpointURL := v.urlBuilderFn(v.getApiEndpoint(config), v.getProjectID(config), v.getModel(config))

	instances := make([]instance, len(input))
	for i := range input {
		instances[i] = instance{Content: input[i]}
	}

	body, err := json.Marshal(embeddingsRequest{instances})
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

	if res.StatusCode != 200 || resBody.Error != nil {
		if resBody.Error != nil {
			return nil, fmt.Errorf("connection to Google PaLM failed with status: %v error: %v",
				res.StatusCode, resBody.Error.Message)
		}
		return nil, fmt.Errorf("connection to Google PaLM failed with status: %d", res.StatusCode)
	}

	if len(resBody.Predictions) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	vectors := make([][]float32, len(resBody.Predictions))
	for i := range resBody.Predictions {
		vectors[i] = resBody.Predictions[i].Embeddings.Values
	}

	return &ent.VectorizationResult{
		Texts:      input,
		Dimensions: len(resBody.Predictions[0].Embeddings.Values),
		Vectors:    vectors,
	}, nil
}

func (v *palm) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	key := "X-Palm-Api-Key"
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
		"neither in request header: X-Palm-Api-Key " +
		"nor in environment variable under PALM_APIKEY")
}

func (v *palm) getApiEndpoint(config ent.VectorizationConfig) string {
	return config.ApiEndpoint
}

func (v *palm) getProjectID(config ent.VectorizationConfig) string {
	return config.ProjectID
}

func (v *palm) getModel(config ent.VectorizationConfig) string {
	return config.Model
}

type embeddingsRequest struct {
	Instances []instance `json:"instances,omitempty"`
}

type instance struct {
	Content string `json:"content"`
}

type embeddingsResponse struct {
	Predictions      []prediction  `json:"predictions,omitempty"`
	Error            *palmApiError `json:"error,omitempty"`
	DeployedModelId  string        `json:"deployedModelId,omitempty"`
	Model            string        `json:"model,omitempty"`
	ModelDisplayName string        `json:"modelDisplayName,omitempty"`
	ModelVersionId   string        `json:"modelVersionId,omitempty"`
}

type prediction struct {
	Embeddings       embeddings        `json:"embeddings,omitempty"`
	SafetyAttributes *safetyAttributes `json:"safetyAttributes,omitempty"`
}

type embeddings struct {
	Values []float32 `json:"values,omitempty"`
}

type safetyAttributes struct {
	Scores     []float64 `json:"scores,omitempty"`
	Blocked    *bool     `json:"blocked,omitempty"`
	Categories []string  `json:"categories,omitempty"`
}

type palmApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}
