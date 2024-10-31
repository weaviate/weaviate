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
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-google/ent"
)

type taskType string

var (
	// Specifies the given text is a document in a search/retrieval setting
	retrievalQuery taskType = "RETRIEVAL_QUERY"
	// Specifies the given text is a query in a search/retrieval setting
	retrievalDocument taskType = "RETRIEVAL_DOCUMENT"
)

func buildURL(useGenerativeAI bool, apiEndoint, projectID, modelID string) string {
	if useGenerativeAI {
		if isLegacyModel(modelID) {
			// legacy PaLM API
			return "https://generativelanguage.googleapis.com/v1beta3/models/embedding-gecko-001:batchEmbedText"
		}
		return fmt.Sprintf("https://generativelanguage.googleapis.com/v1/models/%s:batchEmbedContents", modelID)
	}
	urlTemplate := "https://%s/v1/projects/%s/locations/us-central1/publishers/google/models/%s:predict"
	return fmt.Sprintf(urlTemplate, apiEndoint, projectID, modelID)
}

type google struct {
	apiKey        string
	googleApiKey  *apikey.GoogleApiKey
	useGoogleAuth bool
	httpClient    *http.Client
	urlBuilderFn  func(useGenerativeAI bool, apiEndoint, projectID, modelID string) string
	logger        logrus.FieldLogger
}

func New(apiKey string, useGoogleAuth bool, timeout time.Duration, logger logrus.FieldLogger) *google {
	return &google{
		apiKey:        apiKey,
		useGoogleAuth: useGoogleAuth,
		googleApiKey:  apikey.NewGoogleApiKey(),
		httpClient: &http.Client{
			Timeout: timeout,
		},
		urlBuilderFn: buildURL,
		logger:       logger,
	}
}

func (v *google) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig, titlePropertyValue string,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, retrievalDocument, titlePropertyValue, config)
}

func (v *google) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, retrievalQuery, "", config)
}

func (v *google) vectorize(ctx context.Context, input []string, taskType taskType,
	titlePropertyValue string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	useGenerativeAIEndpoint := v.useGenerativeAIEndpoint(config)

	payload := v.getPayload(useGenerativeAIEndpoint, input, taskType, titlePropertyValue, config)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	endpointURL := v.urlBuilderFn(useGenerativeAIEndpoint,
		v.getApiEndpoint(config), v.getProjectID(config), v.getModel(config))

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx, useGenerativeAIEndpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "Google API Key")
	}
	req.Header.Add("Content-Type", "application/json")
	if useGenerativeAIEndpoint {
		req.Header.Add("x-goog-api-key", apiKey)
	} else {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
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

	if useGenerativeAIEndpoint {
		return v.parseGenerativeAIApiResponse(res.StatusCode, bodyBytes, input, config)
	}
	return v.parseEmbeddingsResponse(res.StatusCode, bodyBytes, input)
}

func (v *google) useGenerativeAIEndpoint(config ent.VectorizationConfig) bool {
	return v.getApiEndpoint(config) == "generativelanguage.googleapis.com"
}

func (v *google) getPayload(useGenerativeAI bool, input []string,
	taskType taskType, title string, config ent.VectorizationConfig,
) interface{} {
	if useGenerativeAI {
		if v.isLegacy(config) {
			return batchEmbedTextRequestLegacy{Texts: input}
		}
		parts := make([]part, len(input))
		for i := range input {
			parts[i] = part{Text: input[i]}
		}
		req := batchEmbedContents{
			Requests: []embedContentRequest{
				{
					Model: fmt.Sprintf("models/%s", config.Model),
					Content: content{
						Parts: parts,
					},
					TaskType: taskType,
					Title:    title,
				},
			},
		}
		return req
	}
	isModelVersion001 := strings.HasSuffix(config.Model, "@001")
	instances := make([]instance, len(input))
	for i := range input {
		if isModelVersion001 {
			instances[i] = instance{Content: input[i]}
		} else {
			instances[i] = instance{Content: input[i], TaskType: taskType, Title: title}
		}
	}
	return embeddingsRequest{instances}
}

func (v *google) checkResponse(statusCode int, googleApiError *googleApiError) error {
	if statusCode != 200 || googleApiError != nil {
		if googleApiError != nil {
			return fmt.Errorf("connection to Google failed with status: %v error: %v",
				statusCode, googleApiError.Message)
		}
		return fmt.Errorf("connection to Google failed with status: %d", statusCode)
	}
	return nil
}

func (v *google) getApiKey(ctx context.Context, useGenerativeAIEndpoint bool) (string, error) {
	return v.googleApiKey.GetApiKey(ctx, v.apiKey, useGenerativeAIEndpoint, v.useGoogleAuth)
}

func (v *google) parseGenerativeAIApiResponse(statusCode int,
	bodyBytes []byte, input []string, config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	var resBody batchEmbedTextResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Embeddings) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	vectors := make([][]float32, len(resBody.Embeddings))
	for i := range resBody.Embeddings {
		if v.isLegacy(config) {
			vectors[i] = resBody.Embeddings[i].Value
		} else {
			vectors[i] = resBody.Embeddings[i].Values
		}
	}
	dimensions := len(resBody.Embeddings[0].Values)
	if v.isLegacy(config) {
		dimensions = len(resBody.Embeddings[0].Value)
	}

	return v.getResponse(input, dimensions, vectors)
}

func (v *google) parseEmbeddingsResponse(statusCode int,
	bodyBytes []byte, input []string,
) (*ent.VectorizationResult, error) {
	var resBody embeddingsResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if err := v.checkResponse(statusCode, resBody.Error); err != nil {
		return nil, err
	}

	if len(resBody.Predictions) == 0 {
		return nil, errors.Errorf("empty embeddings response")
	}

	vectors := make([][]float32, len(resBody.Predictions))
	for i := range resBody.Predictions {
		vectors[i] = resBody.Predictions[i].Embeddings.Values
	}
	dimensions := len(resBody.Predictions[0].Embeddings.Values)

	return v.getResponse(input, dimensions, vectors)
}

func (v *google) getResponse(input []string, dimensions int, vectors [][]float32) (*ent.VectorizationResult, error) {
	return &ent.VectorizationResult{
		Texts:      input,
		Dimensions: dimensions,
		Vectors:    vectors,
	}, nil
}

func (v *google) getApiEndpoint(config ent.VectorizationConfig) string {
	return config.ApiEndpoint
}

func (v *google) getProjectID(config ent.VectorizationConfig) string {
	return config.ProjectID
}

func (v *google) getModel(config ent.VectorizationConfig) string {
	return config.Model
}

func (v *google) isLegacy(config ent.VectorizationConfig) bool {
	return isLegacyModel(config.Model)
}

func isLegacyModel(model string) bool {
	// Check if we are using legacy model which runs on deprecated PaLM API
	return model == "embedding-gecko-001"
}

type embeddingsRequest struct {
	Instances []instance `json:"instances,omitempty"`
}

type instance struct {
	Content  string   `json:"content"`
	TaskType taskType `json:"task_type,omitempty"`
	Title    string   `json:"title,omitempty"`
}

type embeddingsResponse struct {
	Predictions      []prediction    `json:"predictions,omitempty"`
	Error            *googleApiError `json:"error,omitempty"`
	DeployedModelId  string          `json:"deployedModelId,omitempty"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	ModelVersionId   string          `json:"modelVersionId,omitempty"`
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

type googleApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type batchEmbedTextResponse struct {
	Embeddings []embedding     `json:"embeddings,omitempty"`
	Error      *googleApiError `json:"error,omitempty"`
}

type embedding struct {
	Values []float32 `json:"values,omitempty"`
	// Legacy PaLM API
	Value []float32 `json:"value,omitempty"`
}

type batchEmbedContents struct {
	Requests []embedContentRequest `json:"requests,omitempty"`
}

type embedContentRequest struct {
	Model    string   `json:"model"`
	Content  content  `json:"content"`
	TaskType taskType `json:"task_type,omitempty"`
	Title    string   `json:"title,omitempty"`
}

type content struct {
	Parts []part `json:"parts,omitempty"`
	Role  string `json:"role,omitempty"`
}

type part struct {
	Text string `json:"text,omitempty"`
}

// Legacy PaLM API
type batchEmbedTextRequestLegacy struct {
	Texts []string `json:"texts,omitempty"`
}
