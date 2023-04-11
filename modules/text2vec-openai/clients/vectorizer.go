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
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
)

type embeddingsRequest struct {
	Input string `json:"input"`
	Model string `json:"model,omitempty"`
}

type embedding struct {
	Object string          `json:"object"`
	Data   []embeddingData `json:"data,omitempty"`
	Error  *openAIApiError `json:"error,omitempty"`
}

type embeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

type openAIApiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param"`
	Code    string `json:"code"`
}

type vectorizer struct {
	openAIApiKey string
	azureApiKey  string
	httpClient   *http.Client
	logger       logrus.FieldLogger
}

func New(openAIApiKey string, azureApiKey string, logger logrus.FieldLogger) *vectorizer {

	return &vectorizer{
		openAIApiKey: openAIApiKey,
		azureApiKey:  azureApiKey,
		httpClient:   &http.Client{},
		logger:       logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.getModelString(config.Type, config.Model, "document", config.ModelVersion), config)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input string,
	config ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return v.vectorize(ctx, input, v.getModelString(config.Type, config.Model, "query", config.ModelVersion), config)
}

func (v *vectorizer) vectorize(ctx context.Context, input string, model string, config ent.VectorizationConfig) (*ent.VectorizationResult, error) {

	var body []byte
	var err error
	if config.IsAzure {
		body, err = json.Marshal(embeddingsRequest{
			Input: input,
		})
	} else {
		body, err = json.Marshal(embeddingsRequest{
			Input: input,
			Model: model,
		})
	}

	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	oaiUrl, err := buildUrl(config)
	if err != nil {
		return nil, errors.Wrap(err, "join OpenAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", oaiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx, config.IsAzure)
	if err != nil {
		return nil, errors.Wrapf(err, "API Key")
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

	if res.StatusCode >= 500 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Error, "connection to OpenAI failed with status: %d error: %v")
		return nil, errors.Errorf(errorMessage)
	}
	if res.StatusCode >= 400 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Error, "failed with status: %d")
		return nil, errors.Errorf(errorMessage)
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

func buildUrl(config ent.VectorizationConfig) (string, error) {
	host := "https://api.openai.com"
	path := "/v1/embeddings"
	if config.IsAzure {
		host = "https://" + config.ResourceName + ".openai.azure.com"
		path = "openai/deployments/" + config.DeploymentId + "/embeddings"
	}
	return url.JoinPath(host, path)
}

func getErrorMessage(statusCode int, resBodyError *openAIApiError, errorTemplate string) string {
	if resBodyError != nil {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError.Message)
	}
	return fmt.Sprintf(errorTemplate, statusCode)
}

func (v *vectorizer) getApiKey(ctx context.Context, azure bool) (string, error) {
	var apiKey, envVar string

	if azure {
		apiKey = "X-Azure-Api-Key"
		envVar = "AZURE_APIKEY"
		if len(v.azureApiKey) > 0 {
			return v.azureApiKey, nil
		}
	} else {
		apiKey = "X-Openai-Api-Key"
		envVar = "OPENAI_APIKEY"
		if len(v.openAIApiKey) > 0 {
			return v.openAIApiKey, nil
		}
	}

	return getApiKeyFromContext(ctx, apiKey, envVar)
}

func getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiValue := ctx.Value(apiKey); apiValue != nil {
		if apiKeyHeader, ok := apiValue.([]string); ok && len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
			return apiKeyHeader[0], nil
		}
	}
	return "", errors.New(fmt.Sprintf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar))
}

func (v *vectorizer) getModelString(docType, model, action, version string) string {
	if version == "002" {
		return v.getModel002String(model)
	}

	return v.getModel001String(docType, model, action)
}

func (v *vectorizer) getModel001String(docType, model, action string) string {
	modelBaseString := "%s-search-%s-%s-001"
	if action == "document" {
		if docType == "code" {
			return fmt.Sprintf(modelBaseString, docType, model, "code")
		}
		return fmt.Sprintf(modelBaseString, docType, model, "doc")

	} else {
		if docType == "code" {
			return fmt.Sprintf(modelBaseString, docType, model, "text")
		}
		return fmt.Sprintf(modelBaseString, docType, model, "query")
	}
}

func (v *vectorizer) getModel002String(model string) string {
	modelBaseString := "text-embedding-%s-002"
	return fmt.Sprintf(modelBaseString, model)
}
