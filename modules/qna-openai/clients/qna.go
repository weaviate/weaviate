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
	"net/url"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/qna-openai/config"
	"github.com/weaviate/weaviate/modules/qna-openai/ent"
)

func buildUrl(baseURL, resourceName, deploymentID string) (string, error) {
	///X update with base url
	if resourceName != "" && deploymentID != "" {
		host := "https://" + resourceName + ".openai.azure.com"
		path := "openai/deployments/" + deploymentID + "/completions"
		queryParam := "api-version=2022-12-01"
		return fmt.Sprintf("%s/%s?%s", host, path, queryParam), nil
	}
	host := baseURL
	path := "/v1/completions"
	return url.JoinPath(host, path)
}

type qna struct {
	openAIApiKey       string
	openAIOrganization string
	azureApiKey        string
	buildUrlFn         func(baseURL, resourceName, deploymentID string) (string, error)
	httpClient         *http.Client
	logger             logrus.FieldLogger
}

func New(openAIApiKey, openAIOrganization, azureApiKey string, timeout time.Duration, logger logrus.FieldLogger) *qna {
	return &qna{
		openAIApiKey:       openAIApiKey,
		openAIOrganization: openAIOrganization,
		azureApiKey:        azureApiKey,
		httpClient:         &http.Client{Timeout: timeout},
		buildUrlFn:         buildUrl,
		logger:             logger,
	}
}

func (v *qna) Answer(ctx context.Context, text, question string, cfg moduletools.ClassConfig) (*ent.AnswerResult, error) {
	prompt := v.generatePrompt(text, question)

	settings := config.NewClassSettings(cfg)

	body, err := json.Marshal(answersInput{
		Prompt:           prompt,
		Model:            settings.Model(),
		MaxTokens:        settings.MaxTokens(),
		Temperature:      settings.Temperature(),
		Stop:             []string{"\n"},
		FrequencyPenalty: settings.FrequencyPenalty(),
		PresencePenalty:  settings.PresencePenalty(),
		TopP:             settings.TopP(),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	oaiUrl, err := v.buildOpenAIUrl(ctx, settings.BaseURL(), settings.ResourceName(), settings.DeploymentID())
	if err != nil {
		return nil, errors.Wrap(err, "join OpenAI API host and path")
	}
	fmt.Printf("using the OpenAI URL: %v\n", oaiUrl)
	req, err := http.NewRequestWithContext(ctx, "POST", oaiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx, settings.IsAzure())
	if err != nil {
		return nil, errors.Wrapf(err, "OpenAI API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey, settings.IsAzure()))
	if openAIOrganization := v.getOpenAIOrganization(ctx); openAIOrganization != "" {
		req.Header.Add("OpenAI-Organization", openAIOrganization)
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

	var resBody answersResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, resBody.Error, settings.IsAzure())
	}

	if len(resBody.Choices) > 0 && resBody.Choices[0].Text != "" {
		return &ent.AnswerResult{
			Text:     text,
			Question: question,
			Answer:   &resBody.Choices[0].Text,
		}, nil
	}
	return &ent.AnswerResult{
		Text:     text,
		Question: question,
		Answer:   nil,
	}, nil
}

func (v *qna) buildOpenAIUrl(ctx context.Context, baseURL, resourceName, deploymentID string) (string, error) {
	passedBaseURL := baseURL
	if headerBaseURL := v.getValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return v.buildUrlFn(passedBaseURL, resourceName, deploymentID)
}

func (v *qna) getError(statusCode int, resBodyError *openAIApiError, isAzure bool) error {
	endpoint := "OpenAI API"
	if isAzure {
		endpoint = "Azure OpenAI API"
	}
	if resBodyError != nil {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBodyError.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *qna) getApiKeyHeaderAndValue(apiKey string, isAzure bool) (string, string) {
	if isAzure {
		return "api-key", apiKey
	}
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *qna) generatePrompt(text string, question string) string {
	return fmt.Sprintf(`'Please answer the question according to the above context.

===
Context: %v
===
Q: %v
A:`, strings.ReplaceAll(text, "\n", " "), question)
}

func (v *qna) getApiKey(ctx context.Context, isAzure bool) (string, error) {
	var apiKey, envVar string

	if isAzure {
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

	return v.getApiKeyFromContext(ctx, apiKey, envVar)
}

func (v *qna) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *qna) getValueFromContext(ctx context.Context, key string) string {
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

func (v *qna) getOpenAIOrganization(ctx context.Context) string {
	if value := v.getValueFromContext(ctx, "X-Openai-Organization"); value != "" {
		return value
	}
	return v.openAIOrganization
}

type answersInput struct {
	Prompt           string   `json:"prompt"`
	Model            string   `json:"model"`
	MaxTokens        float64  `json:"max_tokens"`
	Temperature      float64  `json:"temperature"`
	Stop             []string `json:"stop"`
	FrequencyPenalty float64  `json:"frequency_penalty"`
	PresencePenalty  float64  `json:"presence_penalty"`
	TopP             float64  `json:"top_p"`
}

type answersResponse struct {
	Choices []choice
	Error   *openAIApiError `json:"error,omitempty"`
}

type choice struct {
	FinishReason string
	Index        float32
	Logprobs     string
	Text         string
}

type openAIApiError struct {
	Message string      `json:"message"`
	Type    string      `json:"type"`
	Param   string      `json:"param"`
	Code    json.Number `json:"code"`
}
