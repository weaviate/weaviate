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
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/qna-openai/config"
	"github.com/weaviate/weaviate/modules/qna-openai/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func buildUrl(baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
	///X update with base url
	if isAzure {
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
	buildUrlFn         func(baseURL, resourceName, deploymentID string, isAzure bool) (string, error)
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
	metrics := monitoring.GetMetrics()
	startTime := time.Now()
	metrics.ModuleExternalRequests.WithLabelValues("qna", "openai").Inc()

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

	oaiUrl, err := v.buildOpenAIUrl(ctx, settings.BaseURL(), settings.ResourceName(), settings.DeploymentID(), settings.IsAzure())
	if err != nil {
		return nil, errors.Wrap(err, "join OpenAI API host and path")
	}

	defer func() {
		monitoring.GetMetrics().ModuleExternalRequestDuration.WithLabelValues("qna", oaiUrl).Observe(time.Since(startTime).Seconds())
	}()

	v.logger.WithField("URL", oaiUrl).Info("using OpenAI")

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
	if res != nil {
		vrst := monitoring.GetMetrics().ModuleExternalResponseStatus
		vrst.WithLabelValues("qna", oaiUrl, fmt.Sprintf("%v", res.StatusCode)).Inc()
	}
	if err != nil {
		vrst := metrics.ModuleExternalResponseStatus
		vrst.WithLabelValues("qna", oaiUrl, fmt.Sprintf("%v", res.StatusCode)).Inc()
		monitoring.GetMetrics().ModuleExternalError.WithLabelValues("qna", "openai", "OpenAI API", fmt.Sprintf("%v", res.StatusCode)).Inc()
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	requestID := res.Header.Get("x-request-id")
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody answersResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	monitoring.GetMetrics().ModuleExternalResponseSize.WithLabelValues("generate", oaiUrl).Observe(float64(len(bodyBytes)))
	vrst := monitoring.GetMetrics().ModuleExternalResponseStatus
	vrst.WithLabelValues("qna", oaiUrl, fmt.Sprintf("%v", res.StatusCode)).Inc()

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, requestID, resBody.Error, settings.IsAzure())
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

func (v *qna) buildOpenAIUrl(ctx context.Context, baseURL, resourceName, deploymentID string, isAzure bool) (string, error) {
	passedBaseURL := baseURL

	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}

	if headerDeploymentID := modulecomponents.GetValueFromContext(ctx, "X-Azure-Deployment-Id"); headerDeploymentID != "" {
		deploymentID = headerDeploymentID
	}

	if headerResourceName := modulecomponents.GetValueFromContext(ctx, "X-Azure-Resource-Name"); headerResourceName != "" {
		resourceName = headerResourceName
	}

	return v.buildUrlFn(passedBaseURL, resourceName, deploymentID, isAzure)
}

func (v *qna) getError(statusCode int, requestID string, resBodyError *openAIApiError, isAzure bool) error {
	endpoint := "OpenAI API"
	if isAzure {
		endpoint = "Azure OpenAI API"
	}
	errorMsg := fmt.Sprintf("connection to: %s failed with status: %d", endpoint, statusCode)
	if requestID != "" {
		errorMsg = fmt.Sprintf("%s request-id: %s", errorMsg, requestID)
	}
	if resBodyError != nil {
		errorMsg = fmt.Sprintf("%s error: %v", errorMsg, resBodyError.Message)
	}
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("qna", "openai", endpoint, fmt.Sprintf("%v", statusCode)).Inc()
	return errors.New(errorMsg)
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
	var apiKey, envVarValue, envVar string

	if isAzure {
		apiKey = "X-Azure-Api-Key"
		envVar = "AZURE_APIKEY"
		envVarValue = v.azureApiKey
	} else {
		apiKey = "X-Openai-Api-Key"
		envVar = "OPENAI_APIKEY"
		envVarValue = v.openAIApiKey
	}

	return v.getApiKeyFromContext(ctx, apiKey, envVarValue, envVar)
}

func (v *qna) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *qna) getOpenAIOrganization(ctx context.Context) string {
	if value := modulecomponents.GetValueFromContext(ctx, "X-Openai-Organization"); value != "" {
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
	Message string     `json:"message"`
	Type    string     `json:"type"`
	Param   string     `json:"param"`
	Code    openAICode `json:"code"`
}

type openAICode string

func (c *openAICode) String() string {
	if c == nil {
		return ""
	}
	return string(*c)
}

func (c *openAICode) UnmarshalJSON(data []byte) (err error) {
	if number, err := strconv.Atoi(string(data)); err == nil {
		str := strconv.Itoa(number)
		*c = openAICode(str)
		return nil
	}
	var str string
	err = json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*c = openAICode(str)
	return nil
}
