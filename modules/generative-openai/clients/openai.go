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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-openai/config"
	openaiparams "github.com/weaviate/weaviate/modules/generative-openai/parameters"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildUrlFn(isLegacy, isAzure bool, resourceName, deploymentID, baseURL, apiVersion string) (string, error) {
	if isAzure {
		host := baseURL
		if host == "" || host == "https://api.openai.com" {
			// Fall back to old assumption
			host = "https://" + resourceName + ".openai.azure.com"
		}
		path := "openai/deployments/" + deploymentID + "/chat/completions"
		queryParam := fmt.Sprintf("api-version=%s", apiVersion)
		return fmt.Sprintf("%s/%s?%s", host, path, queryParam), nil
	}
	path := "/v1/chat/completions"
	if isLegacy {
		path = "/v1/completions"
	}
	return url.JoinPath(baseURL, path)
}

type openai struct {
	openAIApiKey       string
	openAIOrganization string
	azureApiKey        string
	buildUrl           func(isLegacy, isAzure bool, resourceName, deploymentID, baseURL, apiVersion string) (string, error)
	httpClient         *http.Client
	logger             logrus.FieldLogger
}

func New(openAIApiKey, openAIOrganization, azureApiKey string, timeout time.Duration, logger logrus.FieldLogger) *openai {
	return &openai{
		openAIApiKey:       openAIApiKey,
		openAIOrganization: openAIOrganization,
		azureApiKey:        azureApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrl: buildUrlFn,
		logger:   logger,
	}
}

func (v *openai) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *openai) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *openai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	isAzure := config.IsAzure(params.IsAzure, params.ResourceName, params.DeploymentID)
	debugInformation := v.getDebugInformation(debug, prompt)

	oaiUrl, err := v.buildOpenAIUrl(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "url join path")
	}

	input, err := v.generateInput(prompt, params)
	if err != nil {
		return nil, errors.Wrap(err, "generate input")
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", oaiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx, isAzure)
	if err != nil {
		return nil, errors.Wrapf(err, "OpenAI API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey, isAzure))
	if openAIOrganization := v.getOpenAIOrganization(ctx); openAIOrganization != "" {
		req.Header.Add("OpenAI-Organization", openAIOrganization)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	requestID := res.Header.Get("x-request-id")
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, requestID, resBody.Error, params.IsAzure)
	}

	responseParams := v.getResponseParams(resBody.Usage)
	textResponse := resBody.Choices[0].Text
	if len(resBody.Choices) > 0 && textResponse != "" {
		trimmedResponse := strings.Trim(textResponse, "\n")
		return &modulecapabilities.GenerateResponse{
			Result: &trimmedResponse,
			Debug:  debugInformation,
			Params: responseParams,
		}, nil
	}

	message := resBody.Choices[0].Message
	if message != nil {
		textResponse = message.Content
		trimmedResponse := strings.Trim(textResponse, "\n")
		return &modulecapabilities.GenerateResponse{
			Result: &trimmedResponse,
			Debug:  debugInformation,
			Params: responseParams,
		}, nil
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debugInformation,
	}, nil
}

func (v *openai) getParameters(cfg moduletools.ClassConfig, options interface{}) openaiparams.Params {
	settings := config.NewClassSettings(cfg)

	var params openaiparams.Params
	if p, ok := options.(openaiparams.Params); ok {
		params = p
	}

	if params.BaseURL == "" {
		params.BaseURL = settings.BaseURL()
	}
	if params.ApiVersion == "" {
		params.ApiVersion = settings.ApiVersion()
	}
	if params.ResourceName == "" {
		params.ResourceName = settings.ResourceName()
	}
	if params.DeploymentID == "" {
		params.DeploymentID = settings.DeploymentID()
	}
	if !params.IsAzure {
		params.IsAzure = settings.IsAzure()
	}
	if params.Model == "" {
		params.Model = settings.Model()
	}
	if params.Temperature == nil {
		temperature := settings.Temperature()
		params.Temperature = &temperature
	}
	if params.TopP == nil {
		topP := settings.TopP()
		params.TopP = &topP
	}
	if params.FrequencyPenalty == nil {
		frequencyPenalty := settings.FrequencyPenalty()
		params.FrequencyPenalty = &frequencyPenalty
	}
	if params.PresencePenalty == nil {
		presencePenalty := settings.PresencePenalty()
		params.PresencePenalty = &presencePenalty
	}
	if params.MaxTokens == nil {
		maxTokens := int(settings.MaxTokens())
		params.MaxTokens = &maxTokens
	}
	return params
}

func (v *openai) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *openai) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{openaiparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[openaiparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (v *openai) buildOpenAIUrl(ctx context.Context, params openaiparams.Params) (string, error) {
	baseURL := params.BaseURL

	deploymentID := params.DeploymentID
	resourceName := params.ResourceName

	if headerBaseURL := v.getValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}

	if headerDeploymentID := v.getValueFromContext(ctx, "X-Azure-Deployment-Id"); headerDeploymentID != "" {
		deploymentID = headerDeploymentID
	}

	if headerResourceName := v.getValueFromContext(ctx, "X-Azure-Resource-Name"); headerResourceName != "" {
		resourceName = headerResourceName
	}

	isLegacy := config.IsLegacy(params.Model)
	isAzure := config.IsAzure(params.IsAzure, resourceName, deploymentID)

	return v.buildUrl(isLegacy, isAzure, resourceName, deploymentID, baseURL, params.ApiVersion)
}

func (v *openai) generateInput(prompt string, params openaiparams.Params) (generateInput, error) {
	if config.IsLegacy(params.Model) {
		return generateInput{
			Prompt:           prompt,
			Model:            params.Model,
			FrequencyPenalty: params.FrequencyPenalty,
			MaxTokens:        params.MaxTokens,
			N:                params.N,
			PresencePenalty:  params.PresencePenalty,
			Stop:             params.Stop,
			Temperature:      params.Temperature,
			TopP:             params.TopP,
		}, nil
	} else {
		var input generateInput
		messages := []message{{
			Role:    "user",
			Content: prompt,
		}}

		var tokens *int
		var err error
		if config.IsThirdPartyProvider(params.BaseURL, params.IsAzure, params.ResourceName, params.DeploymentID) {
			tokens, err = v.determineTokens(config.GetMaxTokensForModel(params.Model), *params.MaxTokens, params.Model, messages)
		} else {
			tokens = params.MaxTokens
		}

		if err != nil {
			return input, errors.Wrap(err, "determine tokens count")
		}
		input = generateInput{
			Messages:         messages,
			Stream:           false,
			FrequencyPenalty: params.FrequencyPenalty,
			MaxTokens:        tokens,
			N:                params.N,
			PresencePenalty:  params.PresencePenalty,
			Stop:             params.Stop,
			Temperature:      params.Temperature,
			TopP:             params.TopP,
		}
		if !config.IsAzure(params.IsAzure, params.ResourceName, params.DeploymentID) {
			// model is mandatory for OpenAI calls, but obsolete for Azure calls
			input.Model = params.Model
		}
		return input, nil
	}
}

func (v *openai) getError(statusCode int, requestID string, resBodyError *openAIApiError, isAzure bool) error {
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
	return errors.New(errorMsg)
}

func (v *openai) determineTokens(maxTokensSetting float64, classSetting int, model string, messages []message) (*int, error) {
	tokenMessagesCount, err := getTokensCount(model, messages)
	if err != nil {
		maxTokens := 0
		return &maxTokens, err
	}
	messageTokens := tokenMessagesCount
	if messageTokens+classSetting >= int(maxTokensSetting) {
		// max token limit must be in range: [1, maxTokensSetting) that's why -1 is added
		maxTokens := int(maxTokensSetting) - messageTokens - 1
		return &maxTokens, nil
	}
	return &messageTokens, nil
}

func (v *openai) getApiKeyHeaderAndValue(apiKey string, isAzure bool) (string, string) {
	if isAzure {
		return "api-key", apiKey
	}
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *openai) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *openai) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
	all := compile.FindAll([]byte(prompt), -1)
	for _, match := range all {
		originalProperty := string(match)
		replacedProperty := compile.FindStringSubmatch(originalProperty)[1]
		replacedProperty = strings.TrimSpace(replacedProperty)
		value := textProperties[replacedProperty]
		if value == "" {
			return "", errors.Errorf("Following property has empty value: '%v'. Make sure you spell the property name correctly, verify that the property exists and has a value", replacedProperty)
		}
		prompt = strings.ReplaceAll(prompt, originalProperty, value)
	}
	return prompt, nil
}

func (v *openai) getApiKey(ctx context.Context, isAzure bool) (string, error) {
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

func (v *openai) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *openai) getValueFromContext(ctx context.Context, key string) string {
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

func (v *openai) getOpenAIOrganization(ctx context.Context) string {
	if value := v.getValueFromContext(ctx, "X-Openai-Organization"); value != "" {
		return value
	}
	return v.openAIOrganization
}

type generateInput struct {
	Prompt           string    `json:"prompt,omitempty"`
	Messages         []message `json:"messages,omitempty"`
	Stream           bool      `json:"stream,omitempty"`
	Model            string    `json:"model,omitempty"`
	FrequencyPenalty *float64  `json:"frequency_penalty,omitempty"`
	Logprobs         *bool     `json:"logprobs,omitempty"`
	TopLogprobs      *int      `json:"top_logprobs,omitempty"`
	MaxTokens        *int      `json:"max_tokens,omitempty"`
	N                *int      `json:"n,omitempty"`
	PresencePenalty  *float64  `json:"presence_penalty,omitempty"`
	Stop             []string  `json:"stop,omitempty"`
	Temperature      *float64  `json:"temperature,omitempty"`
	TopP             *float64  `json:"top_p,omitempty"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

type generateResponse struct {
	Choices []choice
	Usage   *usage          `json:"usage,omitempty"`
	Error   *openAIApiError `json:"error,omitempty"`
}

type choice struct {
	FinishReason string
	Index        float32
	Text         string   `json:"text,omitempty"`
	Message      *message `json:"message,omitempty"`
}

type openAIApiError struct {
	Message string     `json:"message"`
	Type    string     `json:"type"`
	Param   string     `json:"param"`
	Code    openAICode `json:"code"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
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

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
