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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-databricks/config"
	openaiparams "github.com/weaviate/weaviate/modules/generative-databricks/parameters"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildServingUrlFn(servingUrl string) (string, error) {
	return servingUrl, nil
}

type databricks struct {
	databricksToken string
	buildServingUrl func(servingUrl string) (string, error)
	httpClient      *http.Client
	logger          logrus.FieldLogger
}

func New(databricksToken string, timeout time.Duration, logger logrus.FieldLogger) *databricks {
	return &databricks{
		databricksToken: databricksToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildServingUrl: buildServingUrlFn,
		logger:          logger,
	}
}

func (v *databricks) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *databricks) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *databricks) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	oaiUrl, err := v.buildDatabricksServingUrl(ctx, settings)
	if err != nil {
		return nil, errors.Wrap(err, "url join path")
	}

	input, err := v.generateInput(prompt, params, settings)
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
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "OpenAI API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey))
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

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, resBody.Error)
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

func (v *databricks) getParameters(cfg moduletools.ClassConfig, options interface{}) openaiparams.Params {
	settings := config.NewClassSettings(cfg)

	var params openaiparams.Params
	if p, ok := options.(openaiparams.Params); ok {
		params = p
	}

	if params.Temperature == nil {
		temperature := settings.Temperature()
		params.Temperature = &temperature
	}
	if params.TopP == nil {
		topP := settings.TopP()
		params.TopP = &topP
	}

	if params.MaxTokens == nil {
		maxTokens := int(settings.MaxTokens())
		params.MaxTokens = &maxTokens
	}
	return params
}

func (v *databricks) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *databricks) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{"openai": map[string]interface{}{"usage": usage}}
	}
	return nil
}

func (v *databricks) buildDatabricksServingUrl(ctx context.Context, settings config.ClassSettings) (string, error) {
	servingURL, _ := v.buildServingUrl(settings.ServingURL())
	if headerServingURL := v.getValueFromContext(ctx, "X-Databricks-Servingurl"); headerServingURL != "" {
		servingURL = headerServingURL
	}
	return servingURL, nil
}

func (v *databricks) generateInput(prompt string, params openaiparams.Params, settings config.ClassSettings) (generateInput, error) {
	var input generateInput
	messages := []message{{
		Role:    "user",
		Content: prompt,
	}}

	input = generateInput{
		Messages:    messages,
		Stream:      false,
		Logprobs:    params.Logprobs,
		TopLogprobs: params.TopLogprobs,
		MaxTokens:   params.MaxTokens,
		N:           params.N,
		Stop:        params.Stop,
		Temperature: params.Temperature,
		TopP:        params.TopP,
	}

	return input, nil
}

func (v *databricks) getError(statusCode int, resBodyError *openAIApiError) error {
	endpoint := "Databricks Foundation Model API"
	if resBodyError != nil {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBodyError.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *databricks) determineTokens(maxTokensSetting float64, classSetting int, model string, messages []message) (int, error) {
	tokenMessagesCount, err := getTokensCount(model, messages)
	if err != nil {
		return 0, err
	}
	messageTokens := tokenMessagesCount
	if messageTokens+classSetting >= int(maxTokensSetting) {
		// max token limit must be in range: [1, maxTokensSetting) that's why -1 is added
		return int(maxTokensSetting) - messageTokens - 1, nil
	}
	return messageTokens, nil
}

func (v *databricks) getApiKeyHeaderAndValue(apiKey string) (string, string) {
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *databricks) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *databricks) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *databricks) getApiKey(ctx context.Context) (string, error) {
	var apiKey, envVarValue, envVar string

	apiKey = "X-Databricks-Token"
	envVar = "DATABRICKS_TOKEN"
	envVarValue = v.databricksToken

	return v.getApiKeyFromContext(ctx, apiKey, envVarValue, envVar)
}

func (v *databricks) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *databricks) getValueFromContext(ctx context.Context, key string) string {
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

type generateInput struct {
	Prompt      string    `json:"prompt,omitempty"`
	Messages    []message `json:"messages,omitempty"`
	Stream      bool      `json:"stream,omitempty"`
	Model       string    `json:"model,omitempty"`
	Logprobs    *bool     `json:"logprobs,omitempty"`
	TopLogprobs *int      `json:"top_logprobs,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	N           *int      `json:"n,omitempty"`
	Stop        []string  `json:"stop,omitempty"`
	Temperature *float64  `json:"temperature,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
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
	Logprobs     string
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
