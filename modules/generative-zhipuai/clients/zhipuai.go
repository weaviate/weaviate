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
	"github.com/weaviate/weaviate/modules/generative-zhipuai/config"
	zhipuaiparams "github.com/weaviate/weaviate/modules/generative-zhipuai/parameters"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildUrlFn(baseURL string) (string, error) {

	path := "chat/completions"

	return url.JoinPath(baseURL, path)
}

type zhipuai struct {
	zhipuAIApiKey string
	buildUrl      func(baseURL string) (string, error)
	httpClient    *http.Client
	logger        logrus.FieldLogger
}

func New(zhipuAIApiKey string, timeout time.Duration, logger logrus.FieldLogger) *zhipuai {
	return &zhipuai{
		zhipuAIApiKey: zhipuAIApiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildUrl: buildUrlFn,
		logger:   logger,
	}
}

func (v *zhipuai) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *zhipuai) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *zhipuai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	fmt.Println(params)
	debugInformation := v.getDebugInformation(debug, prompt)

	zpiUrl, err := v.buildZhipuAIUrl(ctx, params)
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

	req, err := http.NewRequestWithContext(ctx, "POST", zpiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "ZhipuAI API Key")
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
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}
	requestID := resBody.RequestId
	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, requestID, resBody.Error)
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

func (v *zhipuai) getParameters(cfg moduletools.ClassConfig, options interface{}) zhipuaiparams.Params {
	settings := config.NewClassSettings(cfg)

	var params zhipuaiparams.Params
	if p, ok := options.(zhipuaiparams.Params); ok {
		params = p
	}

	if params.BaseURL == "" {
		params.BaseURL = settings.BaseURL()
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
	if params.MaxTokens == nil {
		maxTokens := int(settings.MaxTokens())
		params.MaxTokens = &maxTokens
	}
	return params
}

func (v *zhipuai) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *zhipuai) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{zhipuaiparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[zhipuaiparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (v *zhipuai) buildZhipuAIUrl(ctx context.Context, params zhipuaiparams.Params) (string, error) {
	baseURL := params.BaseURL

	if headerBaseURL := v.getValueFromContext(ctx, "X-Zhipuai-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}

	return v.buildUrl(baseURL)
}

func (v *zhipuai) generateInput(prompt string, params zhipuaiparams.Params) (generateInput, error) {
	var input generateInput
	messages := []message{{
		Role:    "user",
		Content: prompt,
	}}
	tools := []tool{{
		Type: "web_search",
		WebSearch: &webSearch{
			Enable: false,
		},
	}}
	input = generateInput{
		Messages:    messages,
		Stream:      false,
		Model:       params.Model,
		MaxTokens:   params.MaxTokens,
		Stop:        params.Stop,
		Temperature: params.Temperature,
		TopP:        params.TopP,
		Tools:       tools,
	}
	return input, nil
}

func (v *zhipuai) getError(statusCode int, requestID string, resBodyError *zhipuAIApiError) error {
	endpoint := "ZhipuAI API"
	errorMsg := fmt.Sprintf("connection to: %s failed with status: %d", endpoint, statusCode)
	if requestID != "" {
		errorMsg = fmt.Sprintf("%s request-id: %s", errorMsg, requestID)
	}
	if resBodyError != nil {
		errorMsg = fmt.Sprintf("%s error: %v", errorMsg, resBodyError.Message)
	}
	return errors.New(errorMsg)
}

func (v *zhipuai) getApiKeyHeaderAndValue(apiKey string) (string, string) {

	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *zhipuai) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *zhipuai) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *zhipuai) getApiKey(ctx context.Context) (string, error) {
	var apiKey, envVarValue, envVar string

	apiKey = "X-Zhipuai-Api-Key"
	envVar = "ZHIPUAI_APIKEY"
	envVarValue = v.zhipuAIApiKey

	return v.getApiKeyFromContext(ctx, apiKey, envVarValue, envVar)
}

func (v *zhipuai) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := v.getValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

func (v *zhipuai) getValueFromContext(ctx context.Context, key string) string {
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
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	Stop        []string  `json:"stop,omitempty"`
	Temperature *float64  `json:"temperature,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
	Tools       []tool    `json:"tools,omitempty"`
}

type tool struct {
	Type      string     `json:"type"`
	WebSearch *webSearch `json:"web_search,omitempty"`
}

type webSearch struct {
	Enable bool `json:"enable"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

type generateResponse struct {
	RequestId string `json:"request_id,omitempty"`
	Choices   []choice
	Usage     *usage           `json:"usage,omitempty"`
	Error     *zhipuAIApiError `json:"error,omitempty"`
}

type choice struct {
	FinishReason string
	Index        float32
	Text         string   `json:"text,omitempty"`
	Message      *message `json:"message,omitempty"`
}

type zhipuAIApiError struct {
	Message string      `json:"message"`
	Type    string      `json:"type"`
	Param   string      `json:"param"`
	Code    zhipuAICode `json:"code"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type zhipuAICode string

func (c *zhipuAICode) String() string {
	if c == nil {
		return ""
	}
	return string(*c)
}

func (c *zhipuAICode) UnmarshalJSON(data []byte) (err error) {
	if number, err := strconv.Atoi(string(data)); err == nil {
		str := strconv.Itoa(number)
		*c = zhipuAICode(str)
		return nil
	}
	var str string
	err = json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*c = zhipuAICode(str)
	return nil
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
