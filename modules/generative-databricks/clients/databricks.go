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
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	generativecomponents "github.com/weaviate/weaviate/usecases/modulecomponents/generative"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-databricks/config"
	databricksparams "github.com/weaviate/weaviate/modules/generative-databricks/parameters"
)

func buildEndpointFn(endpoint string) (string, error) {
	if endpoint == "" {
		return "", fmt.Errorf("endpoint cannot be empty")
	}
	return endpoint, nil
}

type databricks struct {
	databricksToken string
	buildEndpoint   func(endpoint string) (string, error)
	httpClient      *http.Client
	logger          logrus.FieldLogger
}

func New(databricksToken string, timeout time.Duration, logger logrus.FieldLogger) *databricks {
	return &databricks{
		databricksToken: databricksToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		buildEndpoint: buildEndpointFn,
		logger:        logger,
	}
}

func (v *databricks) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generativecomponents.MakeSinglePrompt(generativecomponents.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *databricks) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generativecomponents.MakeTaskPrompt(generativecomponents.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *databricks) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	oaiUrl, err := v.buildDatabricksEndpoint(ctx, params.Endpoint)
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
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Databricks Token")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey))
	req.Header.Add("Content-Type", "application/json")
	if userAgent := modulecomponents.GetValueFromContext(ctx, "X-Databricks-User-Agent"); userAgent != "" {
		req.Header.Add("User-Agent", userAgent)
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

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
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

func (v *databricks) getParameters(cfg moduletools.ClassConfig, options interface{}) databricksparams.Params {
	settings := config.NewClassSettings(cfg)

	var params databricksparams.Params
	if p, ok := options.(databricksparams.Params); ok {
		params = p
	}

	if params.Endpoint == "" {
		params.Endpoint = settings.Endpoint()
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
		maxTokens := settings.MaxTokens()
		params.MaxTokens = maxTokens
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
		return map[string]interface{}{databricksparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[databricksparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (v *databricks) buildDatabricksEndpoint(ctx context.Context, endpoint string) (string, error) {
	if headerEndpoint := modulecomponents.GetValueFromContext(ctx, "X-Databricks-Endpoint"); headerEndpoint != "" {
		return headerEndpoint, nil
	}
	return v.buildEndpoint(endpoint)
}

func (v *databricks) generateInput(prompt string, params databricksparams.Params) (generateInput, error) {
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

func (v *databricks) getError(statusCode int, resBodyError *databricksApiError) error {
	endpoint := "Databricks Foundation Model API"
	if resBodyError != nil {
		return fmt.Errorf("connection to: %s failed with status: %d error: %v", endpoint, statusCode, resBodyError.Message)
	}
	return fmt.Errorf("connection to: %s failed with status: %d", endpoint, statusCode)
}

func (v *databricks) getApiKeyHeaderAndValue(apiKey string) (string, string) {
	return "Authorization", fmt.Sprintf("Bearer %s", apiKey)
}

func (v *databricks) getApiKey(ctx context.Context) (string, error) {
	var apiKey, envVarValue, envVar string

	apiKey = "X-Databricks-Token"
	envVar = "DATABRICKS_TOKEN"
	envVarValue = v.databricksToken

	return v.getApiKeyFromContext(ctx, apiKey, envVarValue, envVar)
}

func (v *databricks) getApiKeyFromContext(ctx context.Context, apiKey, envVarValue, envVar string) (string, error) {
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, apiKey); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	if envVarValue != "" {
		return envVarValue, nil
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
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
	Usage   *usage              `json:"usage,omitempty"`
	Error   *databricksApiError `json:"error,omitempty"`
}

type choice struct {
	FinishReason string
	Index        float32
	Logprobs     string
	Text         string   `json:"text,omitempty"`
	Message      *message `json:"message,omitempty"`
}

type databricksApiError struct {
	Message   string         `json:"message"`
	ErrorCode databricksCode `json:"error_code"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type databricksCode string

func (c *databricksCode) String() string {
	if c == nil {
		return ""
	}
	return string(*c)
}

func (c *databricksCode) UnmarshalJSON(data []byte) (err error) {
	if number, err := strconv.Atoi(string(data)); err == nil {
		str := strconv.Itoa(number)
		*c = databricksCode(str)
		return nil
	}
	var str string
	err = json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*c = databricksCode(str)
	return nil
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
