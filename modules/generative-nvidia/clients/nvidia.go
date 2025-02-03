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
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-nvidia/config"
	nvidiaparams "github.com/weaviate/weaviate/modules/generative-nvidia/parameters"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

type nvidia struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *nvidia {
	return &nvidia{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *nvidia) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *nvidia) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *nvidia) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	nvidiaUrl := v.getNvidiaUrl(ctx, params.BaseURL)
	nvidiaPrompt := []map[string]string{
		{"role": "user", "content": prompt},
	}

	input := generateInput{
		Messages:    nvidiaPrompt,
		Model:       params.Model,
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", nvidiaUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Nvidia API Key")
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

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode == 404 {
		return nil, errors.Errorf("Wrong model name, you can check available Nvidia models from https://docs.api.nvidia.com/nim/reference/llm-apis")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		if resBody.Error != nil {
			return nil, errors.Errorf("connection to Nvidia API failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to Nvidia API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Choices[0].Message.Content

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
		Params: v.getResponseParams(resBody.Usage),
	}, nil
}

func (v *nvidia) getParameters(cfg moduletools.ClassConfig, options interface{}) nvidiaparams.Params {
	settings := config.NewClassSettings(cfg)

	var params nvidiaparams.Params
	if p, ok := options.(nvidiaparams.Params); ok {
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
	if params.MaxTokens == nil {
		maxTokens := settings.MaxTokens()
		params.MaxTokens = &maxTokens
	}
	return params
}

func (v *nvidia) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *nvidia) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{nvidiaparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[nvidiaparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (v *nvidia) getNvidiaUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Nvidia-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/chat/completions", passedBaseURL)
}

func (v *nvidia) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *nvidia) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *nvidia) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Nvidia-Token"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Nvidia-Token " +
		"nor in environment variable under Nvidia_TOKEN")
}

type generateInput struct {
	Model       string              `json:"model"`
	Messages    []map[string]string `json:"messages"`
	MaxTokens   *int                `json:"max_tokens"`
	Temperature *float64            `json:"temperature,omitempty"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Choice struct {
	Message      Message `json:"message"`
	Index        int     `json:"index"`
	FinishReason string  `json:"finish_reason"`
}

type generateResponse struct {
	Choices []Choice
	Usage   *usage `json:"usage,omitempty"`
	Created int64  `json:"created"`

	Error *nvidiaApiError `json:"error,omitempty"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type nvidiaApiError struct {
	Message string `json:"message"`
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
