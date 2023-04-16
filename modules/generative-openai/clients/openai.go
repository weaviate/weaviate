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
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-openai/config"
	"github.com/weaviate/weaviate/modules/generative-openai/ent"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildUrl(isLegacy bool, resourceName, deploymentID string) (string, error) {
	if resourceName != "" && deploymentID != "" {
		host := "https://" + resourceName + ".openai.azure.com"
		path := "openai/deployments/" + deploymentID + "/chat/completions"
		queryParam := "api-version=2023-03-15-preview"
		return fmt.Sprintf("%s/%s?%s", host, path, queryParam), nil
	}
	host := "https://api.openai.com"
	path := "/v1/chat/completions"
	if isLegacy {
		path = "/v1/completions"
	}
	return url.JoinPath(host, path)
}

type openai struct {
	openAIApiKey string
	azureApiKey  string
	buildUrlFn   func(isLegacy bool, resourceName, deploymentID string) (string, error)
	httpClient   *http.Client
	logger       logrus.FieldLogger
}

func New(openAIApiKey, azureApiKey string, logger logrus.FieldLogger) *openai {
	return &openai{
		openAIApiKey: openAIApiKey,
		azureApiKey:  azureApiKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		buildUrlFn: buildUrl,
		logger:     logger,
	}
}

func (v *openai) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *openai) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *openai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*ent.GenerateResult, error) {
	settings := config.NewClassSettings(cfg)

	oaiUrl, err := v.buildUrlFn(settings.IsLegacy(), settings.ResourceName(), settings.DeploymentID())
	if err != nil {
		return nil, errors.Wrap(err, "url join path")
	}

	var input generateInput
	if settings.IsLegacy() {
		input = generateInput{
			Prompt:           prompt,
			Model:            settings.Model(),
			MaxTokens:        settings.MaxTokens(),
			Temperature:      settings.Temperature(),
			FrequencyPenalty: settings.FrequencyPenalty(),
			PresencePenalty:  settings.PresencePenalty(),
			TopP:             settings.TopP(),
		}
	} else {
		tokens := determineTokens(settings.GetMaxTokensForModel(settings.Model()), settings.MaxTokens(), prompt)
		input = generateInput{
			Messages: []message{{
				Role:    "user",
				Content: prompt,
			}},
			Model:            settings.Model(),
			MaxTokens:        float64(tokens),
			Temperature:      settings.Temperature(),
			FrequencyPenalty: settings.FrequencyPenalty(),
			PresencePenalty:  settings.PresencePenalty(),
			TopP:             settings.TopP(),
		}
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
	apiKey, err := v.getApiKey(ctx, settings.IsAzure())
	if err != nil {
		return nil, errors.Wrapf(err, "OpenAI API Key")
	}
	req.Header.Add(v.getApiKeyHeaderAndValue(apiKey, settings.IsAzure()))
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
		if resBody.Error != nil {
			return nil, fmt.Errorf("connection to OpenAI failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, fmt.Errorf("failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Choices[0].Text
	if len(resBody.Choices) > 0 && textResponse != "" {
		trimmedResponse := strings.Trim(textResponse, "\n")
		return &ent.GenerateResult{
			Result: &trimmedResponse,
		}, nil
	}

	message := resBody.Choices[0].Message
	if message != nil {
		textResponse = message.Content
		trimmedResponse := strings.Trim(textResponse, "\n")
		return &ent.GenerateResult{
			Result: &trimmedResponse,
		}, nil
	}

	return &ent.GenerateResult{
		Result: nil,
	}, nil
}

func determineTokens(maxTokensSetting float64, classSetting float64, prompt string) int {
	tokens := float64(countWhitespace(prompt)+1) * 3
	if tokens+classSetting > maxTokensSetting {
		return int(maxTokensSetting - tokens)
	}
	return int(tokens)
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

func countWhitespace(s string) int {
	count := 0
	for {
		idx := strings.IndexFunc(s, unicode.IsSpace)
		if idx == -1 {
			break
		}
		count++
		s = s[idx+1:]
	}
	return count
}

func (v *openai) getApiKey(ctx context.Context, isAzure bool) (string, error) {
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

func (v *openai) getApiKeyFromContext(ctx context.Context, apiKey, envVar string) (string, error) {
	if apiValue := ctx.Value(apiKey); apiValue != nil {
		if apiKeyHeader, ok := apiValue.([]string); ok && len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
			return apiKeyHeader[0], nil
		}
	}
	return "", fmt.Errorf("no api key found neither in request header: %s nor in environment variable under %s", apiKey, envVar)
}

type generateInput struct {
	Prompt           string    `json:"prompt,omitempty"`
	Messages         []message `json:"messages,omitempty"`
	Model            string    `json:"model"`
	MaxTokens        float64   `json:"max_tokens"`
	Temperature      float64   `json:"temperature"`
	Stop             []string  `json:"stop"`
	FrequencyPenalty float64   `json:"frequency_penalty"`
	PresencePenalty  float64   `json:"presence_penalty"`
	TopP             float64   `json:"top_p"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type generateResponse struct {
	Choices []choice
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
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param"`
	Code    string `json:"code"`
}
