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
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-anthropic/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

type anthropic struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *anthropic {
	return &anthropic{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (a *anthropic) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := a.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return a.Generate(ctx, cfg, forPrompt)
}

func (a *anthropic) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := a.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return a.Generate(ctx, cfg, forTask)
}

func (a *anthropic) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

	anthropicURL, err := a.getAnthropicURL(ctx, settings.BaseURL())
	if err != nil {
		return nil, errors.Wrap(err, "get anthropic url")
	}

	input := generateInput{
		Messages: []message{
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Model:         settings.Model(),
		MaxTokens:     settings.MaxTokens(),
		StopSequences: settings.StopSequences(),
		Temperature:   settings.Temperature(),
		TopK:          settings.K(),
		TopP:          settings.P(),
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", anthropicURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := a.getAPIKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Anthropic API key")
	}

	req.Header.Add("x-api-key", apiKey)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Request-Source", "unspecified:weaviate")
	req.Header.Add("anthropic-version", "2023-06-01")

	res, err := a.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "do POST request")
	}

	defer res.Body.Close()

	// errors from the API are identifiable by the status code
	if res.StatusCode != 200 {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, errors.Wrap(err, "read response body")
		}

		var resBody anthropicApiError
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return nil, errors.Wrap(err, "unmarshal response body")
		}

		return nil, fmt.Errorf("Anthropic API error: %s - %s", resBody.Error.Type, resBody.Error.Message)

	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	textResponse := resBody.Content[0].Text
	if len(resBody.Content) > 0 && textResponse != "" {
		trimmedResponse := strings.Trim(textResponse, "\n")
		return &generativemodels.GenerateResponse{
			Result: &trimmedResponse,
		}, nil
	}

	return nil, errors.New("no response from Anthropic")
}

func (a *anthropic) getAnthropicURL(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Anthropic-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return url.JoinPath(passedBaseURL, "/v1/messages")
}

func (a *anthropic) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", errors.Wrap(err, "marshal text properties")
	}
	task = compile.ReplaceAllStringFunc(task, func(match string) string {
		match = strings.Trim(match, "{}")
		for _, textProperty := range textProperties {
			if val, ok := textProperty[match]; ok {
				return val
			}
		}
		return match
	})
	return fmt.Sprintf(task, marshal), nil
}

func (a *anthropic) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (a *anthropic) getAPIKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Anthropic-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if a.apiKey != "" {
		return a.apiKey, nil
	}
	return "", errors.New("no api key found for Anthropic " +
		"neither in request header: X-Anthropic-Api-Key " +
		"nor in the environment variable under ANTHROPIC_APIKEY")
}

type generateInput struct {
	Messages      []message `json:"messages"`
	Model         string    `json:"model"`
	MaxTokens     int       `json:"max_tokens"`
	StopSequences []string  `json:"stop_sequences"`
	Temperature   float64   `json:"temperature"`
	TopK          int       `json:"top_k"`
	TopP          float64   `json:"top_p"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type generateResponse struct {
	Id           string     `json:"id"`
	Type         string     `json:"type"`
	Role         string     `json:"role"`
	Content      []content  `json:"content"`
	Model        string     `json:"model"`
	StopReason   StopReason `json:"stop_reason"`
	StopSequence string     `json:"stop_sequence"`
	Usage        usage      `json:"usage"`
}

type content struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type StopReason string

const (
	EndTurn      StopReason = "end_turn"
	MaxTokens    StopReason = "max_tokens"
	StopSequence StopReason = "stop_sequence"
	ToolUse      StopReason = "tool_use"
)

type usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type anthropicApiError struct {
	Type  string       `json:"type"`
	Error errorMessage `json:"error"`
}

type errorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
