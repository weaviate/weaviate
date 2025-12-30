//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-deepseek/config"
	deepseekparams "github.com/weaviate/weaviate/modules/generative-deepseek/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type deepseek struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *deepseek {
	return &deepseek{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *deepseek) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues("generate", "deepseek").Inc()
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forPrompt, options, debug)
}

func (v *deepseek) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestBatchCount.WithLabelValues("generate", "deepseek").Inc()
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forTask, options, debug)
}

func (v *deepseek) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequests.WithLabelValues("generate", "deepseek").Inc()
	startTime := time.Now()

	params := v.getParameters(cfg, options)

	apiURL, err := v.getApiUrl(ctx, params.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "get api url")
	}

	input := v.createInput(prompt, params)

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	defer func() {
		monitoring.GetMetrics().ModuleExternalRequestDuration.WithLabelValues("generate", apiURL).Observe(time.Since(startTime).Seconds())
	}()

	monitoring.GetMetrics().ModuleExternalRequestSize.WithLabelValues("generate", apiURL).Observe(float64(len(body)))

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "DeepSeek API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if res != nil {
		monitoring.GetMetrics().ModuleExternalResponseStatus.WithLabelValues("generate", apiURL, fmt.Sprintf("%v", res.StatusCode)).Inc()
	}
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	monitoring.GetMetrics().ModuleExternalResponseSize.WithLabelValues("generate", apiURL).Observe(float64(len(bodyBytes)))

	var resBody chatResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, resBody.Error)
	}

	if len(resBody.Choices) > 0 {
		content := strings.TrimSpace(resBody.Choices[0].Message.Content)
		if content != "" {
			return &modulecapabilities.GenerateResponse{
				Result: &content,
				Debug:  v.getDebugInformation(debug, prompt),
				Params: v.getResponseParams(resBody.Usage),
			}, nil
		}
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  v.getDebugInformation(debug, prompt),
	}, nil
}

func (v *deepseek) getParameters(cfg moduletools.ClassConfig, options interface{}) deepseekparams.Params {
	settings := config.NewClassSettings(cfg)

	var params deepseekparams.Params
	if p, ok := options.(deepseekparams.Params); ok {
		params = p
	}

	if params.BaseURL == "" {
		params.BaseURL = settings.BaseURL()
	}
	if params.Model == "" {
		params.Model = settings.Model()
	}
	if params.Temperature == nil {
		if t := settings.Temperature(); t != nil {
			params.Temperature = t
		}
	}
	if params.TopP == nil {
		if tp := settings.TopP(); tp != 0 {
			params.TopP = &tp
		}
	}
	if params.FrequencyPenalty == nil {
		if fp := settings.FrequencyPenalty(); fp != 0 {
			params.FrequencyPenalty = &fp
		}
	}
	if params.PresencePenalty == nil {
		if pp := settings.PresencePenalty(); pp != 0 {
			params.PresencePenalty = &pp
		}
	}
	if params.MaxTokens == nil {
		if mt := settings.MaxTokens(); mt != nil && *mt != -1 {
			val := int(*mt)
			params.MaxTokens = &val
		}
	}
	return params
}

func (v *deepseek) createInput(prompt string, params deepseekparams.Params) chatInput {
	return chatInput{
		Messages: []message{{
			Role:    "user",
			Content: prompt,
		}},
		Model:            params.Model,
		Stream:           false,
		MaxTokens:        params.MaxTokens,
		Temperature:      params.Temperature,
		TopP:             params.TopP,
		FrequencyPenalty: params.FrequencyPenalty,
		PresencePenalty:  params.PresencePenalty,
		Stop:             params.Stop,
	}
}

func (v *deepseek) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *deepseek) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{
			"usage": map[string]interface{}{
				"prompt_tokens":     usage.PromptTokens,
				"completion_tokens": usage.CompletionTokens,
				"total_tokens":      usage.TotalTokens,
			},
		}
	}
	return nil
}

func (v *deepseek) getApiUrl(ctx context.Context, baseURL string) (string, error) {
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Deepseek-Baseurl"); headerBaseURL != "" {
		return url.JoinPath(headerBaseURL, "/chat/completions")
	}
	return url.JoinPath(baseURL, "/chat/completions")
}

func (v *deepseek) getApiKey(ctx context.Context) (string, error) {
	if val := modulecomponents.GetValueFromContext(ctx, "X-Deepseek-Api-Key"); val != "" {
		return val, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", fmt.Errorf("no api key found")
}

func (v *deepseek) getError(statusCode int, apiError *deepSeekError) error {
	msg := fmt.Sprintf("connection to: DeepSeek API failed with status: %d", statusCode)
	if apiError != nil {
		msg = fmt.Sprintf("%s error: %v", msg, apiError.Message)
	}
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("generate", "deepseek", "DeepSeek API", fmt.Sprintf("%v", statusCode)).Inc()
	return errors.New(msg)
}

func (v *deepseek) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "Generative Search - DeepSeek",
		"documentationHref": "https://api-docs.deepseek.com/",
	}, nil
}

// Local simplified structs to avoid reuse/duplication
type chatInput struct {
	Messages         []message `json:"messages"`
	Model            string    `json:"model"`
	Stream           bool      `json:"stream"`
	MaxTokens        *int      `json:"max_tokens,omitempty"`
	Temperature      *float64  `json:"temperature,omitempty"`
	TopP             *float64  `json:"top_p,omitempty"`
	FrequencyPenalty *float64  `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64  `json:"presence_penalty,omitempty"`
	Stop             []string  `json:"stop,omitempty"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []choice       `json:"choices"`
	Usage   *usage         `json:"usage,omitempty"`
	Error   *deepSeekError `json:"error,omitempty"`
}

type choice struct {
	Message      message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type deepSeekError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}
