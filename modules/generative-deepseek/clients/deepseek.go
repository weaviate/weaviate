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

type client struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *client) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues("generate", "deepseek").Inc()
	singlePrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return c.doGenerate(ctx, cfg, singlePrompt, options, debug)
}

func (c *client) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestBatchCount.WithLabelValues("generate", "deepseek").Inc()
	taskPrompt, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return c.doGenerate(ctx, cfg, taskPrompt, options, debug)
}

func (c *client) doGenerate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequests.WithLabelValues("generate", "deepseek").Inc()
	start := time.Now()

	params := c.parseOptions(cfg, options)
	debugData := c.debugData(debug, prompt)

	endpoint, err := c.url(ctx, params.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "resolve endpoint")
	}

	payload := c.makePayload(prompt, params)
	reqBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "marshal payload")
	}

	defer func() {
		monitoring.GetMetrics().ModuleExternalRequestDuration.WithLabelValues("generate", endpoint).Observe(time.Since(start).Seconds())
	}()

	monitoring.GetMetrics().ModuleExternalRequestSize.WithLabelValues("generate", endpoint).Observe(float64(len(reqBytes)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}

	key, err := c.apiKeyFromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "api key")
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", key))
	req.Header.Set("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if res != nil {
		monitoring.GetMetrics().ModuleExternalResponseStatus.WithLabelValues("generate", endpoint, fmt.Sprintf("%v", res.StatusCode)).Inc()
	}
	if err != nil {
		return nil, errors.Wrap(err, "execute request")
	}
	defer res.Body.Close()

	respBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response")
	}

	monitoring.GetMetrics().ModuleExternalResponseSize.WithLabelValues("generate", endpoint).Observe(float64(len(respBytes)))

	var response chatResp
	if err := json.Unmarshal(respBytes, &response); err != nil {
		return nil, errors.Wrapf(err, "unmarshal: %s", string(respBytes))
	}

	if res.StatusCode != http.StatusOK || response.Error != nil {
		return nil, c.apiError(res.StatusCode, response.Error)
	}

	if len(response.Choices) > 0 {
		answer := strings.TrimSpace(response.Choices[0].Message.Content)
		return &modulecapabilities.GenerateResponse{
			Result: &answer,
			Debug:  debugData,
			Params: c.usageParams(response.Usage),
		}, nil
	}

	return &modulecapabilities.GenerateResponse{Result: nil, Debug: debugData}, nil
}

func (c *client) getResponseParams(u *usage) map[string]interface{} {
	if u != nil {
		return map[string]interface{}{
			deepseekparams.Name: map[string]interface{}{
				"usage": map[string]interface{}{
					"prompt_tokens":     u.InputTokens,
					"completion_tokens": u.OutputTokens,
					"total_tokens":      u.TotalTokens,
				},
			},
		}
	}
	return nil
}

func (c *client) parseOptions(cfg moduletools.ClassConfig, options interface{}) deepseekparams.Params {
	settings := config.NewClassSettings(cfg)
	p := deepseekparams.Params{}
	if opt, ok := options.(deepseekparams.Params); ok {
		p = opt
	}

	if p.BaseURL == "" {
		p.BaseURL = settings.BaseURL()
	}
	if p.Model == "" {
		p.Model = settings.Model()
	}
	if p.Temperature == nil {
		if t := settings.Temperature(); t != nil {
			p.Temperature = t
		}
	}
	if p.TopP == nil {
		if tp := settings.TopP(); tp != 0 {
			p.TopP = &tp
		}
	}
	if p.FrequencyPenalty == nil {
		if fp := settings.FrequencyPenalty(); fp != 0 {
			p.FrequencyPenalty = &fp
		}
	}
	if p.PresencePenalty == nil {
		if pp := settings.PresencePenalty(); pp != 0 {
			p.PresencePenalty = &pp
		}
	}
	if p.MaxTokens == nil {
		if mt := settings.MaxTokens(); mt != nil && *mt != -1 {
			val := int(*mt)
			p.MaxTokens = &val
		}
	}
	return p
}

func (c *client) makePayload(prompt string, p deepseekparams.Params) chatPayload {
	return chatPayload{
		Messages: []chatMessage{{
			Role:    "user",
			Content: prompt,
		}},
		Model:            p.Model,
		MaxTokens:        p.MaxTokens,
		Temperature:      p.Temperature,
		TopP:             p.TopP,
		FrequencyPenalty: p.FrequencyPenalty,
		PresencePenalty:  p.PresencePenalty,
		Stop:             p.Stop,
	}
}

func (c *client) debugData(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{Prompt: prompt}
	}
	return nil
}

func (c *client) usageParams(u *usage) map[string]interface{} {
	if u != nil {
		return map[string]interface{}{
			"usage": map[string]interface{}{
				"prompt_tokens":     u.InputTokens,
				"completion_tokens": u.OutputTokens,
				"total_tokens":      u.TotalTokens,
			},
		}
	}
	return nil
}

func (c *client) url(ctx context.Context, base string) (string, error) {
	if override := modulecomponents.GetValueFromContext(ctx, "X-Deepseek-Baseurl"); override != "" {
		return url.JoinPath(override, "/chat/completions")
	}
	return url.JoinPath(base, "/chat/completions")
}

func (c *client) apiKeyFromContext(ctx context.Context) (string, error) {
	if key := modulecomponents.GetValueFromContext(ctx, "X-Deepseek-Api-Key"); key != "" {
		return key, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", fmt.Errorf("no api key found")
}

func (c *client) apiError(code int, errPayload *apiErr) error {
	msg := fmt.Sprintf("DeepSeek API error (status %d)", code)
	if errPayload != nil {
		msg = fmt.Sprintf("%s: %s", msg, errPayload.Message)
	}
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("generate", "deepseek", "API", fmt.Sprintf("%v", code)).Inc()
	return errors.New(msg)
}

func (c *client) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "Generative Search - DeepSeek",
		"documentationHref": "https://api-docs.deepseek.com/",
	}, nil
}

// DeepSeek specific structs (renamed to avoid collisions)
type chatPayload struct {
	Messages         []chatMessage `json:"messages"`
	Model            string        `json:"model"`
	MaxTokens        *int          `json:"max_tokens,omitempty"`
	Temperature      *float64      `json:"temperature,omitempty"`
	TopP             *float64      `json:"top_p,omitempty"`
	FrequencyPenalty *float64      `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64      `json:"presence_penalty,omitempty"`
	Stop             []string      `json:"stop,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResp struct {
	Choices []choice `json:"choices"`
	Usage   *usage   `json:"usage,omitempty"`
	Error   *apiErr  `json:"error,omitempty"`
}

type choice struct {
	Message chatMessage `json:"message"`
	Finish  string      `json:"finish_reason"`
}

type usage struct {
	InputTokens  int `json:"prompt_tokens"`
	OutputTokens int `json:"completion_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type apiErr struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    any    `json:"code"`
}
