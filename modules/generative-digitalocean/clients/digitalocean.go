//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/modules/generative-digitalocean/config"
	digitaloceanparams "github.com/weaviate/weaviate/modules/generative-digitalocean/parameters"
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
		apiKey:     apiKey,
		httpClient: modulecomponents.NewBaseHttpClient(timeout),
		logger:     logger,
	}
}

func (c *client) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options any, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues("generate", digitaloceanparams.Name).Inc()
	singlePrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return c.doGenerate(ctx, cfg, singlePrompt, options, debug)
}

func (c *client) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options any, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestBatchCount.WithLabelValues("generate", digitaloceanparams.Name).Inc()
	taskPrompt, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return c.doGenerate(ctx, cfg, taskPrompt, options, debug)
}

func (c *client) doGenerate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options any, debug bool) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequests.WithLabelValues("generate", digitaloceanparams.Name).Inc()
	start := time.Now()

	params := c.parseOptions(cfg, options)
	debugData := c.debugData(debug, prompt)

	endpoint, err := c.url(ctx, params.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "resolve endpoint")
	}

	reqBytes, err := json.Marshal(c.makePayload(prompt, params))
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
		return nil, errors.Wrapf(err, "unmarshal response (status %d): %s", res.StatusCode, string(respBytes))
	}

	if res.StatusCode != http.StatusOK || response.Error != nil {
		return nil, c.apiError(res.StatusCode, response)
	}

	if len(response.Choices) == 0 {
		return &modulecapabilities.GenerateResponse{Debug: debugData}, nil
	}

	answer := strings.TrimSpace(response.Choices[0].Message.Content)
	return &modulecapabilities.GenerateResponse{
		Result: &answer,
		Debug:  debugData,
		Params: c.usageParams(response.Usage),
	}, nil
}

func (c *client) parseOptions(cfg moduletools.ClassConfig, options any) digitaloceanparams.Params {
	settings := config.NewClassSettings(cfg)

	params := digitaloceanparams.Params{}
	if opt, ok := options.(digitaloceanparams.Params); ok {
		params = opt
	}
	if params.BaseURL == "" {
		params.BaseURL = settings.BaseURL()
	}
	if params.Model == "" {
		params.Model = settings.Model()
	}
	if params.Temperature == nil {
		params.Temperature = settings.Temperature()
	}
	if params.TopP == nil {
		params.TopP = settings.TopP()
	}
	if params.MaxTokens == nil {
		params.MaxTokens = settings.MaxTokens()
	}
	if params.FrequencyPenalty == nil {
		params.FrequencyPenalty = settings.FrequencyPenalty()
	}
	if params.PresencePenalty == nil {
		params.PresencePenalty = settings.PresencePenalty()
	}
	if len(params.Stop) == 0 {
		params.Stop = settings.Stop()
	}
	return params
}

func (c *client) makePayload(prompt string, params digitaloceanparams.Params) chatPayload {
	return chatPayload{
		Messages:            []chatMessage{{Role: "user", Content: prompt}},
		Model:               params.Model,
		MaxCompletionTokens: params.MaxTokens,
		Temperature:         params.Temperature,
		TopP:                params.TopP,
		FrequencyPenalty:    params.FrequencyPenalty,
		PresencePenalty:     params.PresencePenalty,
		Stop:                params.Stop,
	}
}

func (c *client) debugData(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{Prompt: prompt}
	}
	return nil
}

func (c *client) usageParams(u *usage) map[string]any {
	if u != nil {
		return map[string]any{digitaloceanparams.Name: map[string]any{"usage": u}}
	}
	return nil
}

func GetResponseParams(result map[string]any) *responseParams {
	if params, ok := result[digitaloceanparams.Name].(map[string]any); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (c *client) url(ctx context.Context, base string) (string, error) {
	base, err := modulecomponents.ValidatedBaseURLFromHeader(ctx, "X-Digitalocean-Baseurl", base)
	if err != nil {
		return "", err
	}
	return url.JoinPath(base, "/v1/chat/completions")
}

func (c *client) apiKeyFromContext(ctx context.Context) (string, error) {
	if key := modulecomponents.GetValueFromContext(ctx, "X-Digitalocean-Api-Key"); key != "" {
		return key, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Digitalocean-Api-Key " +
		"nor in environment variable under DIGITALOCEAN_APIKEY")
}

func (c *client) apiError(statusCode int, response chatResp) error {
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("generate", digitaloceanparams.Name, "API", fmt.Sprintf("%v", statusCode)).Inc()
	status, message := fmt.Sprintf("%d", statusCode), response.Message
	if message != "" {
		if response.ID != "" {
			status = fmt.Sprintf("%d %s", statusCode, response.ID)
		}
	} else if response.Error != nil {
		message = response.Error.Message
	}
	if message == "" {
		return errors.Errorf("connection to DigitalOcean API failed with status: %s", status)
	}
	if response.RequestID != "" {
		return errors.Errorf("connection to DigitalOcean API failed with status: %s error: %s request_id: %s",
			status, message, response.RequestID)
	}
	return errors.Errorf("connection to DigitalOcean API failed with status: %s error: %s", status, message)
}

func (c *client) MetaInfo() (map[string]any, error) {
	return map[string]any{
		"name":              "Generative Search - DigitalOcean",
		"documentationHref": "https://docs.digitalocean.com/products/inference/how-to/use-chat-completions-api/",
	}, nil
}

type chatPayload struct {
	Messages            []chatMessage `json:"messages"`
	Model               string        `json:"model"`
	MaxCompletionTokens *int          `json:"max_completion_tokens,omitempty"`
	Temperature         *float64      `json:"temperature,omitempty"`
	TopP                *float64      `json:"top_p,omitempty"`
	FrequencyPenalty    *float64      `json:"frequency_penalty,omitempty"`
	PresencePenalty     *float64      `json:"presence_penalty,omitempty"`
	Stop                []string      `json:"stop,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResp struct {
	Choices []choice `json:"choices"`
	Usage   *usage   `json:"usage,omitempty"`
	// DigitalOcean reports failures as a top-level {"id","message","request_id"} body,
	// where id is a short name for the status such as "Payment Required". On a success
	// id instead holds the completion id, so only read it alongside an error status.
	// Models it proxies may answer with the OpenAI-style {"error":{...}} wrapper instead.
	ID        string    `json:"id,omitempty"`
	Message   string    `json:"message,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
	Error     *apiError `json:"error,omitempty"`
}

type choice struct {
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type apiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
