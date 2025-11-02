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
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-contextualai/config"
	contextualaiparams "github.com/weaviate/weaviate/modules/generative-contextualai/parameters"
)

type contextualai struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *contextualai {
	return &contextualai{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *contextualai) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options any, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	knowledge := c.extractKnowledge([]*modulecapabilities.GenerateProperties{properties})
	return c.generate(ctx, cfg, forPrompt, knowledge, options, debug)
}

func (c *contextualai) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options any, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	texts := generative.Texts(properties)
	forTask, err := generative.MakeTaskPrompt(texts, task)
	if err != nil {
		return nil, err
	}
	knowledge := c.extractKnowledge(properties)
	return c.generate(ctx, cfg, forTask, knowledge, options, debug)
}

func (c *contextualai) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, knowledge []string, options any, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := c.getParameters(cfg, options)
	debugInformation := c.getDebugInformation(debug, prompt)

	contextualAIURL, err := c.getContextualAIURL(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get contextual ai url")
	}

	input := c.buildGenerateInput(params, prompt, knowledge)

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	res, err := c.makeAPIRequest(ctx, contextualAIURL, body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	if res.StatusCode != 200 {
		return nil, c.handleAPIError(res.StatusCode, bodyBytes)
	}

	resBody, err := c.parseResponse(bodyBytes)
	if err != nil {
		return nil, err
	}

	textResponse := resBody.Response
	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
	}, nil
}

func (c *contextualai) getParameters(cfg moduletools.ClassConfig, options any) contextualaiparams.Params {
	settings := config.NewClassSettings(cfg)

	var params contextualaiparams.Params
	if p, ok := options.(contextualaiparams.Params); ok {
		params = p
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
	if params.MaxNewTokens == nil {
		params.MaxNewTokens = settings.MaxNewTokens()
	}
	if params.SystemPrompt == "" {
		params.SystemPrompt = settings.SystemPrompt()
	}
	if params.AvoidCommentary == nil {
		params.AvoidCommentary = settings.AvoidCommentary()
	}

	return params
}

func (c *contextualai) buildGenerateInput(params contextualaiparams.Params, prompt string, knowledge []string) generateInput {
	// Build messages array - always include the user prompt
	messages := []message{
		{
			Role:    "user",
			Content: prompt,
		},
	}

	// Use provided knowledge if available, otherwise use knowledge from properties
	finalKnowledge := knowledge
	if len(params.Knowledge) > 0 {
		finalKnowledge = params.Knowledge
	}

	input := generateInput{
		Model:     params.Model,
		Messages:  messages,
		Knowledge: finalKnowledge,
	}

	// Add optional parameters
	if params.SystemPrompt != "" {
		input.SystemPrompt = &params.SystemPrompt
	}
	if params.AvoidCommentary != nil {
		input.AvoidCommentary = params.AvoidCommentary
	}
	if params.Temperature != nil {
		input.Temperature = params.Temperature
	}
	if params.TopP != nil {
		input.TopP = params.TopP
	}
	if params.MaxNewTokens != nil {
		input.MaxNewTokens = params.MaxNewTokens
	}

	return input
}

func (c *contextualai) makeAPIRequest(ctx context.Context, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := c.getAPIKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Contextual AI API key")
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "do POST request")
	}

	return res, nil
}

func (c *contextualai) handleAPIError(statusCode int, bodyBytes []byte) error {
	var apiError contextualAIAPIError
	if err := json.Unmarshal(bodyBytes, &apiError); err == nil {
		if apiError.Message != "" {
			return fmt.Errorf("Contextual AI API error: %s", apiError.Message)
		}
		if len(apiError.Detail) > 0 && apiError.Detail[0].Msg != "" {
			return fmt.Errorf("Contextual AI API error: %s", apiError.Detail[0].Msg)
		}
	}
	return fmt.Errorf("Contextual AI API request failed with status: %d", statusCode)
}

func (c *contextualai) parseResponse(bodyBytes []byte) (*generateResponse, error) {
	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}
	return &resBody, nil
}

func (c *contextualai) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (c *contextualai) extractKnowledge(properties []*modulecapabilities.GenerateProperties) []string {
	var knowledge []string
	for _, property := range properties {
		if property.Text != nil {
			for _, text := range property.Text {
				if text != "" {
					knowledge = append(knowledge, text)
				}
			}
		}
	}
	return knowledge
}

func (c *contextualai) getContextualAIURL(ctx context.Context) (string, error) {
	baseURL := "https://api.contextual.ai"
	if value := ctx.Value(contextKey("X-ContextualAI-Baseurl")); value != nil {
		if urls, ok := value.([]string); ok && len(urls) > 0 {
			baseURL = urls[0]
		}
	} else if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-ContextualAI-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}
	return url.JoinPath(baseURL, "/v1/generate")
}

type contextKey string

func (c *contextualai) getAPIKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-ContextualAI-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if c.apiKey != "" {
		return c.apiKey, nil
	}
	return "", errors.New("no api key found for Contextual AI " +
		"neither in request header: X-ContextualAI-Api-Key " +
		"nor in the environment variable under CONTEXTUALAI_APIKEY")
}

type generateInput struct {
	Model           string    `json:"model"`
	Messages        []message `json:"messages"`
	Knowledge       []string  `json:"knowledge"`
	SystemPrompt    *string   `json:"system_prompt,omitempty"`
	AvoidCommentary *bool     `json:"avoid_commentary,omitempty"`
	Temperature     *float64  `json:"temperature,omitempty"`
	TopP            *float64  `json:"top_p,omitempty"`
	MaxNewTokens    *int      `json:"max_new_tokens,omitempty"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type generateResponse struct {
	Response string `json:"response"`
}

type contextualAIAPIError struct {
	Message string        `json:"message"`
	Detail  []errorDetail `json:"detail"`
}

type errorDetail struct {
	Loc  []string `json:"loc"`
	Msg  string   `json:"msg"`
	Type string   `json:"type"`
}
