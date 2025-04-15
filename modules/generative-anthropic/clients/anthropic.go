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
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-anthropic/config"
	anthropicparams "github.com/weaviate/weaviate/modules/generative-anthropic/parameters"
)

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

func (a *anthropic) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return a.generate(ctx, cfg, forPrompt, generative.Blobs([]*modulecapabilities.GenerateProperties{properties}), options, debug)
}

func (a *anthropic) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	texts := generative.Texts(properties)
	forTask, err := generative.MakeTaskPrompt(texts, task)
	if err != nil {
		return nil, err
	}
	return a.generate(ctx, cfg, forTask, generative.Blobs(properties), options, debug)
}

func (a *anthropic) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, imageProperties []map[string]*string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := a.getParameters(cfg, options, imageProperties)
	debugInformation := a.getDebugInformation(debug, prompt)

	anthropicURL, err := a.getAnthropicURL(ctx, params.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "get anthropic url")
	}

	var content interface{}
	if len(params.Images) > 0 {
		var promptWithImage contentImageInput
		for i := range params.Images {
			promptWithImage = append(promptWithImage, contentText{
				Type: "text",
				Text: fmt.Sprintf("Image %d:", i+1),
			})
			promptWithImage = append(promptWithImage, contentImage{
				Type: "image",
				Source: contentSource{
					Type:      "base64",
					MediaType: "image/jpeg",
					Data:      params.Images[i],
				},
			})
		}
		promptWithImage = append(promptWithImage, contentText{
			Type: "text",
			Text: prompt,
		})
		content = promptWithImage
	} else {
		content = prompt
	}

	input := generateInput{
		Messages: []message{
			{
				Role:    "user",
				Content: content,
			},
		},
		Model:         params.Model,
		MaxTokens:     params.MaxTokens,
		StopSequences: params.StopSequences,
		Temperature:   params.Temperature,
		TopK:          params.TopK,
		TopP:          params.TopP,
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

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var resBody generateResponse

	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 && resBody.Type == "error" {
		return nil, fmt.Errorf("Anthropic API error: %s - %s", resBody.Error.Type, resBody.Error.Message)
	}

	textResponse := resBody.Content[0].Text
	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
		Params: a.getResponseParams(resBody.Usage),
	}, nil
}

func (a *anthropic) getParameters(cfg moduletools.ClassConfig, options interface{}, imagePropertiesArray []map[string]*string) anthropicparams.Params {
	settings := config.NewClassSettings(cfg)

	var params anthropicparams.Params
	if p, ok := options.(anthropicparams.Params); ok {
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
	if params.TopK == nil {
		topK := settings.TopK()
		params.TopK = &topK
	}
	if params.TopP == nil {
		topP := settings.TopP()
		params.TopP = &topP
	}
	if len(params.StopSequences) == 0 {
		params.StopSequences = settings.StopSequences()
	}
	if params.MaxTokens == nil {
		// respect module config settings
		maxTokens := settings.MaxTokens()
		if maxTokens == nil {
			// fallback to default values
			maxTokens = settings.GetMaxTokensForModel(params.Model)
		}
		params.MaxTokens = maxTokens
	}

	params.Images = generative.ParseImageProperties(params.Images, params.ImageProperties, imagePropertiesArray)

	return params
}

func (a *anthropic) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (a *anthropic) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{anthropicparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[anthropicparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (a *anthropic) getAnthropicURL(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Anthropic-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return url.JoinPath(passedBaseURL, "/v1/messages")
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
	Messages      []message `json:"messages,omitempty"`
	Model         string    `json:"model,omitempty"`
	MaxTokens     *int      `json:"max_tokens,omitempty"`
	StopSequences []string  `json:"stop_sequences,omitempty"`
	Temperature   *float64  `json:"temperature,omitempty"`
	TopK          *int      `json:"top_k,omitempty"`
	TopP          *float64  `json:"top_p,omitempty"`
}

type message struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"`
}

type contentImageInput []interface{}

type contentText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type contentImage struct {
	Type   string        `json:"type"`
	Source contentSource `json:"source"`
}

type contentSource struct {
	Type      string  `json:"type"`
	MediaType string  `json:"media_type"`
	Data      *string `json:"data,omitempty"`
}

type generateResponse struct {
	Type         string       `json:"type"`
	Error        errorMessage `json:"error,omitempty"`
	ID           string       `json:"id,omitempty"`
	Role         string       `json:"role,omitempty"`
	Content      []content    `json:"content,omitempty"`
	Model        string       `json:"model,omitempty"`
	StopReason   StopReason   `json:"stop_reason,omitempty"`
	StopSequence string       `json:"stop_sequence,omitempty"`
	Usage        *usage       `json:"usage,omitempty"`
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
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
}

type errorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
