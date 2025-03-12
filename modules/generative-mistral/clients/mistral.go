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
	"github.com/weaviate/weaviate/modules/generative-mistral/config"
	mistralparams "github.com/weaviate/weaviate/modules/generative-mistral/parameters"
)

type mistral struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *mistral {
	return &mistral{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *mistral) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *mistral) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *mistral) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	mistralUrl, err := v.getMistralUrl(ctx, params.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "join Mistral API host and path")
	}

	message := Message{
		Role:    "user",
		Content: prompt,
	}

	input := generateInput{
		Messages:    []Message{message},
		Model:       params.Model,
		Temperature: params.Temperature,
		TopP:        params.TopP,
		MaxTokens:   params.MaxTokens,
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", mistralUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Mistral API Key")
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

	if res.StatusCode != 200 || resBody.Error != nil {
		if resBody.Error != nil {
			return nil, errors.Errorf("connection to Mistral API failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to Mistral API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Choices[0].Message.Content

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
		Params: v.getResponseParams(resBody.Usage),
	}, nil
}

func (v *mistral) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{mistralparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[mistralparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (v *mistral) getParameters(cfg moduletools.ClassConfig, options interface{}) mistralparams.Params {
	settings := config.NewClassSettings(cfg)

	var params mistralparams.Params
	if p, ok := options.(mistralparams.Params); ok {
		params = p
	}
	if params.BaseURL == "" {
		params.BaseURL = settings.BaseURL()
	}
	if params.Model == "" {
		model := settings.Model()
		params.Model = model
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

func (v *mistral) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *mistral) getMistralUrl(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Mistral-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return url.JoinPath(passedBaseURL, "/v1/chat/completions")
}

func (v *mistral) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Mistral-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Mistral-Api-Key " +
		"nor in environment variable under MISTRAL_APIKEY")
}

type generateInput struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature *float64  `json:"temperature,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
}

type generateResponse struct {
	Choices []Choice
	Usage   *usage           `json:"usage,omitempty"`
	Error   *mistralApiError `json:"error,omitempty"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
	Logprobs     *string `json:"logprobs"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// need to check this
// I think you just get message
type mistralApiError struct {
	Message string `json:"message"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
