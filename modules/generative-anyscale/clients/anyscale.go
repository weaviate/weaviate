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
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-anyscale/config"
	anyscaleparams "github.com/weaviate/weaviate/modules/generative-anyscale/parameters"
)

type anyscale struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *anyscale {
	return &anyscale{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *anyscale) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *anyscale) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *anyscale) getParameters(cfg moduletools.ClassConfig, options interface{}) anyscaleparams.Params {
	settings := config.NewClassSettings(cfg)

	var params anyscaleparams.Params
	if p, ok := options.(anyscaleparams.Params); ok {
		params = p
	}
	if params.BaseURL == "" {
		baseURL := settings.BaseURL()
		params.BaseURL = baseURL
	}
	if params.Model == "" {
		model := settings.Model()
		params.Model = model
	}
	if params.Temperature == nil {
		temperature := settings.Temperature()
		params.Temperature = &temperature
	}
	return params
}

func (v *anyscale) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	anyscaleUrl := v.getAnyscaleUrl(ctx, params.BaseURL)
	anyscalePrompt := []map[string]string{
		{"role": "system", "content": "You are a helpful assistant."},
		{"role": "user", "content": prompt},
	}
	input := generateInput{
		Messages:    anyscalePrompt,
		Model:       params.Model,
		Temperature: params.Temperature,
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", anyscaleUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Anyscale (OpenAI) API Key")
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
			return nil, errors.Errorf("connection to Anyscale API failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to Anyscale API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Choices[0].Message.Content

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
	}, nil
}

func (v *anyscale) getAnyscaleUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Anyscale-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/v1/chat/completions", passedBaseURL)
}

func (v *anyscale) getApiKey(ctx context.Context) (string, error) {
	// note Anyscale uses the OpenAI API Key in it's requests.
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Anyscale-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Anyscale-Api-Key " +
		"nor in environment variable under ANYSCALE_APIKEY")
}

func (v *anyscale) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

type generateInput struct {
	Model       string              `json:"model"`
	Messages    []map[string]string `json:"messages"`
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

// The entire response for an error ends up looking different, may want to add omitempty everywhere.
type generateResponse struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []Choice          `json:"choices"`
	Usage   map[string]int    `json:"usage"`
	Error   *anyscaleApiError `json:"error,omitempty"`
}

type anyscaleApiError struct {
	Message string `json:"message"`
}
