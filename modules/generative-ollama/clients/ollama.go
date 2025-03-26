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

package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/weaviate/weaviate/modules/generative-ollama/config"
	ollamaparams "github.com/weaviate/weaviate/modules/generative-ollama/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type ollama struct {
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(timeout time.Duration, logger logrus.FieldLogger) *ollama {
	return &ollama{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *ollama) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forPrompt, generative.Blobs([]*modulecapabilities.GenerateProperties{properties}), options, debug)
}

func (v *ollama) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forTask, generative.Blobs(properties), options, debug)
}

func (v *ollama) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, imageProperties []map[string]*string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options, imageProperties)
	debugInformation := v.getDebugInformation(debug, prompt)

	ollamaUrl := v.getOllamaUrl(ctx, params.ApiEndpoint)
	input := generateInput{
		Model:  params.Model,
		Prompt: prompt,
		Stream: false,
	}
	if params.Temperature != nil {
		input.Options = &generateOptions{Temperature: params.Temperature}
	}
	if len(params.Images) > 0 {
		input.Images = params.Images
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ollamaUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
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

	if resBody.Error != "" {
		return nil, errors.Errorf("connection to Ollama API failed with error: %s", resBody.Error)
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("connection to Ollama API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Response

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
	}, nil
}

func (v *ollama) getParameters(cfg moduletools.ClassConfig, options interface{}, imagePropertiesArray []map[string]*string) ollamaparams.Params {
	settings := config.NewClassSettings(cfg)

	var params ollamaparams.Params
	if p, ok := options.(ollamaparams.Params); ok {
		params = p
	}
	if params.ApiEndpoint == "" {
		params.ApiEndpoint = settings.ApiEndpoint()
	}
	if params.Model == "" {
		params.Model = settings.Model()
	}

	params.Images = generative.ParseImageProperties(params.Images, params.ImageProperties, imagePropertiesArray)

	return params
}

func (v *ollama) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *ollama) getOllamaUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Ollama-BaseURL"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/api/generate", passedBaseURL)
}

type generateInput struct {
	Model   string           `json:"model"`
	Prompt  string           `json:"prompt"`
	Stream  bool             `json:"stream"`
	Options *generateOptions `json:"options,omitempty"`
	Images  []*string        `json:"images,omitempty"`
}

type generateOptions struct {
	Temperature *float64 `json:"temperature,omitempty"`
}

// The entire response for an error ends up looking different, may want to add omitempty everywhere.
type generateResponse struct {
	Model              string `json:"model,omitempty"`
	CreatedAt          string `json:"created_at,omitempty"`
	Response           string `json:"response,omitempty"`
	Done               bool   `json:"done,omitempty"`
	Context            []int  `json:"context,omitempty"`
	TotalDuration      int    `json:"total_duration,omitempty"`
	LoadDuration       int    `json:"load_duration,omitempty"`
	PromptEvalDuration int    `json:"prompt_eval_duration,omitempty"`
	EvalCount          int    `json:"eval_count,omitempty"`
	EvalDuration       int    `json:"eval_duration,omitempty"`
	Error              string `json:"error,omitempty"`
}
