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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-octoai/config"
	octoparams "github.com/weaviate/weaviate/modules/generative-octoai/parameters"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

type octoai struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *octoai {
	return &octoai{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *octoai) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *octoai) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *octoai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	octoAIUrl, isImage, err := v.getOctoAIUrl(ctx, settings.BaseURL())
	if err != nil {
		return nil, errors.Wrap(err, "join OctoAI API host and path")
	}
	octoAIPrompt := []map[string]string{
		{"role": "system", "content": "You are a helpful assistant."},
		{"role": "user", "content": prompt},
	}

	var input interface{}
	if !isImage {
		input = generateInputText{
			Messages:    octoAIPrompt,
			Model:       params.Model,
			MaxTokens:   params.MaxTokens,
			Temperature: params.Temperature,
			N:           params.N,
			TopP:        params.TopP,
		}
	} else {
		input = generateInputImage{
			Prompt:         prompt,
			NegativePrompt: "ugly, tiling, poorly drawn hands, poorly drawn feet, poorly drawn face, out of frame, extra limbs, disfigured, deformed, body out of frame, blurry, bad anatomy, blurred, watermark, grainy, signature, cut off, draft",
			Sampler:        "DDIM",
			CfgScale:       11,
			Height:         1024,
			Width:          1024,
			Seed:           0,
			Steps:          20,
			NumImages:      1,
			HighNoiseFrac:  0.7,
			Strength:       0.92,
			UseRefiner:     true,
		}
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", octoAIUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "OctoAI API Key")
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
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		if resBody.Error != nil {
			return nil, errors.Errorf("connection to OctoAI API failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to OctoAI API failed with status: %d", res.StatusCode)
	}

	var textResponse string
	if isImage {
		textResponse = resBody.Images[0].Image
	} else {
		textResponse = resBody.Choices[0].Message.Content
	}

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
		Params: v.getResponseParams(resBody.Usage),
	}, nil
}

func (v *octoai) getParameters(cfg moduletools.ClassConfig, options interface{}) octoparams.Params {
	settings := config.NewClassSettings(cfg)

	var params octoparams.Params
	if p, ok := options.(octoparams.Params); ok {
		params = p
	}

	if params.Model == "" {
		params.Model = settings.Model()
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

func (v *octoai) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *octoai) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{"octoai": map[string]interface{}{"usage": usage}}
	}
	return nil
}

func (v *octoai) getOctoAIUrl(ctx context.Context, baseURL string) (string, bool, error) {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Octoai-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	if strings.Contains(passedBaseURL, "image") {
		urlTmp, err := url.JoinPath(passedBaseURL, "/generate/sdxl")
		return urlTmp, true, err
	}
	urlTmp, err := url.JoinPath(passedBaseURL, "/v1/chat/completions")
	return urlTmp, false, err
}

func (v *octoai) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *octoai) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *octoai) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Octoai-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-OctoAI-Api-Key " +
		"nor in environment variable under OCTOAI_APIKEY")
}

type generateInputText struct {
	Model       string              `json:"model"`
	Messages    []map[string]string `json:"messages"`
	MaxTokens   *int                `json:"max_tokens"`
	Temperature *float64            `json:"temperature,omitempty"`
	N           *int                `json:"n,omitempty"`
	TopP        *float64            `json:"top_p,omitempty"`
}

type generateInputImage struct {
	Prompt         string  `json:"prompt"`
	NegativePrompt string  `json:"negative_prompt"`
	Sampler        string  `json:"sampler"`
	CfgScale       int     `json:"cfg_scale"`
	Height         int     `json:"height"`
	Width          int     `json:"width"`
	Seed           int     `json:"seed"`
	Steps          int     `json:"steps"`
	NumImages      int     `json:"num_images"`
	HighNoiseFrac  float64 `json:"high_noise_frac"`
	Strength       float64 `json:"strength"`
	UseRefiner     bool    `json:"use_refiner"`
	// StylePreset    string  `json:"style_preset"`
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

type Image struct {
	Image string `json:"image_b64"`
}

type generateResponse struct {
	Choices []Choice
	Images  []Image
	Usage   *usage `json:"usage,omitempty"`

	Error *octoaiApiError `json:"error,omitempty"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type octoaiApiError struct {
	Message string `json:"message"`
}
