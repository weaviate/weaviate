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
	"github.com/weaviate/weaviate/modules/generative-octoai/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
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

func (v *octoai) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *octoai) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *octoai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

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
			Model:       settings.Model(),
			MaxTokens:   settings.MaxTokens(),
			Temperature: settings.Temperature(),
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

	return &generativemodels.GenerateResponse{
		Result: &textResponse,
	}, nil
}

func (v *octoai) getOctoAIUrl(ctx context.Context, baseURL string) (string, bool, error) {
	passedBaseURL := baseURL
	if headerBaseURL := v.getValueFromContext(ctx, "X-Octoai-Baseurl"); headerBaseURL != "" {
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

func (v *octoai) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, key); apiKeyValue != "" {
		return apiKeyValue
	}
	return ""
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
	MaxTokens   int                 `json:"max_tokens"`
	Temperature int                 `json:"temperature"`
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

	Error *octoaiApiError `json:"error,omitempty"`
}

type octoaiApiError struct {
	Message string `json:"message"`
}
