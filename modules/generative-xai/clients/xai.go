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
	"strconv"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-xai/config"
	xaiparams "github.com/weaviate/weaviate/modules/generative-xai/parameters"
)

type xai struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *xai {
	return &xai{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *xai) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forPrompt, options, debug)
}

func (v *xai) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forTask, options, debug)
}

func (v *xai) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options)
	debugInformation := v.getDebugInformation(debug, prompt)

	xaiUrl := v.getXaiUrl(ctx, params.BaseURL)
	input := v.getRequest(prompt, params)

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", xaiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "xAI API Key")
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
			return nil, errors.Errorf("connection to xAI API failed with status: %d error: %s", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to xAI API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Choices[0].Message.Content

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
		Params: v.getResponseParams(resBody.Usage),
	}, nil
}

func (v *xai) getRequest(prompt string, params xaiparams.Params) generateInput {
	var input generateInput

	var content interface{}
	if len(params.Images) > 0 {
		imageInput := contentImageInput{}
		imageInput = append(imageInput, contentText{
			Type: "text",
			Text: prompt,
		})
		for i := range params.Images {
			url := fmt.Sprintf("data:image/jpeg;base64,%s", *params.Images[i])
			imageInput = append(imageInput, contentImage{
				Type:     "image_url",
				ImageURL: contentImageURL{URL: &url},
			})
		}
		content = imageInput
	} else {
		content = prompt
	}

	messages := []message{{
		Role:    "user",
		Content: content,
	}}

	input = generateInput{
		Messages:    messages,
		Model:       params.Model,
		Stream:      false,
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
		TopP:        params.TopP,
	}

	return input
}

func (v *xai) getParameters(cfg moduletools.ClassConfig, options interface{}) xaiparams.Params {
	settings := config.NewClassSettings(cfg)

	var params xaiparams.Params
	if p, ok := options.(xaiparams.Params); ok {
		params = p
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
	if params.MaxTokens == nil {
		params.MaxTokens = settings.MaxTokens()
	}
	if params.TopP == nil {
		params.TopP = settings.TopP()
	}
	return params
}

func (v *xai) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *xai) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{xaiparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[xaiparams.Name].(map[string]interface{}); ok {
		if usage, ok := params["usage"].(*usage); ok {
			return &responseParams{Usage: usage}
		}
	}
	return nil
}

func (v *xai) getXaiUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Xai-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/v1/chat/completions", passedBaseURL)
}

func (v *xai) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Xai-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-xAI-Api-Key " +
		"nor in environment variable under XAI_APIKEY")
}

type generateInput struct {
	Prompt           string    `json:"prompt,omitempty"`
	Messages         []message `json:"messages,omitempty"`
	Stream           bool      `json:"stream,omitempty"`
	Model            string    `json:"model,omitempty"`
	FrequencyPenalty *float64  `json:"frequency_penalty,omitempty"`
	Logprobs         *bool     `json:"logprobs,omitempty"`
	TopLogprobs      *int      `json:"top_logprobs,omitempty"`
	MaxTokens        *int      `json:"max_tokens,omitempty"`
	N                *int      `json:"n,omitempty"`
	PresencePenalty  *float64  `json:"presence_penalty,omitempty"`
	Stop             []string  `json:"stop,omitempty"`
	Temperature      *float64  `json:"temperature,omitempty"`
	TopP             *float64  `json:"top_p,omitempty"`
}

type responseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

type message struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"` // string or array of contentText and contentImage
	Name    string      `json:"name,omitempty"`
}

type contentImageInput []interface{}

type contentText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type contentImage struct {
	Type     string          `json:"type"`
	ImageURL contentImageURL `json:"image_url,omitempty"`
}

type contentImageURL struct {
	URL *string `json:"url"`
}

type generateResponse struct {
	Choices []choice
	Usage   *usage          `json:"usage,omitempty"`
	Error   *openAIApiError `json:"error,omitempty"`
}

type choice struct {
	FinishReason string
	Index        float32
	Text         string           `json:"text,omitempty"`
	Message      *responseMessage `json:"message,omitempty"`
}

type openAIApiError struct {
	Message string     `json:"message"`
	Type    string     `json:"type"`
	Param   string     `json:"param"`
	Code    openAICode `json:"code"`
}

type usage struct {
	PromptTokens     *int `json:"prompt_tokens,omitempty"`
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	TotalTokens      *int `json:"total_tokens,omitempty"`
}

type openAICode string

func (c *openAICode) String() string {
	if c == nil {
		return ""
	}
	return string(*c)
}

func (c *openAICode) UnmarshalJSON(data []byte) (err error) {
	if number, err := strconv.Atoi(string(data)); err == nil {
		str := strconv.Itoa(number)
		*c = openAICode(str)
		return nil
	}
	var str string
	err = json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*c = openAICode(str)
	return nil
}

type responseParams struct {
	Usage *usage `json:"usage,omitempty"`
}
