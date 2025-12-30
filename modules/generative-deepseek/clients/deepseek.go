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
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generative"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/modules/generative-deepseek/config"
	deepseekparams "github.com/weaviate/weaviate/modules/generative-deepseek/parameters"
)

type deepseek struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *deepseek {
	return &deepseek{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *deepseek) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues("generate", "deepseek").Inc()
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forPrompt, generative.Blobs([]*modulecapabilities.GenerateProperties{properties}), options, debug)
}

func (v *deepseek) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequestBatchCount.WithLabelValues("generate", "deepseek").Inc()
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.generate(ctx, cfg, forTask, generative.Blobs(properties), options, debug)
}

func (v *deepseek) generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, imageProperties []map[string]*string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	monitoring.GetMetrics().ModuleExternalRequests.WithLabelValues("generate", "deepseek").Inc()
	startTime := time.Now()

	params := v.getParameters(cfg, options, imageProperties)
	debugInformation := v.getDebugInformation(debug, prompt)

	apiUrl, err := v.getApiUrl(ctx, params)
	if err != nil {
		return nil, errors.Wrap(err, "url join path")
	}

	input, err := v.generateInput(prompt, params)
	if err != nil {
		return nil, errors.Wrap(err, "generate input")
	}

	defer func() {
		monitoring.GetMetrics().ModuleExternalRequestDuration.WithLabelValues("generate", apiUrl).Observe(time.Since(startTime).Seconds())
	}()

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	monitoring.GetMetrics().ModuleExternalRequestSize.WithLabelValues("generate", apiUrl).Observe(float64(len(body)))

	req, err := http.NewRequestWithContext(ctx, "POST", apiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "DeepSeek API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	res, err := v.httpClient.Do(req)
	if res != nil {
		vrst := monitoring.GetMetrics().ModuleExternalResponseStatus
		vrst.WithLabelValues("generate", apiUrl, fmt.Sprintf("%v", res.StatusCode)).Inc()
	}
	if err != nil {
		code := -1
		if res != nil {
			code = res.StatusCode
		}
		monitoring.GetMetrics().ModuleExternalError.WithLabelValues("generate", "deepseek", "DeepSeek API", fmt.Sprintf("%v", code)).Inc()
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	requestID := res.Header.Get("x-request-id")
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	monitoring.GetMetrics().ModuleExternalResponseSize.WithLabelValues("generate", apiUrl).Observe(float64(len(bodyBytes)))

	var resBody generateResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal response body. Got: %v", string(bodyBytes)))
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		return nil, v.getError(res.StatusCode, requestID, resBody.Error)
	}

	responseParams := v.getResponseParams(resBody.Usage)

	if len(resBody.Choices) > 0 {
		textResponse := resBody.Choices[0].Text
		if textResponse == "" && resBody.Choices[0].Message != nil {
			textResponse = resBody.Choices[0].Message.Content
		}

		if textResponse != "" {
			trimmedResponse := strings.Trim(textResponse, "\n")
			return &modulecapabilities.GenerateResponse{
				Result: &trimmedResponse,
				Debug:  debugInformation,
				Params: responseParams,
			}, nil
		}
	}

	return &modulecapabilities.GenerateResponse{
		Result: nil,
		Debug:  debugInformation,
	}, nil
}

func (v *deepseek) getParameters(cfg moduletools.ClassConfig, options interface{}, imagePropertiesArray []map[string]*string) deepseekparams.Params {
	settings := config.NewClassSettings(cfg)

	var params deepseekparams.Params
	if p, ok := options.(deepseekparams.Params); ok {
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
		if temperature != nil {
			params.Temperature = temperature
		}
	}
	if params.TopP == nil {
		topP := settings.TopP()
		params.TopP = &topP
	}
	if params.FrequencyPenalty == nil {
		frequencyPenalty := settings.FrequencyPenalty()
		params.FrequencyPenalty = &frequencyPenalty
	}
	if params.PresencePenalty == nil {
		presencePenalty := settings.PresencePenalty()
		params.PresencePenalty = &presencePenalty
	}
	if params.MaxTokens == nil {
		if settings.MaxTokens() != nil && *settings.MaxTokens() != -1 {
			maxTokens := int(*settings.MaxTokens())
			params.MaxTokens = &maxTokens
		}
	}

	params.Images = generative.ParseImageProperties(params.Images, params.ImageProperties, imagePropertiesArray)

	return params
}

func (v *deepseek) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *deepseek) getResponseParams(usage *usage) map[string]interface{} {
	if usage != nil {
		return map[string]interface{}{deepseekparams.Name: map[string]interface{}{"usage": usage}}
	}
	return nil
}

func (v *deepseek) getApiUrl(ctx context.Context, params deepseekparams.Params) (string, error) {
	baseURL := params.BaseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Openai-Baseurl"); headerBaseURL != "" {
		baseURL = headerBaseURL
	}
	return url.JoinPath(baseURL, "/chat/completions")
}

func (v *deepseek) generateInput(prompt string, params deepseekparams.Params) (generateInput, error) {

	var input generateInput
	var content any

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
		Messages:            messages,
		Stream:              false,
		MaxCompletionTokens: params.MaxTokens,
		FrequencyPenalty:    params.FrequencyPenalty,
		N:                   params.N,
		PresencePenalty:     params.PresencePenalty,
		Stop:                params.Stop,
		Temperature:         params.Temperature,
		TopP:                params.TopP,
		Model:               params.Model,
	}

	return input, nil
}

func (v *deepseek) getError(statusCode int, requestID string, resBodyError *openAIApiError) error {
	errorMsg := fmt.Sprintf("connection to: DeepSeek API failed with status: %d", statusCode)
	if requestID != "" {
		errorMsg = fmt.Sprintf("%s request-id: %s", errorMsg, requestID)
	}
	if resBodyError != nil {
		errorMsg = fmt.Sprintf("%s error: %v", errorMsg, resBodyError.Message)
	}
	monitoring.GetMetrics().ModuleExternalError.WithLabelValues("generate", "deepseek", "DeepSeek API", fmt.Sprintf("%v", statusCode)).Inc()
	return errors.New(errorMsg)
}

func (v *deepseek) getApiKey(ctx context.Context) (string, error) {
	// Check for header override
	if apiKeyValue := modulecomponents.GetValueFromContext(ctx, "X-Deepseek-Api-Key"); apiKeyValue != "" {
		return apiKeyValue, nil
	}
	// Fallback to configured key
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", fmt.Errorf("no api key found")
}

type generateInput struct {
	Prompt              string    `json:"prompt,omitempty"`
	Messages            []message `json:"messages,omitempty"`
	Stream              bool      `json:"stream,omitempty"`
	Model               string    `json:"model,omitempty"`
	FrequencyPenalty    *float64  `json:"frequency_penalty,omitempty"`
	MaxCompletionTokens *int      `json:"max_completion_tokens,omitempty"`
	MaxTokens           *int      `json:"max_tokens,omitempty"`
	N                   *int      `json:"n,omitempty"`
	PresencePenalty     *float64  `json:"presence_penalty,omitempty"`
	Stop                []string  `json:"stop,omitempty"`
	Temperature         *float64  `json:"temperature,omitempty"`
	TopP                *float64  `json:"top_p,omitempty"`
}

type responseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

type message struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"`
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

func (v *deepseek) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "Generative Search - DeepSeek",
		"documentationHref": "https://api-docs.deepseek.com/",
	}, nil
}
