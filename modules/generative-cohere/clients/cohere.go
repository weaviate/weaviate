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
	"github.com/weaviate/weaviate/modules/generative-cohere/config"
	cohereparams "github.com/weaviate/weaviate/modules/generative-cohere/parameters"
)

type contentType string

var (
	contentTypeText     contentType = "text"
	contentTypeImageUrl contentType = "image_url"
)

type cohere struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *cohere {
	return &cohere{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *cohere) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := generative.MakeSinglePrompt(generative.Text(properties), prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, generative.Blobs([]*modulecapabilities.GenerateProperties{properties}), options, debug)
}

func (v *cohere) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := generative.MakeTaskPrompt(generative.Texts(properties), task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, generative.Blobs(properties), options, debug)
}

func (v *cohere) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, imageProperties []map[string]*string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	params := v.getParameters(cfg, options, imageProperties)
	debugInformation := v.getDebugInformation(debug, prompt)

	cohereUrl, err := v.getCohereUrl(ctx, params.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "join Cohere API host and path")
	}

	contents := []content{
		{Type: contentTypeText, Text: &prompt},
	}
	for i := range params.Images {
		contents = append(contents, content{
			Type: contentTypeImageUrl,
			ImageURL: &imageUrl{
				URL: fmt.Sprintf("data:image/png;base64,%s", *params.Images[i]),
			},
		})
	}

	input := generateInput{
		Model: params.Model,
		Messages: []message{
			{
				Role:    "user",
				Content: contents,
			},
		},
		MaxTokens:        params.MaxTokens,
		StopSequences:    params.StopSequences,
		Temperature:      params.Temperature,
		FrequencyPenalty: params.FrequencyPenalty,
		PresencePenalty:  params.PresencePenalty,
		K:                params.K,
		P:                params.P,
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cohereUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Cohere API Key")
	}
	req.Header.Add("Authorization", fmt.Sprintf("BEARER %s", apiKey))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Request-Source", "unspecified:weaviate")

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
	unmarshalErr := json.Unmarshal(bodyBytes, &resBody)
	if res.StatusCode != 200 || (unmarshalErr == nil && resBody.FinishReason == "ERROR") {
		if resBody.Message.ErrorMessage != "" {
			return nil, errors.Errorf("connection to Cohere API failed with status: %d error: %v", res.StatusCode, resBody.Message.ErrorMessage)
		}
		return nil, errors.Errorf("connection to Cohere API failed with status: %d", res.StatusCode)
	}
	if unmarshalErr != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unmarshal Cohere API response body. Got: %v", string(bodyBytes)))
	}

	textResponse := resBody.Message.GetMessage()

	return &modulecapabilities.GenerateResponse{
		Result: &textResponse,
		Debug:  debugInformation,
		Params: v.getResponseParams(resBody.Usage),
	}, nil
}

func (v *cohere) getParameters(cfg moduletools.ClassConfig, options interface{}, imagePropertiesArray []map[string]*string) cohereparams.Params {
	settings := config.NewClassSettings(cfg)

	var params cohereparams.Params
	if p, ok := options.(cohereparams.Params); ok {
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
	if params.K == nil {
		k := settings.K()
		params.K = &k
	}
	if len(params.StopSequences) == 0 {
		params.StopSequences = settings.StopSequences()
	}
	if params.MaxTokens == nil {
		maxTokens := settings.GetMaxTokensForModel(params.Model)
		params.MaxTokens = &maxTokens
	}

	params.Images = generative.ParseImageProperties(params.Images, params.ImageProperties, imagePropertiesArray)

	return params
}

func (v *cohere) getDebugInformation(debug bool, prompt string) *modulecapabilities.GenerateDebugInformation {
	if debug {
		return &modulecapabilities.GenerateDebugInformation{
			Prompt: prompt,
		}
	}
	return nil
}

func (v *cohere) getResponseParams(meta *usage) map[string]interface{} {
	if meta != nil {
		return map[string]interface{}{cohereparams.Name: map[string]interface{}{"meta": meta}}
	}
	return nil
}

func GetResponseParams(result map[string]interface{}) *responseParams {
	if params, ok := result[cohereparams.Name].(map[string]interface{}); ok {
		if meta, ok := params["meta"].(*usage); ok {
			return &responseParams{Meta: meta}
		}
	}
	return nil
}

func (v *cohere) getCohereUrl(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL := baseURL
	if headerBaseURL := modulecomponents.GetValueFromContext(ctx, "X-Cohere-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return url.JoinPath(passedBaseURL, "/v2/chat")
}

func (v *cohere) getApiKey(ctx context.Context) (string, error) {
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Cohere-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if v.apiKey != "" {
		return v.apiKey, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Cohere-Api-Key " +
		"nor in environment variable under COHERE_APIKEY")
}

type generateInput struct {
	Model            string    `json:"model"`
	Messages         []message `json:"messages"`
	MaxTokens        *int      `json:"max_tokens,omitempty"`
	StopSequences    []string  `json:"stop_sequences,omitempty"`
	Temperature      *float64  `json:"temperature,omitempty"`
	FrequencyPenalty *float64  `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64  `json:"presence_penalty,omitempty"`
	K                *int      `json:"k,omitempty"`
	P                *float64  `json:"p,omitempty"`
}

type message struct {
	Role    string    `json:"role,omitempty"`
	Content []content `json:"content,omitempty"`
}

type content struct {
	Type     contentType `json:"type"`
	Text     *string     `json:"text,omitempty"`
	ImageURL *imageUrl   `json:"image_url,omitempty"`
}

type imageUrl struct {
	URL string `json:"url"`
}

type generateResponse struct {
	ID           string `json:"id"`
	FinishReason string `json:"finish_reason"`
	// When an error occurs then the error message object is being returned with an error message
	// https://docs.cohere.com/reference/errors
	Message responseMessage `json:"message"`
	Usage   *usage          `json:"usage,omitempty"`
}

type responseMessage struct {
	Role         string            `json:"role,omitempty"`
	Content      []responseContent `json:"content,omitempty"`
	ErrorMessage string
}

func (rm *responseMessage) GetMessage() string {
	if rm.ErrorMessage != "" {
		return rm.ErrorMessage
	}
	if len(rm.Content) > 0 {
		return rm.Content[0].Text
	}
	return ""
}

// UnmarshalJSON implements custom unmarshaling for responseMessage
func (rm *responseMessage) UnmarshalJSON(data []byte) error {
	var errorMessage string
	if err := json.Unmarshal(data, &errorMessage); err == nil {
		rm.ErrorMessage = errorMessage
		return nil
	}

	type Alias responseMessage
	var temp Alias
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}
	*rm = responseMessage(temp)
	return nil
}

type responseContent struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type usage struct {
	BilledUnits *billedUnits `json:"billed_units,omitempty"`
	Tokens      *tokens      `json:"tokens,omitempty"`
	ApiVersion  *apiVersion  `json:"api_version,omitempty"`
	Warnings    []string     `json:"warnings,omitempty"`
}

type apiVersion struct {
	Version        *string `json:"version,omitempty"`
	IsDeprecated   *bool   `json:"is_deprecated,omitempty"`
	IsExperimental *bool   `json:"is_experimental,omitempty"`
}

type billedUnits struct {
	InputTokens     *float64 `json:"input_tokens,omitempty"`
	OutputTokens    *float64 `json:"output_tokens,omitempty"`
	SearchUnits     *float64 `json:"search_units,omitempty"`
	Classifications *float64 `json:"classifications,omitempty"`
}

type tokens struct {
	InputTokens  *float64 `json:"input_tokens,omitempty"`
	OutputTokens *float64 `json:"output_tokens,omitempty"`
}

type responseParams struct {
	Meta *usage `json:"meta,omitempty"`
}
