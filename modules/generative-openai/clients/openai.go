//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-openai/config"
	"github.com/weaviate/weaviate/modules/generative-openai/ent"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type openai struct {
	apiKey     string
	host       string
	path       string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *openai {
	return &openai{
		apiKey: apiKey,
		httpClient: &http.Client{
			//todo check if we need longer than this!
			Timeout: 60 * time.Second,
		},
		host:   "https://api.openai.com",
		path:   "/v1/completions",
		logger: logger,
	}
}

func (v *openai) Generate(ctx context.Context, text, task, language string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	prompt := v.generatePrompt(text, task, language)

	settings := config.NewClassSettings(cfg)

	body, err := json.Marshal(generateInput{
		Prompt:           prompt,
		Model:            settings.Model(),
		MaxTokens:        settings.MaxTokens(),
		Temperature:      settings.Temperature(),
		FrequencyPenalty: settings.FrequencyPenalty(),
		PresencePenalty:  settings.PresencePenalty(),
		TopP:             settings.TopP(),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "marshal body")
	}

	oaiUrl, err := url.JoinPath(v.host, v.path)
	if err != nil {
		return nil, errors.Wrap(err, "join OpenAI API host and path")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", oaiUrl,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}
	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "OpenAI API Key")
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

	if res.StatusCode > 399 {
		if resBody.Error != nil {
			return nil, errors.Errorf("failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("failed with status: %d", res.StatusCode)
	}
	textResponse := resBody.Choices[0].Text
	if len(resBody.Choices) > 0 && textResponse != "" {
		//todo [@marcin, should we do this? seems like OpenAI returns the \n from the prompt I think for some reason]
		replaceAll := strings.ReplaceAll(textResponse, "\n", "")
		return &ent.GenerateResult{
			Result: &replaceAll,
		}, nil
	}
	return &ent.GenerateResult{
		Result: nil,
	}, nil
}

func (v *openai) generatePrompt(text string, question string, language string) string {
	//todo [byron - this is the prompt created by Bob/Connor, check if it actually performs better than simple piping
	//todo of question + language check out code commented out below code]
	return fmt.Sprintf(`We need your help to complete the task: %v
	
	Additional instructions are as follows:
	- You must base your response on the result in JSON format and nothing else.
	- The response should be in %v.
	- You should only base your results on the result in JSON format. If you can't do this, could you explain why you can't do this?
	- You should only respond with the result to the task, nothing more.
	- You should be kind and decent.
	
	This is the result in JSON format:
	{
		%v
	}
`,
		question, language, strings.ReplaceAll(text, "\n", " "))
	//todo [save this for later, in case needed]
	//return fmt.Sprintf(`'%v in the %v language:
	//%v
	//`, question, language, strings.ReplaceAll(text, "\n", " "))
}

func (v *openai) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	apiKey := ctx.Value("X-Openai-Api-Key")
	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0], nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-OpenAI-Api-Key " +
		"nor in environment variable under OPENAI_APIKEY")
}

type generateInput struct {
	Prompt           string   `json:"prompt"`
	Model            string   `json:"model"`
	MaxTokens        float64  `json:"max_tokens"`
	Temperature      float64  `json:"temperature"`
	Stop             []string `json:"stop"`
	FrequencyPenalty float64  `json:"frequency_penalty"`
	PresencePenalty  float64  `json:"presence_penalty"`
	TopP             float64  `json:"top_p"`
}

type generateResponse struct {
	Choices []choice
	Error   *openAIApiError `json:"error,omitempty"`
}

type choice struct {
	FinishReason string
	Index        float32
	Logprobs     string
	Text         string
}

type openAIApiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param"`
	Code    string `json:"code"`
}
