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
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/qna-openai/config"
	"github.com/weaviate/weaviate/modules/qna-openai/ent"
)

type qna struct {
	apiKey     string
	host       string
	path       string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *qna {
	return &qna{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		host:       "https://api.openai.com",
		path:       "/v1/completions",
		logger:     logger,
	}
}

func (v *qna) Answer(ctx context.Context, text, question string, cfg moduletools.ClassConfig) (*ent.AnswerResult, error) {
	prompt := v.generatePrompt(text, question)

	settings := config.NewClassSettings(cfg)

	body, err := json.Marshal(answersInput{
		Prompt:           prompt,
		Model:            settings.Model(),
		MaxTokens:        settings.MaxTokens(),
		Temperature:      settings.Temperature(),
		Stop:             []string{"\n"},
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

	var resBody answersResponse
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode >= 500 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Error, "connection to OpenAI failed with status: %d error: %v")
		return nil, errors.Errorf(errorMessage)
	} else if res.StatusCode >= 400 {
		errorMessage := getErrorMessage(res.StatusCode, resBody.Error, "failed with status: %d")
		return nil, errors.Errorf(errorMessage)

	}

	if len(resBody.Choices) > 0 && resBody.Choices[0].Text != "" {
		return &ent.AnswerResult{
			Text:     text,
			Question: question,
			Answer:   &resBody.Choices[0].Text,
		}, nil
	}
	return &ent.AnswerResult{
		Text:     text,
		Question: question,
		Answer:   nil,
	}, nil
}

func getErrorMessage(statusCode int, resBodyError *openAIApiError, errorTemplate string) string {
	if resBodyError != nil {
		return fmt.Sprintf(errorTemplate, statusCode, resBodyError.Message)
	}
	return fmt.Sprintf(errorTemplate, statusCode)
}

func (v *qna) generatePrompt(text string, question string) string {
	return fmt.Sprintf(`'Please answer the question according to the above context.

===
Context: %v
===
Q: %v
A:`, strings.ReplaceAll(text, "\n", " "), question)
}

func (v *qna) getApiKey(ctx context.Context) (string, error) {
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

type answersInput struct {
	Prompt           string   `json:"prompt"`
	Model            string   `json:"model"`
	MaxTokens        float64  `json:"max_tokens"`
	Temperature      float64  `json:"temperature"`
	Stop             []string `json:"stop"`
	FrequencyPenalty float64  `json:"frequency_penalty"`
	PresencePenalty  float64  `json:"presence_penalty"`
	TopP             float64  `json:"top_p"`
}

type answersResponse struct {
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
