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
	"github.com/weaviate/weaviate/modules/generative-cohere/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

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

func (v *cohere) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *cohere) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *cohere) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

	cohereUrl, err := v.getCohereUrl(ctx, settings.BaseURL())
	if err != nil {
		return nil, errors.Wrap(err, "join Cohere API host and path")
	}
	input := generateInput{
		Message:       prompt,
		Model:         settings.Model(),
		MaxTokens:     settings.MaxTokens(),
		Temperature:   settings.Temperature(),
		K:             settings.K(),
		StopSequences: settings.StopSequences(),
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
	if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 {
		if resBody.Message != "" {
			return nil, errors.Errorf("connection to Cohere API failed with status: %d error: %v", res.StatusCode, resBody.Message)
		}
		return nil, errors.Errorf("connection to Cohere API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Text

	return &generativemodels.GenerateResponse{
		Result: &textResponse,
	}, nil
}

func (v *cohere) getCohereUrl(ctx context.Context, baseURL string) (string, error) {
	passedBaseURL := baseURL
	if headerBaseURL := v.getValueFromContext(ctx, "X-Cohere-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return url.JoinPath(passedBaseURL, "/v1/chat")
}

func (v *cohere) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *cohere) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *cohere) getValueFromContext(ctx context.Context, key string) string {
	if value := ctx.Value(key); value != nil {
		if keyHeader, ok := value.([]string); ok && len(keyHeader) > 0 && len(keyHeader[0]) > 0 {
			return keyHeader[0]
		}
	}
	// try getting header from GRPC if not successful
	if apiKey := modulecomponents.GetValueFromGRPC(ctx, key); len(apiKey) > 0 && len(apiKey[0]) > 0 {
		return apiKey[0]
	}
	return ""
}

func (v *cohere) getApiKey(ctx context.Context) (string, error) {
	if apiKey := v.getValueFromContext(ctx, "X-Cohere-Api-Key"); apiKey != "" {
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
	ChatHistory   []message `json:"chat_history,omitempty"`
	Message       string    `json:"message"`
	Model         string    `json:"model"`
	MaxTokens     int       `json:"max_tokens"`
	Temperature   int       `json:"temperature"`
	K             int       `json:"k"`
	StopSequences []string  `json:"stop_sequences"`
}

type message struct {
	Role    string `json:"role"`
	Message string `json:"message"`
}

type generateResponse struct {
	Text string `json:"text"`
	// When an error occurs then the error message object is being returned with an error message
	// https://docs.cohere.com/reference/errors
	Message string `json:"message"`
}
