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
	"regexp"
	"strings"
	"time"

	"github.com/weaviate/weaviate/modules/qna-openai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

type ollama struct {
	model      string
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, model string, timeout time.Duration, logger logrus.FieldLogger) *ollama {
	return &ollama{
		origin: origin,
		model:  model,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *ollama) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *ollama) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *ollama) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

	// Marcin! Please take a look at this part!
	ollamaUrl := v.getOllamaUrl(ctx, settings.BaseURL())
	input := generateInput{
		Model:  settings.Model(),
		Prompt: prompt,
		Stream: false,
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
		return nil, errors.Wrap(err, "unmarshal response body")
	}

	if res.StatusCode != 200 || resBody.Error != nil {
		if resBody.Error != nil {
			return nil, errors.Errorf("connection to Ollama API failed with status: %d error: %v", res.StatusCode, resBody.Error.Message)
		}
		return nil, errors.Errorf("connection to Ollama API failed with status: %d", res.StatusCode)
	}

	textResponse := resBody.Response

	return &generativemodels.GenerateResponse{
		Result: &textResponse,
	}, nil
}

func (v *ollama) getOllamaUrl(ctx context.Context, baseURL string) string {
	passedBaseURL := baseURL
	if headerBaseURL := v.getValueFromContext(ctx, "X-Ollama-Baseurl"); headerBaseURL != "" {
		passedBaseURL = headerBaseURL
	}
	return fmt.Sprintf("%s/api/generate", passedBaseURL)
}

func (v *ollama) getOllamaModel(ctx context.Context) string {
	model := v.getValueFromContext(ctx, "X-Ollama-Model")
	return model
}

func (v *ollama) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *ollama) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *ollama) getValueFromContext(ctx context.Context, key string) string {
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

type generateInput struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

// The entire response for an error ends up looking different, may want to add omitempty everywhere.
type generateResponse struct {
	Model              string          `json:"model"`
	CreatedAt          string          `json:"created_at"`
	Response           string          `json:"response"`
	Done               bool            `json:"done"`
	Context            []int           `json:"context"`
	TotalDuration      int             `json:"total_duration"`
	LoadDuration       int             `json:"load_duration"`
	PromptEvalDuration int             `json:"prompt_eval_duration"`
	EvalCount          int             `json:"eval_count"`
	EvalDuration       int             `json:"eval_duration"`
	Error              *ollamaApiError `json:"error,omitempty"`
}

type ollamaApiError struct {
	Message string `json:"error"`
}
