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
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-palm/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func buildURL(apiEndoint, projectID, modelID string) string {
	urlTemplate := "https://%s/v1/projects/%s/locations/us-central1/publishers/google/models/%s:predict"
	return fmt.Sprintf(urlTemplate, apiEndoint, projectID, modelID)
}

type palm struct {
	apiKey     string
	buildUrlFn func(apiEndoint, projectID, modelID string) string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, logger logrus.FieldLogger) *palm {
	return &palm{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		buildUrlFn: buildURL,
		logger:     logger,
	}
}

func (v *palm) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt)
}

func (v *palm) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask)
}

func (v *palm) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

	modelID := settings.ModelID()
	if settings.EndpointID() != "" {
		modelID = settings.EndpointID()
	}

	endpointURL := v.buildUrlFn(settings.ApiEndpoint(), settings.ProjectID(), modelID)

	input := generateInput{
		Instances: []instance{
			{
				Messages: []message{
					{
						Content: prompt,
					},
				},
			},
		},
		Parameters: parameters{
			Temperature:     settings.Temperature(),
			MaxOutputTokens: settings.TokenLimit(),
			TopP:            settings.TopP(),
			TopK:            settings.TopK(),
		},
	}
	body, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "marshal body")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpointURL,
		bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	apiKey, err := v.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "PaLM API Key")
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
			return nil, fmt.Errorf("connection to Google PaLM failed with status: %v error: %v",
				res.StatusCode, resBody.Error.Message)
		}
		return nil, fmt.Errorf("connection to Google PaLM failed with status: %d", res.StatusCode)
	}

	if len(resBody.Predictions) > 0 && len(resBody.Predictions[0].Candidates) > 0 {
		content := resBody.Predictions[0].Candidates[0].Content
		if content != "" {
			trimmedResponse := strings.Trim(content, "\n")
			return &generativemodels.GenerateResponse{
				Result: &trimmedResponse,
			}, nil
		}
	}

	return &generativemodels.GenerateResponse{
		Result: nil,
	}, nil
}

func (v *palm) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *palm) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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

func (v *palm) getApiKey(ctx context.Context) (string, error) {
	if len(v.apiKey) > 0 {
		return v.apiKey, nil
	}
	apiKey := ctx.Value("X-Palm-Api-Key")
	if apiKeyHeader, ok := apiKey.([]string); ok &&
		len(apiKeyHeader) > 0 && len(apiKeyHeader[0]) > 0 {
		return apiKeyHeader[0], nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Palm-Api-Key " +
		"nor in environment variable under PALM_APIKEY")
}

type generateInput struct {
	Instances  []instance `json:"instances,omitempty"`
	Parameters parameters `json:"parameters"`
}

type instance struct {
	Context  string    `json:"context,omitempty"`
	Messages []message `json:"messages,omitempty"`
	Examples []example `json:"examples,omitempty"`
}

type message struct {
	Author  string `json:"author"`
	Content string `json:"content"`
}

type example struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

type parameters struct {
	Temperature     float64 `json:"temperature"`
	MaxOutputTokens int     `json:"maxOutputTokens"`
	TopP            float64 `json:"topP"`
	TopK            int     `json:"topK"`
}

type generateResponse struct {
	Predictions      []prediction  `json:"predictions,omitempty"`
	Error            *palmApiError `json:"error,omitempty"`
	DeployedModelId  string        `json:"deployedModelId,omitempty"`
	Model            string        `json:"model,omitempty"`
	ModelDisplayName string        `json:"modelDisplayName,omitempty"`
	ModelVersionId   string        `json:"modelVersionId,omitempty"`
}

type prediction struct {
	Candidates       []candidate         `json:"candidates,omitempty"`
	SafetyAttributes *[]safetyAttributes `json:"safetyAttributes,omitempty"`
}

type candidate struct {
	Author  string `json:"author"`
	Content string `json:"content"`
}

type safetyAttributes struct {
	Scores     []float64 `json:"scores,omitempty"`
	Blocked    *bool     `json:"blocked,omitempty"`
	Categories []string  `json:"categories,omitempty"`
}

type palmApiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}
