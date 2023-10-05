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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-alephalpha/config"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

// Response represents the response from the Aleph Alpha /complete endpoint.
type Response struct {
    Completions []Completion `json:"completions"`
    ModelVersion string `json:"model_version"`
}

type Completion struct {
    Text string `json:"completion"`
    FinishReason string `json:"finish_reason"`
}

type Request struct {
    URL           string
    Model         string
    Prompt        string
    MaximumTokens int
    APIKey        string
}

type alephalpha struct {
	alephAlphaAPIKey string
	httpClient	  *http.Client
	logger 	  logrus.FieldLogger
}

func New(alephAlpaAPIKey string, logger logrus.FieldLogger) *alephalpha {
	return &alephalpha{
		alephAlphaAPIKey: alephAlpaAPIKey,
		httpClient:	  &http.Client{},
		logger: 	  logger,
	}
}

func (a *alephalpha) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)  {
	forPrompt, err := generateTextForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return a.Generate(ctx, cfg, forPrompt)
}

func (a *alephalpha) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	return a.Generate(ctx, cfg, task)
}

func (a *alephalpha) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	settings := config.NewClassSettings(cfg)

	input := Request{
		URL:           "https://api.aleph-alpha.com/complete",
		Model:         settings.Model(),
		Prompt:        prompt,
		MaximumTokens: 64,
		APIKey: 	  a.alephAlphaAPIKey,
	}

	completions, err := a.makeRequest(input)
    if err != nil {
        return nil, err
    }

	return &generativemodels.GenerateResponse{
		Result: &completions[0].Text,
	}, nil
}

func (a *alephalpha) makeRequest(input Request) ([] Completion, error) {
	method := "POST"
	payload := strings.NewReader(fmt.Sprintf(`{
		"model": "%s",
		"prompt": "%s",
		"maximum_tokens": %d
	}`, input.Model, input.Prompt, input.MaximumTokens))

	req, err := http.NewRequest(method, input.URL, payload)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", a.alephAlphaAPIKey))

	res, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.Errorf("unexpected status code: %d, body: %s", res.StatusCode, body)
	}

	var response Response
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		return nil, err
	}

	return response.Completions, nil
}
// generateTextForPrompt generates text for a given prompt by replacing placeholders in the prompt with values from a map of text properties.
//
// The function takes two arguments:
//
// - textProperties: a map of properties that can be used to replace placeholders in the prompt. The keys of the map are the names of the properties, and the values are the values of the properties.
//
// - prompt: a string that represents the prompt with property placeholders that should be replaced with actual values.
//
// The function uses a regular expression to find all placeholders in the prompt and replaces them with the corresponding values from the textProperties map.
// If a placeholder is found in the prompt that does not have a corresponding value in the textProperties map, the function returns an error.
//
// The function returns the generated text and an error, if any.
func generateTextForPrompt(textProperties map[string]string, prompt string) (string, error) {
    propertyPlaceholderRegex := regexp.MustCompile(`{([\w\s]*?)}`)
    propertyPlaceholders := propertyPlaceholderRegex.FindAllString(prompt, -1)
    for _, propertyPlaceholder := range propertyPlaceholders {
        propertyName := strings.Trim(propertyPlaceholder, "{}")
        propertyValue := textProperties[propertyName]
        if propertyValue == "" {
            return "", errors.Errorf("The following property has an empty value: '%v'. Make sure you spell the property name correctly, verify that the property exists and has a value", propertyName)
        }
        prompt = strings.ReplaceAll(prompt, propertyPlaceholder, propertyValue)
    }
    return prompt, nil
}