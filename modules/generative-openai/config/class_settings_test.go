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

package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name                 string
		cfg                  moduletools.ClassConfig
		wantModel            string
		wantMaxTokens        *float64
		wantTemperature      *float64
		wantTopP             float64
		wantFrequencyPenalty float64
		wantPresencePenalty  float64
		wantResourceName     string
		wantDeploymentID     string
		wantIsAzure          bool
		wantErr              error
		wantBaseURL          string
		wantApiVersion       string
		wantReasoningEffort  *string
		wantVerbosity        *string
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]any{},
			},
			wantModel:            "gpt-5-mini",
			wantMaxTokens:        nil,
			wantTemperature:      nil,
			wantTopP:             1,
			wantFrequencyPenalty: 0.0,
			wantPresencePenalty:  0.0,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-06-01",
			wantReasoningEffort:  nil,
			wantVerbosity:        nil,
		},
		{
			name: "Everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":            "gpt-3.5-turbo",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
					"reasoningEffort":  "high",
					"verbosity":        "medium",
				},
			},
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-06-01",
			wantReasoningEffort:  ptrValue("high"),
			wantVerbosity:        ptrValue("medium"),
		},
		{
			name: "OpenAI Proxy",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":            "gpt-3.5-turbo",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
					"baseURL":          "https://proxy.weaviate.dev/",
				},
			},
			wantBaseURL:          "https://proxy.weaviate.dev/",
			wantApiVersion:       "2024-06-01",
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
		},
		{
			name: "Legacy config",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":            "text-davinci-003",
					"maxTokens":        1200,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantModel:            "text-davinci-003",
			wantMaxTokens:        ptrValue[float64](1200),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-06-01",
		},
		{
			name: "Azure OpenAI config",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"isAzure":          true,
					"resourceName":     "weaviate",
					"deploymentId":     "gpt-3.5-turbo",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantResourceName:     "weaviate",
			wantDeploymentID:     "gpt-3.5-turbo",
			wantIsAzure:          true,
			wantModel:            "gpt-5-mini",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-06-01",
		},
		{
			name: "Azure OpenAI config with baseURL",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"isAzure":          true,
					"baseURL":          "some-base-url",
					"resourceName":     "weaviate",
					"deploymentId":     "gpt-3.5-turbo",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantResourceName:     "weaviate",
			wantDeploymentID:     "gpt-3.5-turbo",
			wantIsAzure:          true,
			wantModel:            "gpt-5-mini",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "some-base-url",
			wantApiVersion:       "2024-06-01",
		},
		{
			name: "With gpt-3.5-turbo-16k model",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":            "gpt-3.5-turbo-16k",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantModel:            "gpt-3.5-turbo-16k",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-06-01",
		},
		{
			name: "With verbosity and reasoning effort",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":            "gpt-3.5-turbo-16k",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
					"reasoningEffort":  "minimal",
					"verbosity":        "high",
				},
			},
			wantModel:            "gpt-3.5-turbo-16k",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-06-01",
			wantReasoningEffort:  ptrValue("minimal"),
			wantVerbosity:        ptrValue("high"),
		},
		{
			name: "Wrong verbosity",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"verbosity": "not-allowed-value",
				},
			},
			wantErr: fmt.Errorf("wrong verbosity value, allowed values are: [low medium high]"),
		},
		{
			name: "Wrong reasoning effort",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"reasoningEffort": "not-allowed-value",
				},
			},
			wantErr: fmt.Errorf("wrong reasoningEffort value, allowed values are: [minimal low medium high]"),
		},
		{
			name: "Wrong maxTokens configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"maxTokens": true,
				},
			},
			wantErr: fmt.Errorf("Wrong maxTokens configuration, values are should have a minimal value of 1 and max is dependant on the model used"),
		},
		{
			name: "Wrong temperature configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"temperature": true,
				},
			},
			wantErr: fmt.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong frequencyPenalty configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"frequencyPenalty": true,
				},
			},
			wantErr: fmt.Errorf("Wrong frequencyPenalty configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong presencePenalty configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"presencePenalty": true,
				},
			},
			wantErr: fmt.Errorf("Wrong presencePenalty configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong topP configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"topP": true,
				},
			},
			wantErr: fmt.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5"),
		},
		{
			name: "Wrong Azure config - empty deploymentId",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"resourceName": "resource-name",
					"isAzure":      true,
				},
			},
			wantErr: fmt.Errorf("both resourceName and deploymentId must be provided"),
		},
		{
			name: "Wrong Azure config - empty resourceName",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"deploymentId": "deployment-name",
					"isAzure":      true,
				},
			},
			wantErr: fmt.Errorf("both resourceName and deploymentId must be provided"),
		},
		{
			name: "Wrong Azure config - wrong api version",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"apiVersion": "wrong-api-version",
				},
			},
			wantErr: fmt.Errorf("wrong Azure OpenAI apiVersion, available api versions are: " +
				"[2022-12-01 2023-03-15-preview 2023-05-15 2023-06-01-preview 2023-07-01-preview 2023-08-01-preview 2023-09-01-preview 2023-12-01-preview 2024-02-15-preview 2024-03-01-preview 2024-02-01 2024-06-01]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, tt.wantErr, ic.Validate(nil).Error())
			} else {
				assert.NoError(t, ic.Validate(nil))
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantTopP, ic.TopP())
				assert.Equal(t, tt.wantFrequencyPenalty, ic.FrequencyPenalty())
				assert.Equal(t, tt.wantPresencePenalty, ic.PresencePenalty())
				assert.Equal(t, tt.wantResourceName, ic.ResourceName())
				assert.Equal(t, tt.wantDeploymentID, ic.DeploymentID())
				assert.Equal(t, tt.wantIsAzure, ic.IsAzure())
				assert.Equal(t, tt.wantBaseURL, ic.BaseURL())
				assert.Equal(t, tt.wantApiVersion, ic.ApiVersion())
				assert.Equal(t, tt.wantReasoningEffort, ic.ReasoningEffort())
				assert.Equal(t, tt.wantVerbosity, ic.Verbosity())
			}
		})
	}
}

type fakeClassConfig struct {
	classConfig map[string]any
}

func (f fakeClassConfig) Class() map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	return nil
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func ptrValue[T float64 | string | int](in T) *T {
	return &in
}
