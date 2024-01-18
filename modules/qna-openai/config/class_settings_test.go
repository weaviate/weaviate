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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name                 string
		cfg                  moduletools.ClassConfig
		wantModel            string
		wantMaxTokens        float64
		wantTemperature      float64
		wantTopP             float64
		wantFrequencyPenalty float64
		wantPresencePenalty  float64
		wantResourceName     string
		wantDeploymentID     string
		wantIsAzure          bool
		wantErr              error
		wantBaseURL          string
	}{
		{
			name: "Happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:            "text-ada-001",
			wantMaxTokens:        16,
			wantTemperature:      0.0,
			wantTopP:             1,
			wantFrequencyPenalty: 0.0,
			wantPresencePenalty:  0.0,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
		},
		{
			name: "Everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":            "text-babbage-001",
					"maxTokens":        100,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
					"baseURL":          "https://openai.proxy.dev",
				},
			},
			wantModel:            "text-babbage-001",
			wantMaxTokens:        100,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantBaseURL:          "https://openai.proxy.dev",
			wantErr:              nil,
		},
		{
			name: "Azure OpenAI config",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"resourceName": "weaviate",
					"deploymentId": "text-ada-001",
				},
			},
			wantModel:            "text-ada-001",
			wantResourceName:     "weaviate",
			wantDeploymentID:     "text-ada-001",
			wantIsAzure:          true,
			wantMaxTokens:        16,
			wantTemperature:      0.0,
			wantTopP:             1,
			wantFrequencyPenalty: 0.0,
			wantPresencePenalty:  0.0,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
		},
		{
			name: "Wrong model data type configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": true,
				},
			},
			wantErr: errors.Errorf("wrong OpenAI model name, available model names are: %v", availableOpenAIModels),
		},
		{
			name: "Wrong model data type configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "this-is-a-non-existing-model",
				},
			},
			wantErr: errors.Errorf("wrong OpenAI model name, available model names are: %v", availableOpenAIModels),
		},
		{
			name: "Wrong maxTokens configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"maxTokens": true,
				},
			},
			wantErr: errors.Errorf("Wrong maxTokens configuration, values are should have a minimal value of 1 and max is dependant on the model used"),
		},
		{
			name: "Wrong temperature configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"temperature": true,
				},
			},
			wantErr: errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong frequencyPenalty configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"frequencyPenalty": true,
				},
			},
			wantErr: errors.Errorf("Wrong frequencyPenalty configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong presencePenalty configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"presencePenalty": true,
				},
			},
			wantErr: errors.Errorf("Wrong presencePenalty configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong topP configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"topP": true,
				},
			},
			wantErr: errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5"),
		},
		{
			name: "Wrong Azure OpenAI config - empty deploymentId",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"resourceName": "resource-name",
				},
			},
			wantErr: errors.Errorf("both resourceName and deploymentId must be provided"),
		},
		{
			name: "Wrong Azure OpenAI config - empty resourceName",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"deploymentId": "ada",
				},
			},
			wantErr: errors.Errorf("both resourceName and deploymentId must be provided"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, tt.wantErr, ic.Validate(nil).Error())
			} else {
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
			}
		})
	}
}

type fakeClassConfig struct {
	classConfig map[string]interface{}
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}
