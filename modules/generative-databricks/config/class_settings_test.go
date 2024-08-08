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
		wantApiVersion       string
	}{
		{
			name: "Happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl": "https://foo.bar.com",
				},
			},
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        4097,
			wantTemperature:      0.0,
			wantTopP:             1,
			wantFrequencyPenalty: 0.0,
			wantPresencePenalty:  0.0,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-02-01",
		},
		{
			name: "Everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":       "https://foo.bar.com",
					"model":            "gpt-3.5-turbo",
					"maxTokens":        4097,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        4097,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-02-01",
		},
		{
			name: "OpenAI Proxy",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":       "https://foo.bar.com",
					"model":            "gpt-3.5-turbo",
					"maxTokens":        4097,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
					"baseURL":          "https://proxy.weaviate.dev/",
				},
			},
			wantBaseURL:          "https://proxy.weaviate.dev/",
			wantApiVersion:       "2024-02-01",
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        4097,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
		},
		{
			name: "Legacy config",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":       "https://foo.bar.com",
					"model":            "text-davinci-003",
					"maxTokens":        1200,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantModel:            "text-davinci-003",
			wantMaxTokens:        1200,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-02-01",
		},
		{
			name: "Azure OpenAI config",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":       "https://foo.bar.com",
					"resourceName":     "weaviate",
					"deploymentId":     "gpt-3.5-turbo",
					"maxTokens":        4097,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantResourceName:     "weaviate",
			wantDeploymentID:     "gpt-3.5-turbo",
			wantIsAzure:          true,
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        4097,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-02-01",
		},
		{
			name: "Azure OpenAI config with baseURL",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":       "https://foo.bar.com",
					"baseURL":          "some-base-url",
					"resourceName":     "weaviate",
					"deploymentId":     "gpt-3.5-turbo",
					"maxTokens":        4097,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantResourceName:     "weaviate",
			wantDeploymentID:     "gpt-3.5-turbo",
			wantIsAzure:          true,
			wantModel:            "gpt-3.5-turbo",
			wantMaxTokens:        4097,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "some-base-url",
			wantApiVersion:       "2024-02-01",
		},
		{
			name: "With gpt-3.5-turbo-16k model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":       "https://foo.bar.com",
					"model":            "gpt-3.5-turbo-16k",
					"maxTokens":        4097,
					"temperature":      0.5,
					"topP":             3,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantModel:            "gpt-3.5-turbo-16k",
			wantMaxTokens:        4097,
			wantTemperature:      0.5,
			wantTopP:             3,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.openai.com",
			wantApiVersion:       "2024-02-01",
		},
		{
			name: "Wrong maxTokens configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl": "https://foo.bar.com",
					"maxTokens":  true,
				},
			},
			wantErr: errors.Errorf("Wrong maxTokens configuration, values should be greater than zero or nil"),
		},
		{
			name: "Wrong temperature configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl":  "https://foo.bar.com",
					"temperature": true,
				},
			},
			wantErr: errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong topP configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl": "https://foo.bar.com",
					"topP":       true,
				},
			},
			wantErr: errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5"),
		},
		{
			name: "Empty servingURL",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"servingUrl": "",
				},
			},
			wantErr: errors.Errorf("servingUrl cannot be empty"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, tt.wantErr, ic.Validate(nil).Error())
			} else {
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantTopP, ic.TopP())
				assert.Equal(t, tt.wantResourceName, ic.ResourceName())
				assert.Equal(t, tt.wantDeploymentID, ic.DeploymentID())
				assert.Equal(t, tt.wantIsAzure, ic.IsAzure())

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

func (f fakeClassConfig) TargetVector() string {
	return ""
}
