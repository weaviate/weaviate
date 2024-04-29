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
		name            string
		cfg             moduletools.ClassConfig
		wantModel       string
		wantMaxTokens   int
		wantTemperature int
		wantBaseURL     string
		wantErr         error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:       "mistral-7b-instruct",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://text.octoai.run",
			wantErr:         nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":       "mixtral-8x7b-instruct",
					"maxTokens":   1024,
					"temperature": 1,
				},
			},
			wantModel:       "mixtral-8x7b-instruct",
			wantMaxTokens:   1024,
			wantTemperature: 1,
			wantBaseURL:     "https://text.octoai.run",
			wantErr:         nil,
		},
		{
			name: "wrong model configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong OctoAI model name, available model names are: " +
				"[qwen1.5-32b-chat meta-llama-3-8b-instruct meta-llama-3-70b-instruct mixtral-8x22b-instruct nous-hermes-2-mixtral-8x7b-dpo mixtral-8x7b-instruct mixtral-8x22b-finetuned hermes-2-pro-mistral-7b mistral-7b-instruct codellama-7b-instruct codellama-13b-instruct codellama-34b-instruct llama-2-13b-chat llama-2-70b-chat]"),
		},
		{
			name: "default settings with meta-llama-3-8b-instruct and baseURL",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":   "meta-llama-3-8b-instruct",
					"baseURL": "https://text.octoai.run",
				},
			},
			wantModel:       "meta-llama-3-8b-instruct",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://text.octoai.run",
			wantErr:         nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr.Error(), ic.Validate(nil).Error())
			} else {
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
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
