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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantModel       string
		wantTemperature int
		wantBaseURL     string
		wantErr         error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:       "meta-llama/Llama-2-70b-chat-hf",
			wantTemperature: 0,
			wantBaseURL:     "https://api.endpoints.anyscale.com",
			wantErr:         nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":       "meta-llama/Llama-2-70b-chat-hf",
					"temperature": 1,
				},
			},
			wantModel:       "meta-llama/Llama-2-70b-chat-hf",
			wantTemperature: 1,
			wantBaseURL:     "https://api.endpoints.anyscale.com",
			wantErr:         nil,
		},
		{
			name: "everything non default configured and base url",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":       "meta-llama/Llama-2-70b-chat-hf",
					"temperature": 1,
					"baseURL":     "https://custom.endpoint.com",
				},
			},
			wantModel:       "meta-llama/Llama-2-70b-chat-hf",
			wantTemperature: 1,
			wantBaseURL:     "https://custom.endpoint.com",
			wantErr:         nil,
		},
		{
			name: "unsupported model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":       "unsupported",
					"temperature": 1,
					"baseURL":     "https://custom.endpoint.com",
				},
			},
			wantErr: errors.New("wrong Anyscale model name, available model names are: [meta-llama/Llama-2-70b-chat-hf meta-llama/Llama-2-13b-chat-hf meta-llama/Llama-2-7b-chat-hf codellama/CodeLlama-34b-Instruct-hf mistralai/Mistral-7B-Instruct-v0.1 mistralai/Mixtral-8x7B-Instruct-v0.1]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr.Error(), ic.Validate(nil).Error())
			} else {
				assert.Equal(t, tt.wantBaseURL, ic.BaseURL())
				assert.Equal(t, tt.wantModel, ic.Model())
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
