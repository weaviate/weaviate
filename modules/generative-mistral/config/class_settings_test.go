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
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantModel       string
		wantMaxTokens   int
		wantTemperature float64
		wantBaseURL     string
		wantErr         error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:       "open-mistral-7b",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://api.mistral.ai",
			wantErr:         nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":       "mistral-medium",
					"maxTokens":   50,
					"temperature": 1,
				},
			},
			wantModel:       "mistral-medium",
			wantMaxTokens:   50,
			wantTemperature: 1,
			wantBaseURL:     "https://api.mistral.ai",
			wantErr:         nil,
		},
		{
			name: "wrong model configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "wrong-model",
				},
			},
			wantErr: fmt.Errorf("wrong Mistral model name, available model names are: " +
				"[open-mistral-7b mistral-tiny-2312 mistral-tiny open-mixtral-8x7b mistral-small-2312 mistral-small mistral-small-2402 mistral-small-latest mistral-medium-latest mistral-medium-2312 mistral-medium mistral-large-latest mistral-large-2402]"),
		},
		{
			name: "default settings with open-mistral-7b",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "open-mistral-7b",
				},
			},
			wantModel:       "open-mistral-7b",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://api.mistral.ai",
			wantErr:         nil,
		},
		{
			name: "default settings with mistral-medium and baseURL",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":   "mistral-medium",
					"baseURL": "http://custom-url.com",
				},
			},
			wantModel:       "mistral-medium",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "http://custom-url.com",
			wantErr:         nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr.Error(), ic.Validate(nil).Error())
			} else {
				assert.NoError(t, ic.Validate(nil))
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
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

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
