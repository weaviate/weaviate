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
			wantModel:       "meta-llama-3.1-70b-instruct",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://inference.friendli.ai",
			wantErr:         nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":       "meta-llama-3.1-8b-instruct",
					"maxTokens":   1024,
					"temperature": 1,
				},
			},
			wantModel:       "meta-llama-3.1-8b-instruct",
			wantMaxTokens:   1024,
			wantTemperature: 1,
			wantBaseURL:     "https://inference.friendli.ai",
			wantErr:         nil,
		},
		{
			name: "newly supported serverless model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "new-serverless-model",
				},
			},
			wantModel:       "new-serverless-model",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://inference.friendli.ai",
			wantErr:         nil,
		},
		{
			name: "custom dedicated model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":              "my-dedicated-model",
					"X-Friendli-Baseurl": "https://inference.friendli.ai/dedicated",
				},
			},
			wantModel:       "my-dedicated-model",
			wantMaxTokens:   2048,
			wantTemperature: 0,
			wantBaseURL:     "https://inference.friendli.ai/dedicated",
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
