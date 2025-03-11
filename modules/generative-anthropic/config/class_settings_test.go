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
		name              string
		cfg               moduletools.ClassConfig
		wantModel         string
		wantMaxTokens     *int
		wantTemperature   float64
		wantTopK          int
		wantTopP          float64
		wantStopSequences []string
		wantBaseURL       string
		wantErr           error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:         "claude-3-5-sonnet-20240620",
			wantMaxTokens:     nil,
			wantTemperature:   1.0,
			wantTopK:          0,
			wantTopP:          0.0,
			wantStopSequences: []string{},
			wantBaseURL:       "https://api.anthropic.com",
			wantErr:           nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":         "claude-3-opus-20240229",
					"maxTokens":     3000,
					"temperature":   0.7,
					"topK":          5,
					"topP":          0.9,
					"stopSequences": []string{"stop1", "stop2"},
					"baseURL":       "https://custom.anthropic.api",
				},
			},
			wantModel:         "claude-3-opus-20240229",
			wantMaxTokens:     ptrInt(3000),
			wantTemperature:   0.7,
			wantTopK:          5,
			wantTopP:          0.9,
			wantStopSequences: []string{"stop1", "stop2"},
			wantBaseURL:       "https://custom.anthropic.api",
			wantErr:           nil,
		},
		{
			name: "new model name configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "some-new-model-name",
				},
			},
			wantModel:         "some-new-model-name",
			wantMaxTokens:     nil,
			wantTemperature:   1.0,
			wantTopK:          0,
			wantTopP:          0.0,
			wantStopSequences: []string{},
			wantBaseURL:       "https://api.anthropic.com",
			wantErr:           nil,
		},
		{
			name: "default settings with claude-3-haiku-20240307",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "claude-3-haiku-20240307",
				},
			},
			wantModel:         "claude-3-haiku-20240307",
			wantMaxTokens:     nil,
			wantTemperature:   1.0,
			wantTopK:          0,
			wantTopP:          0.0,
			wantStopSequences: []string{},
			wantBaseURL:       "https://api.anthropic.com",
			wantErr:           nil,
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
				assert.Equal(t, tt.wantTopK, ic.TopK())
				assert.Equal(t, tt.wantTopP, ic.TopP())
				assert.Equal(t, tt.wantStopSequences, ic.StopSequences())
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

func ptrInt(in int) *int {
	return &in
}
