//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
		wantErr              error
		wantBaseURL          string
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]any{},
			},
			wantModel:            "deepseek-chat",
			wantMaxTokens:        nil,
			wantTemperature:      nil,
			wantTopP:             1.0,
			wantFrequencyPenalty: 0.0,
			wantPresencePenalty:  0.0,
			wantErr:              nil,
			wantBaseURL:          "https://api.deepseek.com",
		},
		{
			name: "Everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":            "deepseek-reasoner",
					"maxTokens":        4096,
					"temperature":      0.5,
					"topP":             0.9,
					"frequencyPenalty": 0.1,
					"presencePenalty":  0.9,
				},
			},
			wantModel:            "deepseek-reasoner",
			wantMaxTokens:        ptrValue[float64](4096),
			wantTemperature:      ptrValue(0.5),
			wantTopP:             0.9,
			wantFrequencyPenalty: 0.1,
			wantPresencePenalty:  0.9,
			wantErr:              nil,
			wantBaseURL:          "https://api.deepseek.com",
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
			wantErr: fmt.Errorf("Wrong temperature configuration, values are between 0.0 and 2.0"),
		},
		{
			name: "Wrong frequencyPenalty configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"frequencyPenalty": true,
				},
			},
			wantErr: fmt.Errorf("Wrong frequencyPenalty configuration, values are between -2.0 and 2.0"),
		},
		{
			name: "Wrong presencePenalty configured",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"presencePenalty": true,
				},
			},
			wantErr: fmt.Errorf("Wrong presencePenalty configuration, values are between -2.0 and 2.0"),
		},
		{
			name: "Wrong topP configured (too high)",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"topP": 2.0,
				},
			},
			wantErr: fmt.Errorf("Wrong topP configuration, values are should have a minimal value of 0 and max of 1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				// Assert error message
				err := ic.Validate(nil)
				assert.Error(t, err)
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, ic.Validate(nil))
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantTopP, ic.TopP())
				assert.Equal(t, tt.wantFrequencyPenalty, ic.FrequencyPenalty())
				assert.Equal(t, tt.wantPresencePenalty, ic.PresencePenalty())
				assert.Equal(t, tt.wantBaseURL, ic.BaseURL())
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
