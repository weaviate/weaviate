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
	"math"
	"testing"

	// "github.com/pkg/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_classSettings_Validate(t *testing.T) {
	maxTokens42 := 42

	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantMaxTokens   *int
		wantTemperature float64
		wantTopP        float64
		wantTopK        int
		wantEndpoint    string
		wantErr         error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantMaxTokens:   nil,
			wantTemperature: 1.0,
			wantTopP:        1,
			wantTopK:        math.MaxInt64,
			wantEndpoint:    "",
			wantErr:         nil,
		},
		{
			name: "Happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint": "https://foo.bar.com",
				},
			},
			wantMaxTokens:   nil,
			wantTemperature: 1.0,
			wantTopP:        1,
			wantTopK:        math.MaxInt64,
			wantEndpoint:    "https://foo.bar.com",
			wantErr:         nil,
		},
		{
			name: "Everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint":    "https://foo.bar.com",
					"maxTokens":   42,
					"temperature": 0.5,
					"topP":        3,
					"topK":        1,
				},
			},
			wantMaxTokens:   &maxTokens42,
			wantTemperature: 0.5,
			wantTopP:        3,
			wantTopK:        1,
			wantEndpoint:    "https://foo.bar.com",
			wantErr:         nil,
		},
		{
			name: "Wrong maxTokens configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint":  "https://foo.bar.com",
					"maxTokens": true,
				},
			},
			wantErr: errors.Errorf("Wrong maxTokens configuration, values should be greater than zero or nil"),
		},
		{
			name: "Wrong temperature configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint":    "https://foo.bar.com",
					"temperature": true,
				},
			},
			wantErr: errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0"),
		},
		{
			name: "Wrong topP configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint": "https://foo.bar.com",
					"topP":     true,
				},
			},
			wantErr: errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5"),
		},
		{
			name: "Wrong topK configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint": "https://foo.bar.com",
					"topK":     true,
				},
			},
			wantErr: errors.Errorf("Wrong topK configuration, values should be greater than zero or nil"),
		},
		{
			name: "Empty endpoint",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint": "",
				},
			},
			wantErr: errors.Errorf("endpoint cannot be empty"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Error(t, tt.wantErr, ic.Validate(nil))
			} else {
				assert.NoError(t, ic.Validate(nil))
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantTopP, ic.TopP())
				assert.Equal(t, tt.wantTopK, ic.TopK())
				assert.Equal(t, tt.wantEndpoint, ic.Endpoint())
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
