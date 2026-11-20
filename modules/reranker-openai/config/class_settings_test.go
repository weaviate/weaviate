//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         moduletools.ClassConfig
		wantModel   string
		wantBaseURL string
		wantErr     error
	}{
		{
			name:        "default settings",
			cfg:         fakeClassConfig{classConfig: map[string]any{}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://api.openai.com",
		},
		{
			name: "custom model and base URL",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"model":   "gpt-6-custom",
				"baseURL": "http://base-url.com",
			}},
			wantModel:   "gpt-6-custom",
			wantBaseURL: "http://base-url.com",
		},
		{
			name:    "empty model",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": ""}},
			wantErr: errors.New("no model provided"),
		},
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: errors.New("empty config"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewClassSettings(tt.cfg)
			err := cs.Validate(nil)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantModel, cs.Model())
			assert.Equal(t, tt.wantBaseURL, cs.BaseURL())
		})
	}
}

func Test_classSettings_ValidateBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{name: "valid HTTPS URL", baseURL: "https://api.openai.com"},
		{name: "HTTP URL is rejected", baseURL: "http://api.example.com", wantErr: true},
		{name: "loopback address is rejected", baseURL: "https://127.0.0.1", wantErr: true},
		{name: "localhost is rejected", baseURL: "https://localhost", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewClassSettings(fakeClassConfig{classConfig: map[string]any{"baseURL": tt.baseURL}})
			err := cs.Validate(nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
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
