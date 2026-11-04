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
				"model":   "custom-model",
				"baseURL": "http://base-url.com",
			}},
			wantModel:   "custom-model",
			wantBaseURL: "http://base-url.com",
		},
		{
			name:    "empty model",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": ""}},
			wantErr: errors.New("model must be a non-empty string"),
		},
		{
			name:    "model of another type",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": 42}},
			wantErr: errors.New("model must be a string, got int"),
		},
		{
			name:    "empty base URL",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": ""}},
			wantErr: errors.New("baseURL must be a non-empty string"),
		},
		{
			name:    "relative base URL",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "api.openai.com"}},
			wantErr: errors.New(`baseURL must be an absolute http or https URL, got "api.openai.com"`),
		},
		{
			name:    "ftp base URL",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "ftp://api.openai.com"}},
			wantErr: errors.New(`baseURL must be an absolute http or https URL, got "ftp://api.openai.com"`),
		},
		{
			name:    "base URL with a port and no host",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "https://:443"}},
			wantErr: errors.New(`baseURL must be an absolute http or https URL, got "https://:443"`),
		},
		{
			name:        "base URL with a path",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://gateway.example/openai"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://gateway.example/openai",
		},
		{
			name:        "base URL with /v1",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://api.openai.com/v1"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://api.openai.com",
		},
		{
			name:        "base URL with /v1/",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://api.openai.com/v1/"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://api.openai.com",
		},
		{
			name:        "base URL with a path ending in /v1",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://gateway.example/openai/v1"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://gateway.example/openai",
		},
		{
			name:        "base URL with /v1 inside the path",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://gateway.example/v1/openai"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://gateway.example/v1/openai",
		},
		{
			name:        "base URL with an encoded slash before /v1",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://gateway.example/a%2Fb/v1"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://gateway.example/a%2Fb",
		},
		{
			name:        "base URL with an encoded slash as the last /v1",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://gateway.example/a%2Fv1"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://gateway.example/a%2Fv1",
		},
		{
			name:        "base URL whose host is v1",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://v1"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://v1",
		},
		{
			name:        "base URL with /v1 and a query",
			cfg:         fakeClassConfig{classConfig: map[string]any{"baseURL": "https://gateway.example/proxy/v1?x=1"}},
			wantModel:   "gpt-6-luna",
			wantBaseURL: "https://gateway.example/proxy?x=1",
		},
		{
			name:    "base URL of another type",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": 42}},
			wantErr: errors.New("baseURL must be a string, got int"),
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
