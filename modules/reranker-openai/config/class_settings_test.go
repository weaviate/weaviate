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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_classSettings_Validate(t *testing.T) {
	const (
		wholeErr   = "maxDocuments must be a whole number between 1 and 1000, got "
		baseURLErr = "baseURL must be a URL with an http or https scheme and a host, such as https://api.openai.com, got "
	)
	tests := []struct {
		name             string
		cfg              moduletools.ClassConfig
		wantModel        string
		wantBaseURL      string
		wantMaxDocuments int
		wantErr          string
	}{
		{
			name:             "default settings",
			cfg:              fakeClassConfig{classConfig: map[string]any{}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 100,
		},
		{
			name: "custom settings",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"model":        "gpt-4o-mini",
				"baseURL":      "https://proxy.example.com",
				"maxDocuments": 25,
			}},
			wantModel:        "gpt-4o-mini",
			wantBaseURL:      "https://proxy.example.com",
			wantMaxDocuments: 25,
		},
		{
			name:             "statement question",
			cfg:              fakeClassConfig{classConfig: map[string]any{"question": "statement"}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 100,
		},
		{
			name:    "unknown question",
			cfg:     fakeClassConfig{classConfig: map[string]any{"question": "summary"}},
			wantErr: `question must be "relevance" or "statement", got "summary"`,
		},
		{
			name:             "empty baseURL is the default",
			cfg:              fakeClassConfig{classConfig: map[string]any{"baseURL": ""}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 100,
		},
		{
			name:             "maxDocuments lowest value",
			cfg:              fakeClassConfig{classConfig: map[string]any{"maxDocuments": 1}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 1,
		},
		{
			name:             "maxDocuments highest value",
			cfg:              fakeClassConfig{classConfig: map[string]any{"maxDocuments": 1000}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 1000,
		},
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: "empty config",
		},
		{
			name:    "empty model",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": ""}},
			wantErr: "no model provided",
		},
		{
			name:    "maxDocuments zero",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": 0}},
			wantErr: "maxDocuments must be between 1 and 1000, got 0",
		},
		{
			name:    "maxDocuments negative",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": -5}},
			wantErr: "maxDocuments must be between 1 and 1000, got -5",
		},
		{
			name:    "maxDocuments above the limit",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": 1001}},
			wantErr: "maxDocuments must be between 1 and 1000, got 1001",
		},
		{
			name:    "maxDocuments as text that is not a number",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": "many"}},
			wantErr: wholeErr + `"many"`,
		},
		{
			name:             "maxDocuments as text that is a whole number",
			cfg:              fakeClassConfig{classConfig: map[string]any{"maxDocuments": "3"}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 3,
		},
		{
			name:             "maxDocuments as json.Number",
			cfg:              fakeClassConfig{classConfig: map[string]any{"maxDocuments": json.Number("25")}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 25,
		},
		{
			name:             "maxDocuments as a whole float",
			cfg:              fakeClassConfig{classConfig: map[string]any{"maxDocuments": 25.0}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 25,
		},
		{
			name:             "maxDocuments as a whole json.Number in float notation",
			cfg:              fakeClassConfig{classConfig: map[string]any{"maxDocuments": json.Number("25.0")}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://api.openai.com",
			wantMaxDocuments: 25,
		},
		{
			name:    "maxDocuments as a float with a fraction",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": 2.5}},
			wantErr: wholeErr + "2.5",
		},
		{
			name:    "maxDocuments as a json.Number with a fraction",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": json.Number("2.5")}},
			wantErr: wholeErr + "2.5",
		},
		{
			name:    "maxDocuments as text with a fraction",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": "2.5"}},
			wantErr: wholeErr + `"2.5"`,
		},
		{
			name:    "maxDocuments as a float too large for a whole number",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": 1e300}},
			wantErr: wholeErr + "1e+300",
		},
		{
			name:    "maxDocuments as a bool",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": true}},
			wantErr: wholeErr + "true",
		},
		{
			name:    "maxDocuments as a list",
			cfg:     fakeClassConfig{classConfig: map[string]any{"maxDocuments": []any{json.Number("5")}}},
			wantErr: wholeErr + "[5]",
		},
		{
			name:    "model as a number",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": json.Number("4")}},
			wantErr: "model must be a string, got a number",
		},
		{
			name:    "model as a bool",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": true}},
			wantErr: "model must be a string, got a boolean",
		},
		{
			name:    "model as a list",
			cfg:     fakeClassConfig{classConfig: map[string]any{"model": []any{"gpt-4o-mini"}}},
			wantErr: "model must be a string, got a list",
		},
		{
			name:    "question as a number",
			cfg:     fakeClassConfig{classConfig: map[string]any{"question": 1}},
			wantErr: "question must be a string, got a number",
		},
		{
			name:    "question as a bool",
			cfg:     fakeClassConfig{classConfig: map[string]any{"question": false}},
			wantErr: "question must be a string, got a boolean",
		},
		{
			name:    "question as a list",
			cfg:     fakeClassConfig{classConfig: map[string]any{"question": []any{"statement"}}},
			wantErr: "question must be a string, got a list",
		},
		{
			name:    "baseURL as a number",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": json.Number("8080")}},
			wantErr: "baseURL must be a string, got a number",
		},
		{
			name:    "baseURL as a bool",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": true}},
			wantErr: "baseURL must be a string, got a boolean",
		},
		{
			name:    "baseURL as an object",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": map[string]any{"host": "x"}}},
			wantErr: "baseURL must be a string, got an object",
		},
		{
			name:             "baseURL ending in v1",
			cfg:              fakeClassConfig{classConfig: map[string]any{"baseURL": "https://proxy.example.com/v1"}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://proxy.example.com",
			wantMaxDocuments: 100,
		},
		{
			name:             "baseURL ending in v1 and a slash",
			cfg:              fakeClassConfig{classConfig: map[string]any{"baseURL": "https://proxy.example.com/openai/v1/"}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "https://proxy.example.com/openai",
			wantMaxDocuments: 100,
		},
		{
			name:             "baseURL with a port and plain http",
			cfg:              fakeClassConfig{classConfig: map[string]any{"baseURL": "http://localhost:8080/"}},
			wantModel:        "gpt-6-luna",
			wantBaseURL:      "http://localhost:8080",
			wantMaxDocuments: 100,
		},
		{
			name:    "baseURL without a scheme",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "api.openai.com/v1"}},
			wantErr: baseURLErr + `"api.openai.com/v1"`,
		},
		{
			name:    "baseURL with host and port but no scheme",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "localhost:8080"}},
			wantErr: baseURLErr + `"localhost:8080"`,
		},
		{
			name:    "baseURL without a host",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "https:///v1"}},
			wantErr: baseURLErr + `"https:///v1"`,
		},
		{
			name:    "baseURL with a scheme that is not http",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "ftp://api.openai.com"}},
			wantErr: baseURLErr + `"ftp://api.openai.com"`,
		},
		{
			name:    "baseURL that does not parse",
			cfg:     fakeClassConfig{classConfig: map[string]any{"baseURL": "https://api.openai.com/%zz"}},
			wantErr: baseURLErr + `"https://api.openai.com/%zz"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)

			err := ic.Validate(nil)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			settings, err := ic.Resolve()
			require.NoError(t, err)
			assert.Equal(t, tt.wantModel, settings.Model)
			assert.Equal(t, tt.wantBaseURL, settings.BaseURL)
			assert.Equal(t, tt.wantMaxDocuments, settings.MaxDocuments)
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
		{name: "default", baseURL: "https://api.openai.com"},
		{name: "empty means the default", baseURL: ""},
		{name: "plain http", baseURL: "http://api.openai.com", wantErr: true},
		{name: "loopback address", baseURL: "https://127.0.0.1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(fakeClassConfig{classConfig: map[string]any{"baseURL": tt.baseURL}})

			err := ic.Validate(nil)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
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
