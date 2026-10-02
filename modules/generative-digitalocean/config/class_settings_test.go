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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestClassSettings(t *testing.T) {
	tests := []struct {
		name                string
		cfg                 moduletools.ClassConfig
		expectedErr         string
		expectedBaseURL     string
		expectedModel       string
		expectedTemperature *float64
		expectedTopP        *float64
		expectedMaxTokens   *int
		expectedFreqPenalty *float64
		expectedPresPenalty *float64
		expectedStop        []string
	}{
		{
			name:            "defaults",
			cfg:             fakeClassConfig{},
			expectedBaseURL: "https://inference.do-ai.run",
			expectedModel:   "llama-4-maverick",
		},
		{
			name: "all properties set",
			cfg: fakeClassConfig{
				"baseURL":          "https://custom.do-ai.run",
				"model":            "openai-gpt-4o",
				"temperature":      0.5,
				"topP":             0.9,
				"maxTokens":        512,
				"frequencyPenalty": 1.5,
				"presencePenalty":  -1.5,
				"stop":             []any{"\n", "END"},
			},
			expectedBaseURL:     "https://custom.do-ai.run",
			expectedModel:       "openai-gpt-4o",
			expectedTemperature: ptr(0.5),
			expectedTopP:        ptr(0.9),
			expectedMaxTokens:   ptr(512),
			expectedFreqPenalty: ptr(1.5),
			expectedPresPenalty: ptr(-1.5),
			expectedStop:        []string{"\n", "END"},
		},
		{
			name:        "nil config",
			cfg:         nil,
			expectedErr: "empty config",
		},
		{
			name:        "temperature too high",
			cfg:         fakeClassConfig{"temperature": 2.1},
			expectedErr: "wrong temperature configuration",
		},
		{
			name:        "temperature too low",
			cfg:         fakeClassConfig{"temperature": -0.1},
			expectedErr: "wrong temperature configuration",
		},
		{
			name:        "topP too high",
			cfg:         fakeClassConfig{"topP": 1.1},
			expectedErr: "wrong topP configuration",
		},
		{
			name:        "maxTokens below one",
			cfg:         fakeClassConfig{"maxTokens": 0},
			expectedErr: "wrong maxTokens configuration",
		},
		{
			name:        "frequencyPenalty out of range",
			cfg:         fakeClassConfig{"frequencyPenalty": 2.5},
			expectedErr: "wrong frequencyPenalty configuration",
		},
		{
			name:        "presencePenalty out of range",
			cfg:         fakeClassConfig{"presencePenalty": -2.5},
			expectedErr: "wrong presencePenalty configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := NewClassSettings(tt.cfg)

			err := settings.Validate(context.Background(), nil)
			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedBaseURL, settings.BaseURL())
			assert.Equal(t, tt.expectedModel, settings.Model())
			assert.Equal(t, tt.expectedTemperature, settings.Temperature())
			assert.Equal(t, tt.expectedTopP, settings.TopP())
			assert.Equal(t, tt.expectedMaxTokens, settings.MaxTokens())
			assert.Equal(t, tt.expectedFreqPenalty, settings.FrequencyPenalty())
			assert.Equal(t, tt.expectedPresPenalty, settings.PresencePenalty())
			assert.Equal(t, tt.expectedStop, settings.Stop())
		})
	}
}

func TestClassSettingsValidateModel(t *testing.T) {
	tests := []struct {
		name          string
		cfg           fakeClassConfig
		lister        *fakeModelLister
		apiKeyEnv     string
		wantErrMsg    string
		wantNoListing bool
	}{
		{
			name:      "model is in the available list",
			cfg:       fakeClassConfig{"model": "openai-gpt-4o"},
			lister:    &fakeModelLister{models: []string{"llama-4-maverick", "openai-gpt-4o"}},
			apiKeyEnv: "dop_v1_test",
		},
		{
			name:      "default model is in the available list",
			cfg:       fakeClassConfig{},
			lister:    &fakeModelLister{models: []string{"llama-4-maverick"}},
			apiKeyEnv: "dop_v1_test",
		},
		{
			name:       "model is not in the available list",
			cfg:        fakeClassConfig{"model": "made-up-model"},
			lister:     &fakeModelLister{models: []string{"llama-4-maverick"}},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: `model "made-up-model" is not available`,
		},
		{
			name:          "api key missing - validation skipped",
			cfg:           fakeClassConfig{"model": "made-up-model"},
			lister:        &fakeModelLister{models: []string{"llama-4-maverick"}},
			wantNoListing: true,
		},
		{
			name:       "lister returns error",
			cfg:        fakeClassConfig{"model": "llama-4-maverick"},
			lister:     &fakeModelLister{err: errors.New("boom")},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: "list DigitalOcean models",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DIGITALOCEAN_APIKEY", tt.apiKeyEnv)

			prev := DefaultModelLister
			DefaultModelLister = tt.lister
			t.Cleanup(func() { DefaultModelLister = prev })

			err := NewClassSettings(tt.cfg).Validate(context.Background(), nil)
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
			}
			if tt.wantNoListing {
				assert.Equal(t, 0, tt.lister.calls)
			}
		})
	}
}

func TestClassSettingsEnvVars(t *testing.T) {
	t.Setenv("DIGITALOCEAN_APIKEY", "dop_v1_test")
	t.Setenv("VECTOR_DB_UUID", "test-uuid")

	settings := NewClassSettings(fakeClassConfig{})
	assert.Equal(t, "dop_v1_test", settings.apiKey())
	assert.Equal(t, "test-uuid", settings.WeaviateUUID())
}

type fakeModelLister struct {
	models []string
	err    error
	calls  int
}

func (f *fakeModelLister) ListModels(_ context.Context, _, _, _ string) ([]string, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.models, nil
}

func ptr[T any](v T) *T {
	return &v
}

type fakeClassConfig map[string]any

func (f fakeClassConfig) Class() map[string]any { return f }

func (f fakeClassConfig) Tenant() string { return "" }

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any { return f }

func (f fakeClassConfig) Property(propName string) map[string]any { return nil }

func (f fakeClassConfig) TargetVector() string { return "" }

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType { return nil }

func (f fakeClassConfig) Config() *config.Config { return nil }
