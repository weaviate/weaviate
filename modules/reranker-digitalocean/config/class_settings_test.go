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
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       moduletools.ClassConfig
		wantModel string
		wantErr   error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel: "bge-reranker-v2-m3",
		},
		{
			name: "custom settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "custom-model",
				},
			},
			wantModel: "custom-model",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(context.Background(), nil), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantModel, ic.Model())
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

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func Test_classSettings_ValidateBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	t.Setenv("DIGITALOCEAN_APIKEY", "")
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{
			name:    "valid HTTPS URL",
			baseURL: "https://api.openai.com",
			wantErr: false,
		},
		{
			name:    "HTTP URL is rejected",
			baseURL: "http://api.example.com",
			wantErr: true,
		},
		{
			name:    "loopback address is rejected",
			baseURL: "https://127.0.0.1",
			wantErr: true,
		},
		{
			name:    "private network address is rejected",
			baseURL: "https://192.168.1.1",
			wantErr: true,
		},
		{
			name:    "empty host is rejected",
			baseURL: "https://",
			wantErr: true,
		},
		{
			name:    "localhost is rejected",
			baseURL: "https://localhost",
			wantErr: true,
		},
		{
			name:    "local domain is rejected",
			baseURL: "https://myhost.local",
			wantErr: true,
		},
		{
			name:    "default URL is valid",
			baseURL: "https://inference.do-ai.run",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL": tt.baseURL,
				},
			})
			err := ic.Validate(context.Background(), nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_classSettings_ValidateModel(t *testing.T) {
	tests := []struct {
		name          string
		cfg           map[string]any
		lister        *fakeModelLister
		apiKeyEnv     string
		uuidEnv       string
		wantErrMsg    string
		wantNoListing bool
	}{
		{
			name:      "default model is in the list",
			cfg:       map[string]any{},
			lister:    &fakeModelLister{models: []string{"bge-reranker-v2-m3", "llama-4-maverick"}},
			apiKeyEnv: "dop_v1_test",
			uuidEnv:   "test-uuid",
		},
		{
			name:      "custom model is in the list",
			cfg:       map[string]any{"model": "custom-reranker"},
			lister:    &fakeModelLister{models: []string{"custom-reranker"}},
			apiKeyEnv: "dop_v1_test",
		},
		{
			name:       "model is not in the available list",
			cfg:        map[string]any{"model": "made-up-model"},
			lister:     &fakeModelLister{models: []string{"bge-reranker-v2-m3"}},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: `model "made-up-model" is not available`,
		},
		{
			name:       "default model is not in the available list",
			cfg:        map[string]any{},
			lister:     &fakeModelLister{models: []string{"other-model"}},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: `model "bge-reranker-v2-m3" is not available`,
		},
		{
			name:       "lister returns error",
			cfg:        map[string]any{},
			lister:     &fakeModelLister{err: errors.New("boom")},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: "list DigitalOcean models: boom",
		},
		{
			name:          "api key missing - validation skipped",
			cfg:           map[string]any{"model": "any-model-we-cannot-verify"},
			lister:        &fakeModelLister{models: []string{"bge-reranker-v2-m3"}},
			wantNoListing: true,
		},
		{
			name:          "invalid baseURL fails before listing",
			cfg:           map[string]any{"baseURL": "http://127.0.0.1"},
			lister:        &fakeModelLister{models: []string{"bge-reranker-v2-m3"}},
			apiKeyEnv:     "dop_v1_test",
			wantErrMsg:    "baseURL",
			wantNoListing: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			t.Setenv("DIGITALOCEAN_APIKEY", tt.apiKeyEnv)
			t.Setenv("VECTOR_DB_UUID", tt.uuidEnv)

			prev := DefaultModelLister
			DefaultModelLister = tt.lister
			t.Cleanup(func() { DefaultModelLister = prev })

			cs := NewClassSettings(fakeClassConfig{classConfig: tt.cfg})
			err := cs.Validate(context.Background(), nil)
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErrMsg)
			}
			if tt.wantNoListing {
				assert.Equal(t, 0, tt.lister.calls)
				return
			}
			assert.Equal(t, 1, tt.lister.calls)
			assert.Equal(t, cs.BaseURL(), tt.lister.gotBaseURL)
			assert.Equal(t, tt.apiKeyEnv, tt.lister.gotAPIKey)
			assert.Equal(t, tt.uuidEnv, tt.lister.gotUUID)
		})
	}
}

func Test_classSettings_EnvAccessors(t *testing.T) {
	t.Setenv("DIGITALOCEAN_APIKEY", "dop_v1_test")
	t.Setenv("VECTOR_DB_UUID", "test-uuid")
	cs := NewClassSettings(fakeClassConfig{classConfig: map[string]any{}})
	assert.Equal(t, "dop_v1_test", cs.apiKey())
	assert.Equal(t, "test-uuid", cs.WeaviateUUID())
}

type fakeModelLister struct {
	models     []string
	err        error
	calls      int
	gotBaseURL string
	gotAPIKey  string
	gotUUID    string
}

func (f *fakeModelLister) ListModels(_ context.Context, baseURL, apiKey, weaviateUUID string) ([]string, error) {
	f.calls++
	f.gotBaseURL, f.gotAPIKey, f.gotUUID = baseURL, apiKey, weaviateUUID
	if f.err != nil {
		return nil, f.err
	}
	return f.models, nil
}
