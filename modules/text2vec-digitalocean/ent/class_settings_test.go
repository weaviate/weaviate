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

package ent

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_classSettings_Validate(t *testing.T) {
	class := &models.Class{
		Class: "Test",
		Properties: []*models.Property{
			{
				DataType: []string{schema.DataTypeText.String()},
				Name:     "text",
			},
		},
	}

	tests := []struct {
		name          string
		cfg           moduletools.ClassConfig
		lister        *fakeModelLister
		apiKeyEnv     string
		wantErrMsg    string
		wantNoListing bool
	}{
		{
			name: "model is set and is in the list",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "qwen3-embedding-0.6b",
				},
			},
			lister:    &fakeModelLister{models: []string{"qwen3-embedding-0.6b", "openai-gpt-oss-20b"}},
			apiKeyEnv: "dop_v1_test",
		},
		{
			name: "model is missing",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			lister:     &fakeModelLister{models: []string{"qwen3-embedding-0.6b"}},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: "model must be set",
		},
		{
			name: "model is not in the available list",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "made-up-model",
				},
			},
			lister:     &fakeModelLister{models: []string{"qwen3-embedding-0.6b"}},
			apiKeyEnv:  "dop_v1_test",
			wantErrMsg: `model "made-up-model" is not available`,
		},
		{
			// Without a server-side env var we cannot pre-validate the model
			// against /v1/models, so Validate skips silently and the
			// Serverless Inference endpoint will surface any "unknown model"
			// error at vectorize time (where users can supply their own key
			// via the X-Digitalocean-Api-Key header).
			name: "api key missing - validation skipped",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "any-model-we-cannot-verify",
				},
			},
			lister:        &fakeModelLister{models: []string{"qwen3-embedding-0.6b"}},
			apiKeyEnv:     "",
			wantNoListing: true,
		},
		{
			name: "lister returns error",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "qwen3-embedding-0.6b",
				},
			},
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

			cs := NewClassSettings(tt.cfg)
			err := cs.Validate(context.Background(), class)
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
			}
			if tt.wantNoListing {
				assert.Equal(t, 0, tt.lister.calls, "expected the model lister to not be called when the API key is missing")
			}
		})
	}
}

func Test_classSettings_Defaults(t *testing.T) {
	cs := NewClassSettings(fakeClassConfig{classConfig: map[string]interface{}{}})
	assert.Equal(t, DefaultBaseURL, cs.BaseURL())
	assert.Equal(t, "", cs.Model())
	assert.Nil(t, cs.Dimensions())
}

func Test_classSettings_Overrides(t *testing.T) {
	cs := NewClassSettings(fakeClassConfig{classConfig: map[string]interface{}{
		"baseURL":    "https://example.com",
		"model":      "qwen3-embedding-0.6b",
		"dimensions": 1024,
	}})
	assert.Equal(t, "https://example.com", cs.BaseURL())
	assert.Equal(t, "qwen3-embedding-0.6b", cs.Model())
	// DigitalOcean's /v1/embeddings does not support the "dimensions" field,
	// so Dimensions() is hardwired to nil even when the user sets it.
	assert.Nil(t, cs.Dimensions())
}
