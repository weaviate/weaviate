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

package modrerankerdigitalocean

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/reranker-digitalocean/config"
	usecasesconfig "github.com/weaviate/weaviate/usecases/config"
)

func TestValidateClass(t *testing.T) {
	tests := []struct {
		name       string
		cfg        map[string]any
		models     []string
		wantErrMsg string
	}{
		{
			name:   "default model available",
			cfg:    map[string]any{},
			models: []string{config.DefaultModel},
		},
		{
			name:       "model not available",
			cfg:        map[string]any{"model": "made-up-model"},
			models:     []string{config.DefaultModel},
			wantErrMsg: `model "made-up-model" is not available`,
		},
		{
			name:       "invalid baseURL",
			cfg:        map[string]any{"baseURL": "http://127.0.0.1"},
			models:     []string{config.DefaultModel},
			wantErrMsg: "baseURL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			t.Setenv("DIGITALOCEAN_APIKEY", "dop_v1_test")

			prev := config.DefaultModelLister
			config.DefaultModelLister = fakeModelLister(tt.models)
			t.Cleanup(func() { config.DefaultModelLister = prev })

			err := New().ValidateClass(context.Background(), nil, fakeClassConfig(tt.cfg))
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErrMsg)
			}
		})
	}
}

type fakeModelLister []string

func (f fakeModelLister) ListModels(_ context.Context, _, _, _ string) ([]string, error) {
	return f, nil
}

type fakeClassConfig map[string]any

func (f fakeClassConfig) Class() map[string]any                              { return f }
func (f fakeClassConfig) Tenant() string                                     { return "" }
func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any { return f }
func (f fakeClassConfig) Property(propName string) map[string]any            { return nil }
func (f fakeClassConfig) TargetVector() string                               { return "" }
func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType    { return nil }
func (f fakeClassConfig) Config() *usecasesconfig.Config                     { return nil }
