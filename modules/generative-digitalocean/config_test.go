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

package modgenerativedigitalocean

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/generative-digitalocean/config"
	usecasesconfig "github.com/weaviate/weaviate/usecases/config"
)

func TestValidateClass(t *testing.T) {
	tests := []struct {
		name          string
		cfg           fakeClassConfig
		lister        *fakeModelLister
		apiKeyEnv     string
		expectedErr   string
		wantNoListing bool
	}{
		{
			name:      "model available",
			cfg:       fakeClassConfig{"model": "llama-4-maverick"},
			lister:    &fakeModelLister{models: []string{"llama-4-maverick"}},
			apiKeyEnv: "dop_v1_test",
		},
		{
			name:          "api key missing skips the model check",
			cfg:           fakeClassConfig{"model": "retired-model"},
			lister:        &fakeModelLister{models: []string{"llama-4-maverick"}},
			wantNoListing: true,
		},
		{
			name:          "empty model skips the model check",
			cfg:           fakeClassConfig{"model": ""},
			lister:        &fakeModelLister{models: []string{"llama-4-maverick"}},
			apiKeyEnv:     "dop_v1_test",
			wantNoListing: true,
		},
		{
			name:        "model not available",
			cfg:         fakeClassConfig{"model": "retired-model"},
			lister:      &fakeModelLister{models: []string{"llama-4-maverick"}},
			apiKeyEnv:   "dop_v1_test",
			expectedErr: `model "retired-model" is not available`,
		},
		{
			name:        "lister error",
			cfg:         fakeClassConfig{"model": "llama-4-maverick"},
			lister:      &fakeModelLister{err: errors.New("endpoint unreachable")},
			apiKeyEnv:   "dop_v1_test",
			expectedErr: "list DigitalOcean models: endpoint unreachable",
		},
		{
			name:          "invalid local config fails before the model check",
			cfg:           fakeClassConfig{"temperature": 3.0},
			lister:        &fakeModelLister{models: []string{"llama-4-maverick"}},
			apiKeyEnv:     "dop_v1_test",
			expectedErr:   "wrong temperature configuration",
			wantNoListing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DIGITALOCEAN_APIKEY", tt.apiKeyEnv)

			prev := config.DefaultModelLister
			config.DefaultModelLister = tt.lister
			t.Cleanup(func() { config.DefaultModelLister = prev })

			m := &GenerativeDigitalOceanModule{}
			err := m.ValidateClass(context.Background(), &models.Class{Class: "Test"}, tt.cfg)
			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
			}
			if tt.wantNoListing {
				assert.Zero(t, tt.lister.calls)
			}
		})
	}
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

type fakeClassConfig map[string]any

func (f fakeClassConfig) Class() map[string]any { return f }

func (f fakeClassConfig) Tenant() string { return "" }

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any { return f }

func (f fakeClassConfig) Property(propName string) map[string]any { return nil }

func (f fakeClassConfig) TargetVector() string { return "" }

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType { return nil }

func (f fakeClassConfig) Config() *usecasesconfig.Config { return nil }
