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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name             string
		cfg              moduletools.ClassConfig
		wantModel        string
		wantBaseURL      string
		wantMaxDocuments int
		wantMinProb      float64
		// wantBatchSize of 0 stands for the default.
		wantBatchSize int
		// wantOrder empty stands for the default.
		wantOrder string
		wantErr   string
	}{
		{
			name:             "default settings",
			cfg:              fakeClassConfig{classConfig: map[string]any{}},
			wantModel:        "jev-latest",
			wantBaseURL:      "https://api.typesafe.ai",
			wantMaxDocuments: 100,
		},
		{
			name: "custom settings",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"model":          "jev-1.13.0",
				"baseURL":        "https://proxy.example.com",
				"maxDocuments":   25,
				"minProbability": 0.8,
			}},
			wantMinProb:      0.8,
			wantModel:        "jev-1.13.0",
			wantBaseURL:      "https://proxy.example.com",
			wantMaxDocuments: 25,
		},
		{
			// Validation reads an empty baseURL as "use the default".
			name:             "empty baseURL is the default",
			cfg:              fakeClassConfig{classConfig: map[string]any{"baseURL": ""}},
			wantModel:        "jev-latest",
			wantBaseURL:      "https://api.typesafe.ai",
			wantMaxDocuments: 100,
		},
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: "empty config",
		},
		{
			name: "empty model",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"model": "",
			}},
			wantErr: "no model provided",
		},
		{
			name: "maxDocuments zero",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"maxDocuments": 0,
			}},
			wantErr: "maxDocuments must be between 1 and 1000, got 0",
		},
		{
			name: "maxDocuments negative",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"maxDocuments": -5,
			}},
			wantErr: "maxDocuments must be between 1 and 1000, got -5",
		},
		{
			name: "maxDocuments above the upper bound",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"maxDocuments": 1001,
			}},
			wantErr: "maxDocuments must be between 1 and 1000, got 1001",
		},
		{
			name: "order probability",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"order": "probability",
			}},
			wantModel:        "jev-latest",
			wantBaseURL:      "https://api.typesafe.ai",
			wantMaxDocuments: 100,
			wantOrder:        "probability",
		},
		{
			name: "unknown order",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"order": "newest",
			}},
			wantErr: `order must be "search" or "probability", got "newest"`,
		},
		{
			name: "batchSize at the maximum",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"batchSize": 25,
			}},
			wantModel:        "jev-latest",
			wantBaseURL:      "https://api.typesafe.ai",
			wantMaxDocuments: 100,
			wantBatchSize:    25,
		},
		{
			name: "batchSize zero",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"batchSize": 0,
			}},
			wantErr: "batchSize must be between 1 and 25, got 0",
		},
		{
			name: "batchSize above the maximum",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"batchSize": 26,
			}},
			wantErr: "batchSize must be between 1 and 25, got 26",
		},
		{
			name: "batchSize of the wrong type",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"batchSize": "big",
			}},
			wantErr: "batchSize must be between 1 and 25, got 0",
		},
		{
			name: "minProbability of 1",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"minProbability": 1,
			}},
			wantModel:        "jev-latest",
			wantBaseURL:      "https://api.typesafe.ai",
			wantMaxDocuments: 100,
			wantMinProb:      1,
		},
		{
			name: "minProbability negative",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"minProbability": -0.2,
			}},
			wantErr: "minProbability must be between 0 and 1, got -0.2",
		},
		{
			name: "minProbability above 1",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"minProbability": 1.01,
			}},
			wantErr: "minProbability must be between 0 and 1, got 1.01",
		},
		{
			name: "minProbability of the wrong type",
			cfg: fakeClassConfig{classConfig: map[string]any{
				"minProbability": "high",
			}},
			wantErr: "minProbability must be between 0 and 1, got -1",
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
			assert.Equal(t, tt.wantModel, ic.Model())
			assert.Equal(t, tt.wantBaseURL, ic.BaseURL())
			assert.Equal(t, tt.wantMaxDocuments, ic.MaxDocuments())
			assert.Equal(t, tt.wantMinProb, ic.MinProbability())
			wantBatchSize := tt.wantBatchSize
			if wantBatchSize == 0 {
				wantBatchSize = 1
			}
			assert.Equal(t, wantBatchSize, ic.BatchSize())
			wantOrder := tt.wantOrder
			if wantOrder == "" {
				wantOrder = "search"
			}
			assert.Equal(t, wantOrder, ic.Order())
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
