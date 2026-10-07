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

package moddecisionsopenai

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestModuleNameAndType(t *testing.T) {
	m := New()

	assert.Equal(t, "decisions-openai", Name)
	assert.Equal(t, "decisions-openai", m.Name())
	assert.Equal(t, modulecapabilities.Decisions, m.Type())
}

func TestMaxConcurrentRequestsFromEnv(t *testing.T) {
	const rangeErr = "DECISIONS_OPENAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "unset uses the default", value: "", want: 32},
		{name: "lowest value", value: "1", want: 1},
		{name: "highest value", value: "256", want: 256},
		{name: "zero", value: "0", wantErr: rangeErr + `"0"`},
		{name: "above the limit", value: "257", wantErr: rangeErr + `"257"`},
		{name: "negative", value: "-4", wantErr: rangeErr + `"-4"`},
		{name: "not a whole number", value: "1.5", wantErr: rangeErr + `"1.5"`},
		{name: "not a number", value: "many", wantErr: rangeErr + `"many"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, tt.value)

			got, err := maxConcurrentRequestsFromEnv()

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestInitAdditional(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "valid limit", value: "8"},
		{
			name:    "invalid limit fails the init",
			value:   "0",
			wantErr: `DECISIONS_OPENAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "0"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, tt.value)
			logger, _ := test.NewNullLogger()
			m := New()

			err := m.initAdditional(context.Background(), 0, logger)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Contains(t, m.AdditionalProperties(), "rerank")
			meta, err := m.MetaInfo()
			require.NoError(t, err)
			assert.Equal(t, "Decisions - OpenAI", meta["name"])
			assert.NotEmpty(t, meta["documentationHref"])
		})
	}
}

func TestValidateClass(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		wantErr  string
	}{
		{name: "defaults", settings: map[string]any{}},
		{name: "valid settings", settings: map[string]any{"model": "gpt-4o-mini", "maxDocuments": 10}},
		{
			name:     "maxDocuments above the upper bound",
			settings: map[string]any{"maxDocuments": 100000},
			wantErr:  "maxDocuments must be between 1 and 1000, got 100000",
		},
		{
			name:     "maxDocuments of the wrong type",
			settings: map[string]any{"maxDocuments": "many"},
			wantErr:  `maxDocuments must be a whole number between 1 and 1000, got "many"`,
		},
		{
			name:     "maxDocuments with a fraction",
			settings: map[string]any{"maxDocuments": json.Number("2.5")},
			wantErr:  "maxDocuments must be a whole number between 1 and 1000, got 2.5",
		},
		{
			name:     "model of the wrong type",
			settings: map[string]any{"model": json.Number("4")},
			wantErr:  "model must be a string, got a number",
		},
		{
			name:     "baseURL without a scheme",
			settings: map[string]any{"baseURL": "api.openai.com"},
			wantErr:  `baseURL must be a URL with an http or https scheme and a host, such as https://api.openai.com, got "api.openai.com"`,
		},
		{name: "baseURL in the OpenAI SDK form", settings: map[string]any{"baseURL": "https://api.openai.com/v1"}},
		{
			name:     "empty model",
			settings: map[string]any{"model": ""},
			wantErr:  "no model provided",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{
				Class:        "Ticket",
				ModuleConfig: map[string]any{Name: tt.settings},
			}
			cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

			err := New().ValidateClass(context.Background(), class, cfg)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
